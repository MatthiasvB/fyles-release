use rand::RngExt;
use tracing::{error, info, warn};

use crate::constants::{
    LISTEN_PORT_PICKS, LISTEN_RECLAIM_BACKOFF, RANDOM_LISTEN_PORTS, RELISTEN_DELAYS,
};
use crate::core::brain::GigaBrain;
use crate::core::brain::types::FilerequestResult;
use crate::core::listen::{ListenFailure, ListenPort, ListenStatus, TemporaryPort};
use crate::core::p2p::NodeConfig;
use crate::library::util::util::TimeoutLock;

impl GigaBrain {
    pub(super) async fn establish_listening(&self) {
        let mut status = self.listen_status.timeout_lock().await;
        if self.establish(&mut status).await {
            self.listen_wakeup.notify_one();
        }
    }

    /// A rebuilt swarm listens nowhere until told again. Before the node reported ready, the
    /// first establish is still to come and covers the new swarm too.
    pub(super) async fn restart_network(&self, config: NodeConfig) {
        let mut status = self.listen_status.timeout_lock().await;
        self.network.read().await.restart(config).await;
        if status.is_some() && self.establish(&mut status).await {
            self.listen_wakeup.notify_one();
        }
    }

    pub(super) async fn set_listen_port(&self, port: ListenPort) -> FilerequestResult<()> {
        let mut status = self.listen_status.timeout_lock().await;
        self.db.store_listen_port(port).await?;
        if self.establish(&mut status).await {
            self.listen_wakeup.notify_one();
        }
        Ok(())
    }

    pub(super) async fn listen_status(&self) -> Option<ListenStatus> {
        self.listen_status.timeout_lock().await.clone()
    }

    /// Retries a degraded status until the configured port is back, backing off while it is not.
    pub(super) async fn watch_listen_status(&self) {
        let mut attempt = 0;
        loop {
            let degraded = self
                .listen_status
                .timeout_lock()
                .await
                .as_ref()
                .is_some_and(ListenStatus::is_degraded);
            if !degraded {
                self.listen_wakeup.notified().await;
                attempt = 0;
                continue;
            }
            let delay = LISTEN_RECLAIM_BACKOFF[attempt.min(LISTEN_RECLAIM_BACKOFF.len() - 1)];
            tokio::select! {
                () = tokio::time::sleep(delay) => {
                    attempt += 1;
                    let mut status = self.listen_status.timeout_lock().await;
                    self.establish(&mut status).await;
                }
                () = self.listen_wakeup.notified() => attempt = 0,
            }
        }
    }

    /// Returns whether the status changed, which the clients have then been told.
    async fn establish(&self, status: &mut Option<ListenStatus>) -> bool {
        let next = match self.db.get_listen_port().await {
            Ok(Some(configured)) => {
                let held = status.as_ref().and_then(ListenStatus::active_port);
                self.listen_on_configured(configured, held).await
            }
            Ok(None) => self.listen_on_first_pick().await,
            Err(e) => {
                error!(
                    "Cannot read the configured listen port, so the node keeps its listeners: {e}"
                );
                return false;
            }
        };
        if status.as_ref() == Some(&next) {
            return false;
        }
        match &next {
            ListenStatus::Listening(port) => info!(%port, "Listening for peers"),
            degraded => warn!(
                ?degraded,
                "Peers cannot reach the node on its configured port"
            ),
        }
        *status = Some(next.clone());
        self.push_listen_status(next).await;
        true
    }

    async fn listen_on_configured(
        &self,
        configured: ListenPort,
        held: Option<ListenPort>,
    ) -> ListenStatus {
        let failure = match self.listen_on_retrying(configured).await {
            Ok(()) => return ListenStatus::Listening(configured),
            Err(failure) => failure,
        };
        let candidates = held
            .into_iter()
            .chain(random_ports(LISTEN_PORT_PICKS))
            .filter(|port| *port != configured);
        let temporary = self
            .listen_on_first(candidates)
            .await
            .ok()
            .and_then(|active| TemporaryPort::new(configured, failure.clone(), active));
        match temporary {
            Some(temporary) => ListenStatus::Temporary(temporary),
            None => ListenStatus::NotListening {
                configured: Some(configured),
                failure,
            },
        }
    }

    async fn listen_on_first_pick(&self) -> ListenStatus {
        match self.listen_on_first(random_ports(LISTEN_PORT_PICKS)).await {
            Ok(port) => {
                if let Err(e) = self.db.store_listen_port(port).await {
                    error!(%port, "Listening on a picked port that could not be kept: {e}");
                }
                ListenStatus::Listening(port)
            }
            Err(failure) => ListenStatus::NotListening {
                configured: None,
                failure,
            },
        }
    }

    async fn listen_on_retrying(&self, port: ListenPort) -> Result<(), ListenFailure> {
        for delay in RELISTEN_DELAYS {
            match self.listen_on(port).await {
                Err(ListenFailure::AddressInUse) => tokio::time::sleep(delay).await,
                outcome => return outcome,
            }
        }
        self.listen_on(port).await
    }

    async fn listen_on_first(
        &self,
        candidates: impl IntoIterator<Item = ListenPort>,
    ) -> Result<ListenPort, ListenFailure> {
        let mut last_failure = ListenFailure::Other("no port left to try".into());
        for port in candidates {
            match self.listen_on(port).await {
                Ok(()) => return Ok(port),
                Err(failure) => last_failure = failure,
            }
        }
        Err(last_failure)
    }

    async fn listen_on(&self, port: ListenPort) -> Result<(), ListenFailure> {
        self.network.read().await.listen_on_port(port).await
    }
}

fn random_ports(count: usize) -> Vec<ListenPort> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            ListenPort::try_from(rng.random_range(RANDOM_LISTEN_PORTS))
                .expect("the random range to hold only unprivileged ports")
        })
        .collect()
}
