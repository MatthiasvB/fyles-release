use crate::event_loop::{EventLoopSender, InnerEvent, LocalNetworkSwarm};
use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{Span, instrument, trace};

/// The queued closure was dropped without ever reaching a swarm, because the swarm it was queued
/// against was rebuilt or the event loop stopped. The caller's request never went out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("swarm work was discarded before it ran (swarm rebuilt or event loop stopped)")]
pub struct SwarmWorkDiscarded;

pub trait GetInnerEventSender<T: LocalNetworkSwarm> {
    fn get_inner_event_sender(&self) -> &EventLoopSender<T>;
}

#[async_trait]
pub trait WithSwarm<T> {
    fn with_swarm(&self, block: impl FnOnce(&mut T) + Send + Sync + 'static);

    async fn with_swarm_res<R, F>(&self, block: F) -> Result<R, SwarmWorkDiscarded>
    where
        R: Send + 'static,
        // explicit lifetime 'a needed only due to usage of async_trait and the way it desugars this signature
        F: for<'a> FnOnce(&'a mut T) -> R + Send + 'static;
}

#[async_trait]
impl<S: LocalNetworkSwarm + 'static, T: GetInnerEventSender<S> + Sync> WithSwarm<S> for T {
    fn with_swarm(&self, block: impl FnOnce(&mut S) + Send + Sync + 'static) {
        trace!("Sending workload to swarm");
        let _ = self.get_inner_event_sender().send(InnerEvent::DoWithSwarm {
            block: Box::new(block),
            span: Span::current(),
        });
    }

    #[instrument(skip_all, level = "trace")]
    async fn with_swarm_res<R, F>(&self, block: F) -> Result<R, SwarmWorkDiscarded>
    where
        R: Send + 'static,
        // explicit lifetime 'a needed only due to usage of async_trait and the way it desugars this signature
        F: for<'a> FnOnce(&'a mut S) -> R + Send + 'static,
    {
        let (sender, receiver) = oneshot::channel();
        trace!("Sending workload to swarm");
        let _ = self.get_inner_event_sender().send(InnerEvent::DoWithSwarm {
            block: Box::new(|swarm| {
                let res = block(swarm);
                let _ = sender.send(res);
            }),
            span: Span::current(),
        });
        trace!("Sent");
        receiver.await.map_err(|_| SwarmWorkDiscarded)
    }
}
