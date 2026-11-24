use std::io;
use std::net::{Ipv4Addr, TcpListener, UdpSocket};

use fyles_core::core::listen::{ListenFailure, ListenPort};
use libp2p::core::transport::ListenerId;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, TransportError};

use crate::event_loop::LocalNetworkSwarm;

pub struct ActiveListeners {
    port: ListenPort,
    quic: ListenerId,
    tcp: ListenerId,
}

/// Moves the node onto `port`. A failure leaves `current` and its listeners untouched.
pub fn listen_on_port<S: LocalNetworkSwarm + ?Sized>(
    swarm: &mut S,
    current: &mut Option<ActiveListeners>,
    port: ListenPort,
) -> Result<(), ListenFailure> {
    if current.as_ref().is_some_and(|active| active.port == port) {
        return Ok(());
    }
    probe(port)?;
    let quic = swarm
        .listen_on(quic_addr(port))
        .map_err(transport_failure)?;
    let tcp = match swarm.listen_on(tcp_addr(port)) {
        Ok(tcp) => tcp,
        Err(e) => {
            swarm.remove_listener(quic);
            return Err(transport_failure(e));
        }
    };
    if let Some(previous) = current.replace(ActiveListeners { port, quic, tcp }) {
        swarm.remove_listener(previous.quic);
        swarm.remove_listener(previous.tcp);
    }
    Ok(())
}

/// libp2p hides the error kind behind its boxed transport, and its TCP listener sets
/// `SO_REUSEPORT`, so it would share a port with another instance instead of failing. Plain
/// sockets do neither.
fn probe(port: ListenPort) -> Result<(), ListenFailure> {
    let addr = (Ipv4Addr::UNSPECIFIED, port.get());
    let _udp = UdpSocket::bind(addr)?;
    let _tcp = TcpListener::bind(addr)?;
    Ok(())
}

fn transport_failure(e: TransportError<io::Error>) -> ListenFailure {
    ListenFailure::Other(e.to_string())
}

fn quic_addr(port: ListenPort) -> Multiaddr {
    Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Udp(port.get()))
        .with(Protocol::QuicV1)
}

fn tcp_addr(port: ListenPort) -> Multiaddr {
    Multiaddr::empty()
        .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
        .with(Protocol::Tcp(port.get()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use futures::StreamExt;
    use libp2p::Swarm;
    use libp2p::identity::Keypair;
    use libp2p::swarm::SwarmEvent;

    use super::*;
    use crate::behaviour::LocalNetworkBehaviour;
    use crate::event_loop::{CoreFileTracker, LocalEventLoop};

    fn swarm() -> Swarm<LocalNetworkBehaviour> {
        LocalEventLoop::<CoreFileTracker>::swarm_factory(Keypair::generate_ed25519()).unwrap()
    }

    fn free_port() -> ListenPort {
        loop {
            let udp = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
            let port = udp.local_addr().unwrap().port();
            let tcp = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port));
            if let (Ok(port), Ok(_)) = (ListenPort::try_from(port), tcp) {
                return port;
            }
        }
    }

    async fn await_closed(swarm: &mut Swarm<LocalNetworkBehaviour>, ids: [ListenerId; 2]) {
        let mut open: HashSet<ListenerId> = ids.into();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !open.is_empty() {
                if let SwarmEvent::ListenerClosed { listener_id, .. } =
                    swarm.select_next_some().await
                {
                    open.remove(&listener_id);
                }
            }
        })
        .await
        .expect("the replaced listeners to close");
    }

    #[tokio::test]
    async fn binds_quic_and_tcp_on_the_same_port() {
        let mut swarm = swarm();
        let mut current = None;
        let port = free_port();

        listen_on_port(&mut swarm, &mut current, port).unwrap();

        assert!(current.as_ref().is_some_and(|active| active.port == port));
        assert_eq!(probe(port), Err(ListenFailure::AddressInUse));
    }

    #[tokio::test]
    async fn the_held_port_is_not_bound_again() {
        let mut swarm = swarm();
        let mut current = None;
        let port = free_port();
        listen_on_port(&mut swarm, &mut current, port).unwrap();
        let held = current.as_ref().map(|active| (active.quic, active.tcp));

        listen_on_port(&mut swarm, &mut current, port).unwrap();

        assert_eq!(current.map(|active| (active.quic, active.tcp)), held);
    }

    #[tokio::test]
    async fn a_held_udp_port_is_in_use() {
        let mut swarm = swarm();
        let mut current = None;
        let port = free_port();
        let _udp = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port.get())).unwrap();

        let outcome = listen_on_port(&mut swarm, &mut current, port);

        assert_eq!(outcome, Err(ListenFailure::AddressInUse));
        assert!(current.is_none());
    }

    #[tokio::test]
    async fn a_held_tcp_port_is_in_use_and_nothing_stays_bound() {
        let mut swarm = swarm();
        let mut current = None;
        let port = free_port();
        let tcp = TcpListener::bind((Ipv4Addr::UNSPECIFIED, port.get())).unwrap();

        let outcome = listen_on_port(&mut swarm, &mut current, port);

        assert_eq!(outcome, Err(ListenFailure::AddressInUse));
        assert!(current.is_none());
        drop(tcp);
        assert_eq!(probe(port), Ok(()));
    }

    #[tokio::test]
    async fn another_node_on_the_port_is_detected_despite_port_reuse() {
        let mut first = swarm();
        let mut first_current = None;
        let port = free_port();
        listen_on_port(&mut first, &mut first_current, port).unwrap();

        let mut second = swarm();
        let mut second_current = None;
        let outcome = listen_on_port(&mut second, &mut second_current, port);

        assert_eq!(outcome, Err(ListenFailure::AddressInUse));
        assert!(second_current.is_none());
    }

    #[tokio::test]
    async fn switching_ports_closes_the_old_listeners() {
        let mut swarm = swarm();
        let mut current = None;
        let old = free_port();
        listen_on_port(&mut swarm, &mut current, old).unwrap();
        let old_ids = current
            .as_ref()
            .map(|active| [active.quic, active.tcp])
            .unwrap();
        let new = free_port();

        listen_on_port(&mut swarm, &mut current, new).unwrap();

        assert!(current.as_ref().is_some_and(|active| active.port == new));
        await_closed(&mut swarm, old_ids).await;
    }

    #[tokio::test]
    async fn a_failed_switch_keeps_the_old_listeners() {
        let mut swarm = swarm();
        let mut current = None;
        let old = free_port();
        listen_on_port(&mut swarm, &mut current, old).unwrap();
        let taken = free_port();
        let _udp = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, taken.get())).unwrap();

        let outcome = listen_on_port(&mut swarm, &mut current, taken);

        assert_eq!(outcome, Err(ListenFailure::AddressInUse));
        assert!(current.as_ref().is_some_and(|active| active.port == old));
        assert_eq!(probe(old), Err(ListenFailure::AddressInUse));
    }
}
