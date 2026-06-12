use libp2p::mdns::{self, Event};
use tracing::{debug, instrument};

use crate::event_loop::{FileTracker, LocalNetworkSwarm, RefCountEventLoopData};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    #[instrument(skip_all)]
    pub(super) fn handle_mdns_event(self, event: Event) {
        match event {
            mdns::Event::Discovered(list) => {
                for (peer_id, multiaddr) in list {
                    debug!("mDNS discovered a new peer {peer_id} on {multiaddr}");
                    self.clone().handle_discovered_peer(peer_id, multiaddr);
                }
            }
            mdns::Event::Expired(list) => {
                for (peer_id, _multiaddr) in list {
                    debug!("mDNS discover peer has expired: {peer_id}");
                }
            }
        }
    }
}
