use crate::{
    behaviour::{CoreBehaviourEvent, LocalNetworkBehaviourEvent},
    event_loop::{FileTracker, LocalNetworkSwarm, RefCountEventLoopData},
};

impl<T: FileTracker + 'static, S: LocalNetworkSwarm + 'static> RefCountEventLoopData<T, S> {
    pub fn handle_local_behaviour_event(self, event: LocalNetworkBehaviourEvent) {
        match event {
            LocalNetworkBehaviourEvent::Mdns(event) => self.handle_mdns_event(event),
            LocalNetworkBehaviourEvent::Core(event) => self.handle_core_behaviour_event(event),
        }
    }

    pub(super) fn handle_core_behaviour_event(self, event: CoreBehaviourEvent) {
        match event {
            CoreBehaviourEvent::Filerequest(filerequest_event) => {
                self.handle_filerequest_event(filerequest_event)
            }
            CoreBehaviourEvent::SessionEstablishment(event) => {
                self.handle_session_establishment_event(event)
            }
            CoreBehaviourEvent::SelfContactInvite(event) => {
                self.handle_self_contact_invite_event(event);
            }
            CoreBehaviourEvent::ContactShare(event) => {
                self.handle_contact_share_event(event);
            }
        }
    }
}
