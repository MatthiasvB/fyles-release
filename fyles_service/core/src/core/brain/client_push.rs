//! Everything the attached clients get told about, in one place.
//!
//! Clients hold no authoritative state of their own: a window renders what it was last pushed, so
//! any state a window can show needs a push here for every way that state can change. Auditing
//! coverage means reading this module against the `ServerMessage` oneof.

use tracing::error;

use crate::core::brain::GigaBrain;
use crate::core::domain_models::{Filerequest, FylesId, PendingFile};
use crate::core::listen::ListenStatus;
use crate::core::settings::SettingsView;
use crate::core::sync::{
    ContactChange, DeviceChange, DocChange, DocKind, RemoteFilerequestChange, list_owned_devices,
};
use crate::library::wire::api::ServerMessage;

impl GigaBrain {
    async fn push(&self, message: ServerMessage) {
        if let Err(e) = self.client_push_sender.send(message).await {
            error!("Failed to push to clients: {e:?}");
        }
    }

    /// The entity a local write touched, straight from the write itself. No read, and no other
    /// window's data on the wire.
    pub(super) async fn push_doc_change(&self, change: &DocChange) {
        let message = match change {
            DocChange::Contacts(ContactChange::Upserted(contact)) => {
                ServerMessage::contact_upserted(contact.clone())
            }
            DocChange::Contacts(ContactChange::Removed(id)) => ServerMessage::contact_removed(id),
            DocChange::RemoteFilerequests(RemoteFilerequestChange::Upserted(remote_fr)) => {
                ServerMessage::remote_filerequest_upserted(remote_fr.clone())
            }
            DocChange::RemoteFilerequests(RemoteFilerequestChange::Removed(id)) => {
                ServerMessage::remote_filerequest_removed(id)
            }
            DocChange::Devices(DeviceChange::Upserted(peer_id, name)) => {
                ServerMessage::owned_device_upserted(peer_id.clone(), name.clone())
            }
            DocChange::Devices(DeviceChange::Removed(peer_id)) => {
                ServerMessage::owned_device_removed(peer_id)
            }
        };
        self.push(message).await;
    }

    pub(super) async fn push_filerequest_upserted(&self, filerequest: &Filerequest) {
        self.push(ServerMessage::filerequest_upserted(filerequest))
            .await;
    }

    pub(super) async fn push_filerequest_removed(&self, id: &FylesId) {
        self.push(ServerMessage::filerequest_removed(id)).await;
    }

    pub(super) async fn push_pending_files_added(&self, pending_files: Vec<PendingFile>) {
        self.push(ServerMessage::pending_files_added(pending_files))
            .await;
    }

    pub(super) async fn push_pending_file_removed(
        &self,
        id: &FylesId,
        target_filerequest_id: &FylesId,
    ) {
        self.push(ServerMessage::pending_file_removed(
            id,
            target_filerequest_id,
        ))
        .await;
    }

    pub(super) async fn push_self_contact(&self) {
        match self.db.get_self_contact_for_display().await {
            Ok(self_contact) => {
                self.push(ServerMessage::self_contact_changed(self_contact))
                    .await
            }
            Err(e) => error!("Failed to load the self contact to push: {e:?}"),
        }
    }

    pub(super) async fn push_received_files(&self, filerequest_id: &FylesId) {
        self.push(ServerMessage::received_files_changed(filerequest_id))
            .await;
    }

    pub(super) async fn push_settings(&self, view: SettingsView) {
        self.push(ServerMessage::settings_changed(view)).await;
    }

    pub(super) async fn push_listen_status(&self, status: ListenStatus) {
        self.push(ServerMessage::listen_status_changed(status))
            .await;
    }

    /// A convergence merged an unknown set of changes, so the whole concern goes out and the
    /// client replaces its copy. Only the sync path needs this.
    pub(super) async fn push_converged_state(&self, kind: DocKind) {
        match kind {
            DocKind::Contacts => self.push_all_contacts().await,
            DocKind::RemoteFilerequests => self.push_all_remote_filerequests().await,
            DocKind::Devices => self.push_all_owned_devices().await,
        }
    }

    async fn push_all_contacts(&self) {
        match self.db.get_contacts().await {
            Ok(contacts) => self.push(ServerMessage::contacts_changed(contacts)).await,
            Err(e) => error!("Failed to load contacts to push: {e:?}"),
        }
    }

    async fn push_all_remote_filerequests(&self) {
        let remote_frs = match self.db.get_all_remote_filerequests().await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to load remote filerequests to push: {e:?}");
                return;
            }
        };
        let pending = match self.db.get_all_pending_files().await {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to load pending files to push: {e:?}");
                return;
            }
        };
        self.push(ServerMessage::remote_filerequests_changed(
            remote_frs, pending,
        ))
        .await;
    }

    async fn push_all_owned_devices(&self) {
        match list_owned_devices(self.db.as_ref()).await {
            Ok(devices) => {
                self.push(ServerMessage::owned_devices_changed(devices))
                    .await
            }
            Err(e) => error!("Failed to load owned devices to push: {e:?}"),
        }
    }
}
