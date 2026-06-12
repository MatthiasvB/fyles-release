use crypto::{
    deserialize_dilithium_private_key, deserialize_dilithium_public_key, deserialize_ed25519_private_key, deserialize_ed25519_public_key,
    serialize_dilithium_private_key, serialize_dilithium_public_key,
    serialize_ed25519_private_key, serialize_ed25519_public_key,
    ContactKeys, ContactPrivateKeys,
    ContactPublicKeys, PrivateEd25519DeserializationError, PublicEd25519DeserializationError,
};
use itertools::Itertools;
use thiserror::Error;
use tonic::Status;
use tracing::{error, trace, warn};

use crate::{
    core::domain_models::{Contact, DisplayContact, SelfContact},
    library::wire::api::{
        self, server_message, AccessProto, ContactKeysProto, ContactPrivateKeysProto,
        ContactProto, ContactPublicKeysProto, CreateRemoteFileRequestProto,
        DatabaseRestoredFromBackup, FileRequestProto, PendingFileProto, PendingFileStatusChanged,
        PublicProto, ReceiveStatusProto, ReceivedContactShareOverNetwork,
        ReceivedFileProto, ReceivedFileStatusChanged, ReceivedSelfContactInviteOverNetwork,
        RemoteFileRequestProto, SelfContactInviteAcceptedOverNetwork, SelfContactProto,
        SelfContactPublicProto, SendStatusProto, ServerMessage, SpecificUsersProto,
        StorePendingFileRequest,
    },
};
use crate::library::wire::api::TransferDataProto;
use super::domain_models::{CreatePendingFiles, CreateRemoteFilerequest, FileInfo, Filerequest, FilerequestAccess, InProgressSendStatus, PendingFile, ReceiveStatus, ReceivedFile, RemoteFilerequest, SendStatus};

impl From<&Contact> for ContactProto {
    fn from(contact: &Contact) -> Self {
        ContactProto {
            id: contact.id.0.clone(),
            name: contact.name.clone(),
        }
    }
}

impl From<ContactProto> for DisplayContact {
    fn from(proto: ContactProto) -> Self {
        Self {
            id: proto.id.into(),
            name: proto.name,
        }
    }
}

impl From<DisplayContact> for ContactProto {
    fn from(contact: DisplayContact) -> Self {
        ContactProto {
            id: contact.id.0,
            name: contact.name,
        }
    }
}

impl From<FilerequestAccess> for AccessProto {
    fn from(access: FilerequestAccess) -> Self {
        AccessProto {
            access: Some(match access {
                FilerequestAccess::Public => {
                    crate::library::wire::api::access_proto::Access::Public(PublicProto {})
                }
                FilerequestAccess::Audience { contact_ids } => {
                    crate::library::wire::api::access_proto::Access::Audience(SpecificUsersProto {
                        contact_ids: contact_ids.into_iter().map(|id| id.0).collect(),
                    })
                }
            }),
        }
    }
}

impl From<&Filerequest> for FileRequestProto {
    fn from(fr: &Filerequest) -> Self {
        Self {
            id: fr.id.clone().0,
            title: fr.title.clone(),
            description: fr.description.clone(),
            is_active: fr.is_active,
            access: Some(fr.access.clone().into()),
        }
    }
}

impl TryFrom<AccessProto> for FilerequestAccess {
    // TODO: Should not use tonic error
    type Error = tonic::Status;

    fn try_from(proto: AccessProto) -> Result<Self, Self::Error> {
        match proto.access {
            Some(api::access_proto::Access::Public(_)) => Ok(FilerequestAccess::Public),
            Some(api::access_proto::Access::Audience(users)) => Ok(FilerequestAccess::Audience {
                contact_ids: users.contact_ids.into_iter().map_into().collect(),
            }),
            None => Err(Status::invalid_argument("Missing access type")),
        }
    }
}

impl TryFrom<Option<AccessProto>> for FilerequestAccess {
    type Error = tonic::Status;

    fn try_from(proto: Option<AccessProto>) -> Result<Self, Self::Error> {
        match proto {
            Some(proto) => proto.try_into(),
            None => Err(Status::invalid_argument("Missing access type")),
        }
    }
}

impl TryFrom<FileRequestProto> for Filerequest {
    type Error = tonic::Status;

    fn try_from(proto: FileRequestProto) -> Result<Self, Self::Error> {
        Ok(Filerequest {
            id: proto.id.into(),
            title: proto.title,
            description: proto.description,
            is_active: proto.is_active,
            access: proto.access.try_into()?,
        })
    }
}

impl From<RemoteFilerequest> for RemoteFileRequestProto {
    fn from(r: RemoteFilerequest) -> Self {
        RemoteFileRequestProto {
            id: r.id.0,
            peer_id: r.peer_id.0,
            file_request_id: r.filerequest_id.0,
            name: r.name,
            contact_id: r.contact_id.0,
        }
    }
}

impl From<RemoteFileRequestProto> for RemoteFilerequest {
    fn from(proto: RemoteFileRequestProto) -> Self {
        RemoteFilerequest {
            id: proto.id.into(),
            peer_id: proto.peer_id.into(),
            filerequest_id: proto.file_request_id.into(),
            name: proto.name,
            contact_id: proto.contact_id.into(),
        }
    }
}

impl From<CreateRemoteFilerequest> for CreateRemoteFileRequestProto {
    fn from(r: CreateRemoteFilerequest) -> Self {
        CreateRemoteFileRequestProto {
            peer_id: r.peer_id.0,
            file_request_id: r.filerequest_id,
            name: r.name,
            contact_id: r.contact_id.0,
        }
    }
}

impl From<CreateRemoteFileRequestProto> for CreateRemoteFilerequest {
    fn from(proto: CreateRemoteFileRequestProto) -> Self {
        CreateRemoteFilerequest {
            peer_id: proto.peer_id.into(),
            filerequest_id: proto.file_request_id,
            name: proto.name,
            contact_id: proto.contact_id.into(),
        }
    }
}

impl From<&SendStatus> for SendStatusProto {
    fn from(stat: &SendStatus) -> Self {
        match stat {
            SendStatus::Pending => SendStatusProto::Pending,
            SendStatus::InProgress(in_progress) => match in_progress.status {
                InProgressSendStatus::Prepared => SendStatusProto::Prepared,
                InProgressSendStatus::Sending => SendStatusProto::Sending,
                InProgressSendStatus::Interrupted => SendStatusProto::Interrupted,
                InProgressSendStatus::PendingSent => SendStatusProto::PendingSent,
            },
            SendStatus::Sent => SendStatusProto::Success,
            SendStatus::Rejected => SendStatusProto::Rejected,
            SendStatus::Failed => SendStatusProto::Failed,
            SendStatus::Unknown(s) => {
                warn!("Converting unknown SendStatus \"{s}\" to Pending");
                SendStatusProto::Pending
            }
        }
    }
}

impl From<&PendingFile> for PendingFileProto {
    fn from(p: &PendingFile) -> Self {
        let transfer_data = p.status.transfer_data();
        PendingFileProto {
            id: p.id.clone().0,
            file_path: p.file_path.clone(),
            target_file_request_id: p.target_filerequest_id.clone().0,
            send_status: SendStatusProto::from(&p.status).into(),
            transfer_data: transfer_data.map(|it| TransferDataProto {
                progress_bytes: it.progress_bytes,
                transfer_id: it.transfer_id.0.clone(),
                file_size_bytes: it.file_size_bytes,
            }),
            // progress_bytes: transfer_data.map(|d| d.progress_bytes),
            // transfer_id: transfer_data.map(|d| d.transfer_id.0.clone()),
            // file_size_bytes: transfer_data.map(|d| d.file_size_bytes),
            display_name: p.display_name.clone(),
            interruption_reasons: p.interruption_reasons.clone(),
        }
    }
}

impl From<StorePendingFileRequest> for CreatePendingFiles {
    fn from(p: StorePendingFileRequest) -> Self {
        let pending_files = p.pending_files.unwrap();
        CreatePendingFiles {
            file_infos: pending_files.file_infos.into_iter().map(|it| FileInfo { path: it.path, display_name: it.display_name }).collect(),
            target_filerequest_id: pending_files.target_file_request_id.into(),
        }
    }
}

impl From<ReceivedFile> for ReceivedFileProto {
    fn from(r: ReceivedFile) -> Self {
        ReceivedFileProto {
            id: r.id.0,
            contact_id: r.contact_id.map(|id| id.0),
            filerequest_id: r.filerequest_id.0,
            file_name: r.file_name,
            file_path: r.file_path.unwrap_or_default(),
            size: r.file_size_bytes as i64,
            received_at_ms: r.received_at_ms.unwrap_or(0),
        }
    }
}


#[derive(Debug, Error)]
pub enum PublicKeyDeserializationError {
    #[error("Failed to deserialize public dilithium key. Wrong key length?")]
    PublicDilithium(#[from] bincode::error::DecodeError),
    #[error("{0}")]
    PublicEd25519(#[from] PublicEd25519DeserializationError),
}

#[derive(Debug, Error)]
pub enum PublicKeySerializationError {
    #[error("Failed to serialize public dilithium key. Wrong key length?")]
    PublicDilithium(#[from] bincode::error::EncodeError),
}

#[derive(Debug, Error)]
pub enum PrivateKeyDeserializationError {
    #[error("Failed to deserialize private dilithium key. Wrong key length?")]
    PrivateDilithium(#[from] bincode::error::DecodeError),
    #[error("{0}")]
    PrivateEd25519(#[from] PrivateEd25519DeserializationError),
}

#[derive(Debug, Error)]
pub enum PrivateKeySerializationError {
    #[error("Failed to deserialize private dilithium key. Wrong key length?")]
    PrivateDilithium(#[from] bincode::error::EncodeError),
}

impl TryFrom<ContactPublicKeysProto> for ContactPublicKeys {
    type Error = PublicKeyDeserializationError;
    fn try_from(proto: ContactPublicKeysProto) -> Result<Self, Self::Error> {
        Ok(Self {
            dilithium: Box::new(deserialize_dilithium_public_key(proto.dilithium)?),
            ed25519: deserialize_ed25519_public_key(proto.ed25519)?,
        })
    }
}

#[derive(Error, Debug)]
pub enum SelfContactDeserializationError {
    #[error("Proto3 does not have required keys. No compiler support. Missing key: \"{0}\"")]
    Proto3FuckMissingKey(&'static str),
    #[error("Could not deserialize a private key")]
    PrivateKeyDeserialization(#[from] PrivateKeyDeserializationError),
    #[error("Could not deserialize a public key")]
    PublicKeyDeserialization(#[from] PublicKeyDeserializationError),
}

#[derive(Error, Debug)]
pub enum SelfContactSerializationError {
    #[error("Could not deserialize a private key")]
    PrivateKeySerialization(#[from] PrivateKeySerializationError),
    #[error("Could not deserialize a public key")]
    PublicKeySerialization(#[from] PublicKeySerializationError),
}

impl TryFrom<SelfContactProto> for SelfContact {
    type Error = SelfContactDeserializationError;
    fn try_from(value: SelfContactProto) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.into(),
            name: value.name,
            keys: value
                .keys
                .ok_or(SelfContactDeserializationError::Proto3FuckMissingKey(
                    "keys",
                ))?
                .try_into()?,
        })
    }
}

impl TryFrom<SelfContact> for SelfContactProto {
    type Error = SelfContactSerializationError;
    fn try_from(value: SelfContact) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id.0,
            name: value.name,
            keys: Some(value.keys.try_into()?),
        })
    }
}

impl TryFrom<ContactKeys> for ContactKeysProto {
    type Error = SelfContactSerializationError;
    fn try_from(value: ContactKeys) -> Result<Self, Self::Error> {
        Ok(Self {
            private_keys: Some(value.private.try_into()?),
            public_keys: Some(value.public.try_into()?),
        })
    }
}

impl TryFrom<ContactPublicKeys> for ContactPublicKeysProto {
    type Error = PublicKeySerializationError;
    fn try_from(value: ContactPublicKeys) -> Result<Self, Self::Error> {
        Ok(Self {
            dilithium: serialize_dilithium_public_key(&value.dilithium)?,
            ed25519: serialize_ed25519_public_key(&value.ed25519),
        })
    }
}

impl TryFrom<ContactPrivateKeys> for ContactPrivateKeysProto {
    type Error = PrivateKeySerializationError;

    fn try_from(value: ContactPrivateKeys) -> Result<Self, Self::Error> {
        Ok(Self {
            dilithium: serialize_dilithium_private_key(&value.dilithium)?,
            ed25519: serialize_ed25519_private_key(&value.ed25519),
        })
    }
}

impl TryFrom<ContactKeysProto> for ContactKeys {
    type Error = SelfContactDeserializationError;

    fn try_from(value: ContactKeysProto) -> Result<Self, Self::Error> {
        Ok(Self {
            private: value
                .private_keys
                .ok_or(SelfContactDeserializationError::Proto3FuckMissingKey(
                    "private_keys",
                ))?
                .try_into()?,
            public: value
                .public_keys
                .ok_or(SelfContactDeserializationError::Proto3FuckMissingKey(
                    "public_keys",
                ))?
                .try_into()?,
        })
    }
}

impl TryFrom<ContactPrivateKeysProto> for ContactPrivateKeys {
    type Error = PrivateKeyDeserializationError;
    fn try_from(value: ContactPrivateKeysProto) -> Result<Self, Self::Error> {
        Ok(Self {
            dilithium: Box::new(deserialize_dilithium_private_key(value.dilithium)?),
            ed25519: deserialize_ed25519_private_key(value.ed25519)?,
        })
    }
}

impl TryFrom<Contact> for SelfContactPublicProto {
    type Error = PublicKeySerializationError;
    fn try_from(contact: Contact) -> Result<Self, Self::Error> {
        Ok(SelfContactPublicProto {
            name: contact.name,
            contact_id: contact.id.0,
            public_keys: Some(contact.public_keys.try_into()?),
        })
    }
}

impl TryFrom<SelfContactPublicProto> for Contact {
    type Error = SelfContactDeserializationError;
    fn try_from(proto: SelfContactPublicProto) -> Result<Self, Self::Error> {
        Ok(Contact {
            id: proto.contact_id.into(),
            name: proto.name,
            public_keys: proto
                .public_keys
                .ok_or(SelfContactDeserializationError::Proto3FuckMissingKey(
                    "public_keys",
                ))?
                .try_into()?,
        })
    }
}

impl ServerMessage {
    pub fn received_self_contact_invite_over_network() -> Self {
        ServerMessage {
            message: Some(
                server_message::Message::ReceivedSelfContactInviteOverNetwork(
                    ReceivedSelfContactInviteOverNetwork {},
                ),
            ),
        }
    }

    pub fn self_contact_invite_accepted_over_network() -> Self {
        ServerMessage {
            message: Some(
                server_message::Message::SelfContactInviteAcceptedOverNetwork(
                    SelfContactInviteAcceptedOverNetwork {},
                ),
            ),
        }
    }

    pub fn contact_share_accepted_over_network() -> Self {
        ServerMessage {
            message: Some(server_message::Message::ReceivedContactShareOverNetwork(
                ReceivedContactShareOverNetwork {},
            )),
        }
    }

    pub fn database_restored() -> Self {
        ServerMessage {
            message: Some(server_message::Message::DatabaseRestoredFromBackup(
                DatabaseRestoredFromBackup {},
            )),
        }
    }

    pub fn pending_file_status_changed(pending_file_id: &str, status: &SendStatus, target_filerequest_id: &str) -> Self {
        match status {
            SendStatus::InProgress(_) => {
                trace!(?status, "Sending sending file status update");
            }

            SendStatus::Unknown(_) => {
                error!(?status, "Sending sending file status update");
            }
            _ => {
                warn!(?status, "Sending sending file status update");
            }
        }
        let status_proto: SendStatusProto = status.into();
        let transfer_data = status.transfer_data();
        ServerMessage {
            message: Some(server_message::Message::PendingFileStatusChanged(
                PendingFileStatusChanged {
                    pending_file_id: pending_file_id.to_string(),
                    new_status: status_proto.into(),
                    progress_bytes: transfer_data.map(|td| td.progress_bytes),
                    transfer_id: transfer_data.map(|td| td.transfer_id.to_string()),
                    file_size_bytes: transfer_data.map(|it| it.file_size_bytes),
                    target_file_request_id: target_filerequest_id.to_string(),
                    interruption_reasons: vec![],
                },
            )),
        }
    }

    pub fn received_file_status_changed(rf: &ReceivedFile) -> Self {
        let status_proto: ReceiveStatusProto = (&rf.status).into();
        ServerMessage {
            message: Some(server_message::Message::ReceivedFileStatusChanged(
                ReceivedFileStatusChanged {
                    transfer_id: rf
                        .transfer_id
                        .as_ref()
                        .map(|t| t.to_string()),
                    filerequest_id: rf.filerequest_id.to_string(),
                    new_status: status_proto.into(),
                    progress_bytes: rf.progress_bytes,
                    file_size_bytes: rf.file_size_bytes,
                    file_name: rf.file_name.clone(),
                },
            )),
        }
    }
}

impl From<&ReceiveStatus> for ReceiveStatusProto {
    fn from(status: &ReceiveStatus) -> Self {
        match status {
            ReceiveStatus::Receiving => ReceiveStatusProto::ReceiveStatusReceiving,
            ReceiveStatus::Interrupted => ReceiveStatusProto::ReceiveStatusInterrupted,
            ReceiveStatus::Completed => ReceiveStatusProto::ReceiveStatusCompleted,
            ReceiveStatus::Failed => ReceiveStatusProto::ReceiveStatusFailed,
            ReceiveStatus::Unknown(_) => ReceiveStatusProto::ReceiveStatusFailed,
        }
    }
}
