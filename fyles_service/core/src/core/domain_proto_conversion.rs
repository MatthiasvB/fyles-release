use crate::core::backup::{
    BackupOutcome, BackupPasswordStatus, PasswordCheck, PasswordSuggestion, PasswordWarning,
    RestoreOutcome, RestoreRefusal, UnpackedFormat,
};
use crate::core::listen::{ListenFailure, ListenStatus};
use crate::core::settings::SettingsView;
use crypto::{
    ContactKeys, ContactPrivateKeys, ContactPublicKeys, PqSigningPrivateKeyDeserializationError,
    PqSigningPublicKeyDeserializationError, PrivateEd25519DeserializationError,
    PublicEd25519DeserializationError, deserialize_ed25519_private_key,
    deserialize_ed25519_public_key, deserialize_pq_signing_private_key,
    deserialize_pq_signing_public_key, serialize_ed25519_private_key, serialize_ed25519_public_key,
    serialize_pq_signing_private_key, serialize_pq_signing_public_key,
};
use itertools::Itertools;
use thiserror::Error;
use tonic::Status;
use tracing::{error, trace, warn};

use super::domain_models::{
    CreatePendingFiles, CreateRemoteFilerequest, FileInfo, Filerequest, FilerequestAccess,
    InProgressSendStatus, PendingFile, ReceiveStatus, ReceivedFile, RemoteFilerequest, SendStatus,
};
use crate::library::wire::api::TransferDataProto;
use crate::{
    core::brain::types::SignedContactShare,
    core::domain_models::{
        Contact, ContactId, ContactIdParseError, DisplayContact, FylesId, PeerIdWrapper,
        SelfContact, SelfContactInvite,
    },
    core::license::PreparedLicenseRequest,
    core::license::evaluate::Entitlements,
    library::wire::api::{
        self, AccessProto, BackupDataResponse, ContactKeysProto, ContactPrivateKeysProto,
        ContactProto, ContactPublicKeysProto, ContactRemoved,
        ContactShareRejected as ContactShareRejectedProto, ContactUpserted, ContactsChanged,
        CreateRemoteFileRequestProto, EntitlementProto, EntitlementsChanged,
        EvaluateBackupPasswordResponse, FileRequestProto, FilerequestRemoved, FilerequestUpserted,
        GetBackupPasswordStatusResponse, GetEntitlementStatusResponse, GetListenStatusResponse,
        LicenseCheckNotDue, LicenseHttpRequest, ListenFailureProto, ListenStarting,
        ListenStatusChanged, ListenStatusProto, ListeningOnConfiguredPort,
        ListeningOnTemporaryPort, NotListening, OwnedDeviceProto, OwnedDeviceRemoved,
        OwnedDeviceUpserted, OwnedDevicesChanged, PendingFileProto, PendingFileRemoved,
        PendingFileStatusChanged, PendingFilesAdded, PrepareLicenseCheckResponse, PublicProto,
        ReceiveStatusProto, ReceivedContactShareOverNetwork, ReceivedFileProto,
        ReceivedFileStatusChanged, ReceivedFilesChanged, ReceivedSelfContactInviteOverNetwork,
        RemoteFileRequestProto, RemoteFilerequestRemoved, RemoteFilerequestUpserted,
        RemoteFilerequestsChanged, RestoreDataResponse, SelfContactChanged,
        SelfContactInviteAcceptedOverNetwork, SelfContactInviteProto, SelfContactPublicProto,
        SendStatusProto, ServerMessage, SettingsChanged, SpecificUsersProto,
        StorePendingFileRequest, backup_data_response, evaluate_backup_password_response,
        get_backup_password_status_response, get_listen_status_response, listen_failure_proto,
        listen_status_proto, prepare_license_check_response, restore_data_response, server_message,
    },
};

impl From<ListenFailure> for ListenFailureProto {
    fn from(failure: ListenFailure) -> Self {
        let failure = match failure {
            ListenFailure::AddressInUse => {
                listen_failure_proto::Failure::AddressInUse(api::AddressInUse {})
            }
            ListenFailure::AccessDenied => {
                listen_failure_proto::Failure::AccessDenied(api::AccessDenied {})
            }
            ListenFailure::Other(message) => listen_failure_proto::Failure::Other(message),
        };
        ListenFailureProto {
            failure: Some(failure),
        }
    }
}

impl From<ListenStatus> for ListenStatusProto {
    fn from(status: ListenStatus) -> Self {
        let status = match status {
            ListenStatus::Listening(port) => {
                listen_status_proto::Status::Listening(ListeningOnConfiguredPort {
                    port: port.get().into(),
                })
            }
            ListenStatus::Temporary(temporary) => {
                listen_status_proto::Status::Temporary(ListeningOnTemporaryPort {
                    configured: temporary.configured().get().into(),
                    failure: Some(temporary.failure().clone().into()),
                    active: temporary.active().get().into(),
                })
            }
            ListenStatus::NotListening {
                configured,
                failure,
            } => listen_status_proto::Status::NotListening(NotListening {
                configured: configured.map(|port| port.get().into()),
                failure: Some(failure.into()),
            }),
        };
        ListenStatusProto {
            status: Some(status),
        }
    }
}

impl From<Option<ListenStatus>> for GetListenStatusResponse {
    fn from(status: Option<ListenStatus>) -> Self {
        let state = match status {
            Some(status) => get_listen_status_response::State::Status(status.into()),
            None => get_listen_status_response::State::Starting(ListenStarting {}),
        };
        GetListenStatusResponse { state: Some(state) }
    }
}

impl From<BackupOutcome> for BackupDataResponse {
    fn from(outcome: BackupOutcome) -> Self {
        let outcome = match outcome {
            BackupOutcome::Written => backup_data_response::Outcome::Written(api::BackupWritten {}),
            BackupOutcome::PasswordNotSet => {
                backup_data_response::Outcome::PasswordNotSet(api::BackupPasswordNotSet {})
            }
        };
        BackupDataResponse {
            outcome: Some(outcome),
        }
    }
}

impl From<RestoreOutcome> for RestoreDataResponse {
    fn from(outcome: RestoreOutcome) -> Self {
        let outcome = match outcome {
            RestoreOutcome::Restored(UnpackedFormat::Encrypted) => {
                restore_data_response::Outcome::Restored(api::RestoredEncrypted {})
            }
            RestoreOutcome::Restored(UnpackedFormat::LegacyPlain) => {
                restore_data_response::Outcome::RestoredLegacyPlain(api::RestoredLegacyPlain {})
            }
            RestoreOutcome::Refused(RestoreRefusal::NotABackup) => {
                restore_data_response::Outcome::NotABackup(api::BackupNotABackup {})
            }
            RestoreOutcome::Refused(RestoreRefusal::Unsupported) => {
                restore_data_response::Outcome::Unsupported(api::BackupUnsupported {})
            }
            RestoreOutcome::Refused(RestoreRefusal::PasswordRequired) => {
                restore_data_response::Outcome::PasswordRequired(api::BackupPasswordRequired {})
            }
            RestoreOutcome::Refused(RestoreRefusal::WrongPassword) => {
                restore_data_response::Outcome::WrongPassword(api::BackupWrongPassword {})
            }
            RestoreOutcome::Refused(RestoreRefusal::Corrupt) => {
                restore_data_response::Outcome::Corrupt(api::BackupCorrupt {})
            }
        };
        RestoreDataResponse {
            outcome: Some(outcome),
        }
    }
}

impl From<BackupPasswordStatus> for GetBackupPasswordStatusResponse {
    fn from(status: BackupPasswordStatus) -> Self {
        let status = match status {
            BackupPasswordStatus::NotSet => {
                get_backup_password_status_response::Status::Missing(api::BackupPasswordNotSet {})
            }
            BackupPasswordStatus::Set { verification_due } => {
                get_backup_password_status_response::Status::Present(api::BackupPasswordSet {
                    verification_due,
                })
            }
        };
        GetBackupPasswordStatusResponse {
            status: Some(status),
        }
    }
}

impl From<PasswordWarning> for api::PasswordWarning {
    fn from(warning: PasswordWarning) -> Self {
        match warning {
            PasswordWarning::StraightRowsOfKeysAreEasyToGuess => Self::StraightRowsOfKeys,
            PasswordWarning::ShortKeyboardPatternsAreEasyToGuess => Self::ShortKeyboardPatterns,
            PasswordWarning::RepeatsLikeAaaAreEasyToGuess => Self::RepeatedCharacters,
            PasswordWarning::RepeatsLikeAbcAbcAreOnlySlightlyHarderToGuess => Self::RepeatedBlocks,
            PasswordWarning::SequencesLikeAbcAreEasyToGuess => Self::Sequences,
            PasswordWarning::RecentYearsAreEasyToGuess => Self::RecentYears,
            PasswordWarning::DatesAreOftenEasyToGuess => Self::Dates,
        }
    }
}

impl From<PasswordSuggestion> for api::PasswordSuggestion {
    fn from(suggestion: PasswordSuggestion) -> Self {
        match suggestion {
            PasswordSuggestion::UseAFewWordsAvoidCommonPhrases => Self::UseAFewWords,
            PasswordSuggestion::NoNeedForSymbolsDigitsOrUppercaseLetters => Self::NoNeedForSymbols,
            PasswordSuggestion::AddAnotherWordOrTwo => Self::AddAnotherWord,
            PasswordSuggestion::UseALongerKeyboardPatternWithMoreTurns => {
                Self::LongerKeyboardPattern
            }
            PasswordSuggestion::AvoidRepeatedWordsAndCharacters => Self::AvoidRepeats,
            PasswordSuggestion::AvoidSequences => Self::AvoidSequences,
            PasswordSuggestion::AvoidRecentYears => Self::AvoidRecentYears,
            PasswordSuggestion::AvoidYearsThatAreAssociatedWithYou => Self::AvoidPersonalYears,
            PasswordSuggestion::AvoidDatesAndYearsThatAreAssociatedWithYou => {
                Self::AvoidPersonalDates
            }
        }
    }
}

impl From<PasswordCheck> for EvaluateBackupPasswordResponse {
    fn from(check: PasswordCheck) -> Self {
        let check = match check {
            PasswordCheck::TooShort => {
                evaluate_backup_password_response::Check::TooShort(api::PasswordTooShort {})
            }
            PasswordCheck::Guessable {
                warning,
                suggestions,
            } => evaluate_backup_password_response::Check::Guessable(api::PasswordGuessable {
                warning: warning.map(|warning| api::PasswordWarning::from(warning) as i32),
                suggestions: suggestions
                    .into_iter()
                    .map(|suggestion| api::PasswordSuggestion::from(suggestion) as i32)
                    .collect(),
            }),
            PasswordCheck::NoWeaknessFound => {
                evaluate_backup_password_response::Check::NoWeaknessFound(
                    api::PasswordNoWeaknessFound {},
                )
            }
        };
        EvaluateBackupPasswordResponse { check: Some(check) }
    }
}

impl From<license_format::Entitlement> for EntitlementProto {
    fn from(entitlement: license_format::Entitlement) -> Self {
        EntitlementProto {
            id: entitlement.id,
            expires_at: entitlement.expires_at,
        }
    }
}

impl From<Entitlements> for EntitlementsChanged {
    fn from(entitlements: Entitlements) -> Self {
        EntitlementsChanged {
            entitlements: entitlements.entitlements.into_iter().map_into().collect(),
            provisional: entitlements.provisional,
        }
    }
}

impl From<Entitlements> for GetEntitlementStatusResponse {
    fn from(entitlements: Entitlements) -> Self {
        GetEntitlementStatusResponse {
            entitlements: entitlements.entitlements.into_iter().map_into().collect(),
            provisional: entitlements.provisional,
        }
    }
}

impl From<Option<PreparedLicenseRequest>> for PrepareLicenseCheckResponse {
    fn from(prepared: Option<PreparedLicenseRequest>) -> Self {
        let outcome = match prepared {
            Some(request) => prepare_license_check_response::Outcome::Request(LicenseHttpRequest {
                body: request.body,
            }),
            None => prepare_license_check_response::Outcome::NotDue(LicenseCheckNotDue {}),
        };
        PrepareLicenseCheckResponse {
            outcome: Some(outcome),
        }
    }
}

impl From<&Contact> for ContactProto {
    fn from(contact: &Contact) -> Self {
        ContactProto {
            id: contact.id().to_string(),
            name: contact.name.clone(),
        }
    }
}

impl TryFrom<ContactProto> for DisplayContact {
    type Error = ContactIdParseError;

    fn try_from(proto: ContactProto) -> Result<Self, Self::Error> {
        Ok(Self {
            id: proto.id.parse()?,
            name: proto.name,
        })
    }
}

impl From<DisplayContact> for ContactProto {
    fn from(contact: DisplayContact) -> Self {
        ContactProto {
            id: contact.id.to_string(),
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
                        contact_ids: contact_ids.into_iter().map(|id| id.to_string()).collect(),
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
                contact_ids: users
                    .contact_ids
                    .into_iter()
                    .map(|id| id.parse())
                    .collect::<Result<_, ContactIdParseError>>()
                    .map_err(|e| Status::invalid_argument(e.to_string()))?,
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
            contact_id: r.contact_id.to_string(),
        }
    }
}

impl TryFrom<RemoteFileRequestProto> for RemoteFilerequest {
    type Error = ContactIdParseError;

    fn try_from(proto: RemoteFileRequestProto) -> Result<Self, Self::Error> {
        Ok(RemoteFilerequest {
            id: proto.id.into(),
            peer_id: proto.peer_id.into(),
            filerequest_id: proto.file_request_id.into(),
            name: proto.name,
            contact_id: proto.contact_id.parse()?,
        })
    }
}

impl From<CreateRemoteFilerequest> for CreateRemoteFileRequestProto {
    fn from(r: CreateRemoteFilerequest) -> Self {
        CreateRemoteFileRequestProto {
            peer_id: r.peer_id.0,
            file_request_id: r.filerequest_id,
            name: r.name,
            contact_id: r.contact_id.to_string(),
        }
    }
}

impl TryFrom<CreateRemoteFileRequestProto> for CreateRemoteFilerequest {
    type Error = ContactIdParseError;

    fn try_from(proto: CreateRemoteFileRequestProto) -> Result<Self, Self::Error> {
        Ok(CreateRemoteFilerequest {
            peer_id: proto.peer_id.into(),
            filerequest_id: proto.file_request_id,
            name: proto.name,
            contact_id: proto.contact_id.parse()?,
        })
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
            display_name: p.display_name.clone(),
            interruption_reasons: p.interruption_reasons.clone(),
        }
    }
}

impl From<PendingFile> for PendingFileProto {
    fn from(p: PendingFile) -> Self {
        let send_status = SendStatusProto::from(&p.status).into();
        let transfer_data = p.status.into_transfer_data();
        PendingFileProto {
            id: p.id.0,
            file_path: p.file_path,
            target_file_request_id: p.target_filerequest_id.0,
            send_status,
            transfer_data: transfer_data.map(|it| TransferDataProto {
                progress_bytes: it.progress_bytes,
                transfer_id: it.transfer_id.0,
                file_size_bytes: it.file_size_bytes,
            }),
            display_name: p.display_name,
            interruption_reasons: p.interruption_reasons,
        }
    }
}

impl From<StorePendingFileRequest> for CreatePendingFiles {
    fn from(p: StorePendingFileRequest) -> Self {
        let pending_files = p.pending_files.unwrap();
        CreatePendingFiles {
            file_infos: pending_files
                .file_infos
                .into_iter()
                .map(|it| FileInfo {
                    path: it.path,
                    display_name: it.display_name,
                })
                .collect(),
            target_filerequest_id: pending_files.target_file_request_id.into(),
        }
    }
}

impl From<ReceivedFile> for ReceivedFileProto {
    fn from(r: ReceivedFile) -> Self {
        ReceivedFileProto {
            id: r.id.0,
            contact_id: r.contact_id.map(|id| id.to_string()),
            filerequest_id: r.filerequest_id.0,
            file_name: r.file_name,
            file_path: r.file_path.unwrap_or_default(),
            size: r.file_size_bytes as i64,
            received_at_ms: r.received_at_ms.unwrap_or(0),
            persisted_as_name: r.persisted_as_name,
        }
    }
}

#[derive(Debug, Error)]
pub enum PublicKeyDeserializationError {
    #[error("{0}")]
    PqSigning(#[from] PqSigningPublicKeyDeserializationError),
    #[error("{0}")]
    PublicEd25519(#[from] PublicEd25519DeserializationError),
}

#[derive(Debug, Error)]
pub enum PrivateKeyDeserializationError {
    #[error("{0}")]
    PqSigning(#[from] PqSigningPrivateKeyDeserializationError),
    #[error("{0}")]
    PrivateEd25519(#[from] PrivateEd25519DeserializationError),
}

impl TryFrom<ContactPublicKeysProto> for ContactPublicKeys {
    type Error = PublicKeyDeserializationError;
    fn try_from(proto: ContactPublicKeysProto) -> Result<Self, Self::Error> {
        Ok(Self {
            pq_signing: Box::new(deserialize_pq_signing_public_key(proto.pq_signing)?),
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

impl TryFrom<SelfContactInviteProto> for SelfContactInvite {
    type Error = SelfContactDeserializationError;
    fn try_from(value: SelfContactInviteProto) -> Result<Self, Self::Error> {
        Ok(Self {
            contact: SelfContact {
                name: value.name,
                keys: value
                    .keys
                    .ok_or(SelfContactDeserializationError::Proto3FuckMissingKey(
                        "keys",
                    ))?
                    .try_into()?,
            },
            inviter_peer_id: value.inviter_peer_id.into(),
            inviter_device_name: value.inviter_device_name,
        })
    }
}

impl From<SelfContactInvite> for SelfContactInviteProto {
    fn from(value: SelfContactInvite) -> Self {
        Self {
            name: value.contact.name,
            keys: Some(value.contact.keys.into()),
            inviter_peer_id: value.inviter_peer_id.0,
            inviter_device_name: value.inviter_device_name,
        }
    }
}

impl From<ContactKeys> for ContactKeysProto {
    fn from(value: ContactKeys) -> Self {
        // Only the private keys are carried; the receiver re-derives the public keys.
        Self {
            private_keys: Some(value.private.into()),
        }
    }
}

impl From<ContactPublicKeys> for ContactPublicKeysProto {
    fn from(value: ContactPublicKeys) -> Self {
        Self {
            pq_signing: serialize_pq_signing_public_key(&value.pq_signing),
            ed25519: serialize_ed25519_public_key(&value.ed25519),
        }
    }
}

impl From<ContactPrivateKeys> for ContactPrivateKeysProto {
    fn from(value: ContactPrivateKeys) -> Self {
        Self {
            pq_signing: serialize_pq_signing_private_key(&value.pq_signing),
            ed25519: serialize_ed25519_private_key(&value.ed25519),
        }
    }
}

impl TryFrom<ContactKeysProto> for ContactKeys {
    type Error = SelfContactDeserializationError;

    fn try_from(value: ContactKeysProto) -> Result<Self, Self::Error> {
        let private = value
            .private_keys
            .ok_or(SelfContactDeserializationError::Proto3FuckMissingKey(
                "private_keys",
            ))?
            .try_into()?;
        // The public keys are derived from the private ones rather than transmitted.
        Ok(ContactKeys::from_private(private))
    }
}

impl TryFrom<ContactPrivateKeysProto> for ContactPrivateKeys {
    type Error = PrivateKeyDeserializationError;
    fn try_from(value: ContactPrivateKeysProto) -> Result<Self, Self::Error> {
        Ok(Self {
            pq_signing: Box::new(deserialize_pq_signing_private_key(value.pq_signing)?),
            ed25519: deserialize_ed25519_private_key(value.ed25519)?,
        })
    }
}

impl TryFrom<SignedContactShare> for SelfContactPublicProto {
    type Error = bincode::error::EncodeError;
    fn try_from(share: SignedContactShare) -> Result<Self, Self::Error> {
        Ok(SelfContactPublicProto {
            signed_share: bincode::serde::encode_to_vec(&share, crypto::BINCODE_CONFIG)?,
        })
    }
}

impl TryFrom<SelfContactPublicProto> for SignedContactShare {
    type Error = bincode::error::DecodeError;
    fn try_from(proto: SelfContactPublicProto) -> Result<Self, Self::Error> {
        bincode::serde::decode_from_slice(&proto.signed_share, crypto::BINCODE_CONFIG)
            .map(|(share, _)| share)
    }
}

pub(crate) fn decode_share(
    proto: Option<SelfContactPublicProto>,
) -> Result<SignedContactShare, Status> {
    proto
        .ok_or(Status::invalid_argument("Missing sharable_contact"))?
        .try_into()
        .map_err(|e: bincode::error::DecodeError| Status::invalid_argument(e.to_string()))
}

pub(crate) fn parse_contact_id(id: String) -> Result<ContactId, Status> {
    id.parse()
        .map_err(|e: ContactIdParseError| Status::invalid_argument(e.to_string()))
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

    pub fn contact_share_rejected() -> Self {
        ServerMessage {
            message: Some(server_message::Message::ContactShareRejected(
                ContactShareRejectedProto {},
            )),
        }
    }

    pub fn contacts_changed(contacts: Vec<DisplayContact>) -> Self {
        ServerMessage {
            message: Some(server_message::Message::ContactsChanged(ContactsChanged {
                contacts: contacts.into_iter().map(ContactProto::from).collect(),
            })),
        }
    }

    /// Carries every pending file alongside the remote filerequests, so the client rebuilds the
    /// nested structure without a refetch.
    pub fn remote_filerequests_changed(
        remote_filerequests: Vec<RemoteFilerequest>,
        pending_files: Vec<PendingFile>,
    ) -> Self {
        ServerMessage {
            message: Some(server_message::Message::RemoteFilerequestsChanged(
                RemoteFilerequestsChanged {
                    remote_file_requests: remote_filerequests
                        .into_iter()
                        .map(RemoteFileRequestProto::from)
                        .collect(),
                    pending_files: pending_files
                        .into_iter()
                        .map(PendingFileProto::from)
                        .collect(),
                },
            )),
        }
    }

    pub fn owned_devices_changed(devices: Vec<(PeerIdWrapper, String)>) -> Self {
        ServerMessage {
            message: Some(server_message::Message::OwnedDevicesChanged(
                OwnedDevicesChanged {
                    devices: devices
                        .into_iter()
                        .map(|(peer_id, name)| OwnedDeviceProto {
                            peer_id: peer_id.0,
                            name,
                        })
                        .collect(),
                },
            )),
        }
    }

    pub fn self_contact_changed(self_contact: DisplayContact) -> Self {
        ServerMessage {
            message: Some(server_message::Message::SelfContactChanged(
                SelfContactChanged {
                    self_contact: Some(ContactProto::from(self_contact)),
                },
            )),
        }
    }

    pub fn settings_changed(view: SettingsView) -> Self {
        ServerMessage {
            message: Some(server_message::Message::SettingsChanged(SettingsChanged {
                view: view.0,
            })),
        }
    }

    pub fn listen_status_changed(status: ListenStatus) -> Self {
        ServerMessage {
            message: Some(server_message::Message::ListenStatusChanged(
                ListenStatusChanged {
                    status: Some(status.into()),
                },
            )),
        }
    }

    pub fn received_files_changed(filerequest_id: &FylesId) -> Self {
        ServerMessage {
            message: Some(server_message::Message::ReceivedFilesChanged(
                ReceivedFilesChanged {
                    filerequest_id: filerequest_id.to_string(),
                },
            )),
        }
    }

    pub fn contact_upserted(contact: DisplayContact) -> Self {
        ServerMessage {
            message: Some(server_message::Message::ContactUpserted(ContactUpserted {
                contact: Some(ContactProto::from(contact)),
            })),
        }
    }

    pub fn contact_removed(contact_id: &ContactId) -> Self {
        ServerMessage {
            message: Some(server_message::Message::ContactRemoved(ContactRemoved {
                contact_id: contact_id.to_string(),
            })),
        }
    }

    pub fn filerequest_upserted(filerequest: &Filerequest) -> Self {
        ServerMessage {
            message: Some(server_message::Message::FilerequestUpserted(
                FilerequestUpserted {
                    file_request: Some(FileRequestProto::from(filerequest)),
                },
            )),
        }
    }

    pub fn filerequest_removed(id: &FylesId) -> Self {
        ServerMessage {
            message: Some(server_message::Message::FilerequestRemoved(
                FilerequestRemoved { id: id.to_string() },
            )),
        }
    }

    pub fn remote_filerequest_upserted(remote_filerequest: RemoteFilerequest) -> Self {
        ServerMessage {
            message: Some(server_message::Message::RemoteFilerequestUpserted(
                RemoteFilerequestUpserted {
                    remote_file_request: Some(RemoteFileRequestProto::from(remote_filerequest)),
                },
            )),
        }
    }

    pub fn remote_filerequest_removed(id: &FylesId) -> Self {
        ServerMessage {
            message: Some(server_message::Message::RemoteFilerequestRemoved(
                RemoteFilerequestRemoved { id: id.to_string() },
            )),
        }
    }

    pub fn pending_files_added(pending_files: Vec<PendingFile>) -> Self {
        ServerMessage {
            message: Some(server_message::Message::PendingFilesAdded(
                PendingFilesAdded {
                    pending_files: pending_files
                        .into_iter()
                        .map(PendingFileProto::from)
                        .collect(),
                },
            )),
        }
    }

    pub fn pending_file_removed(id: &FylesId, target_filerequest_id: &FylesId) -> Self {
        ServerMessage {
            message: Some(server_message::Message::PendingFileRemoved(
                PendingFileRemoved {
                    id: id.to_string(),
                    target_file_request_id: target_filerequest_id.to_string(),
                },
            )),
        }
    }

    pub fn owned_device_upserted(peer_id: PeerIdWrapper, name: String) -> Self {
        ServerMessage {
            message: Some(server_message::Message::OwnedDeviceUpserted(
                OwnedDeviceUpserted {
                    device: Some(OwnedDeviceProto {
                        peer_id: peer_id.0,
                        name,
                    }),
                },
            )),
        }
    }

    pub fn owned_device_removed(peer_id: &PeerIdWrapper) -> Self {
        ServerMessage {
            message: Some(server_message::Message::OwnedDeviceRemoved(
                OwnedDeviceRemoved {
                    peer_id: peer_id.to_string(),
                },
            )),
        }
    }

    /// `interruption_reason` is what *this* transition recorded, if anything. The client appends it
    /// to the reasons it already holds, so the accumulated history never needs re-sending.
    pub fn pending_file_status_changed(
        pending_file_id: &str,
        status: &SendStatus,
        target_filerequest_id: &str,
        interruption_reason: Option<String>,
    ) -> Self {
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
                    interruption_reasons: interruption_reason.into_iter().collect(),
                },
            )),
        }
    }

    pub fn received_file_status_changed(rf: &ReceivedFile) -> Self {
        let status_proto: ReceiveStatusProto = (&rf.status).into();
        ServerMessage {
            message: Some(server_message::Message::ReceivedFileStatusChanged(
                ReceivedFileStatusChanged {
                    transfer_id: rf.transfer_id.as_ref().map(|t| t.to_string()),
                    filerequest_id: rf.filerequest_id.to_string(),
                    new_status: status_proto.into(),
                    progress_bytes: rf.progress_bytes,
                    file_size_bytes: rf.file_size_bytes,
                    file_name: rf.file_name.clone(),
                    persisted_as_name: rf.persisted_as_name.clone(),
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
