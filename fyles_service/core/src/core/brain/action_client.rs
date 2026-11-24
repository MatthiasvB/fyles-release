use crate::core::backup::{
    BackupOutcome, BackupPasswordStatus, NewBackupPassword, Password, RestoreOutcome,
    RestoreRequest,
};
use crate::core::listen::{ListenPort, ListenReport};
use crate::core::settings::{SettingsMutation, SettingsView};
use std::collections::HashMap;

use crate::core::{
    brain::types::{
        ContactRegistration, ContactShareChallenge, SelfContactInviteChallenge, SignedContactShare,
    },
    domain_models::{
        Contact, ContactId, CreateFilerequest, CreatePendingFiles, CreateRemoteFilerequest,
        DisplayContact, Filerequest, FilerequestLocation, FylesId, GivenName, PeerIdWrapper,
        PendingFile, ReceivedFile, RemoteFilerequest, SelfContactInvite,
    },
    license::PreparedLicenseRequest,
    license::evaluate::Entitlements,
    p2p::NodeStatusInfo,
};

use super::types::{BrainRequest, CreationResult, FilerequestResult};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientAction {
    WaitForReady(BrainRequest<(), bool>),

    // Filerequest operations
    CreateFilerequest(BrainRequest<CreateFilerequest, CreationResult>),
    ReadFilerequest(BrainRequest<FylesId, FilerequestResult<Filerequest>>),
    UpdateFilerequest(BrainRequest<Filerequest, FilerequestResult<()>>),
    DeleteFilerequest(BrainRequest<FylesId, FilerequestResult<()>>),
    ListFilerequests(BrainRequest<(), FilerequestResult<Vec<Filerequest>>>),

    // Contact operations
    GetContactName(BrainRequest<ContactId, FilerequestResult<String>>),
    GetContactNames(BrainRequest<Vec<ContactId>, FilerequestResult<HashMap<ContactId, String>>>),
    GetContact(BrainRequest<ContactId, FilerequestResult<Contact>>),
    GetFullSelfContact(BrainRequest<(), FilerequestResult<SelfContactInvite>>),
    ListContacts(BrainRequest<(), FilerequestResult<Vec<DisplayContact>>>),
    // CreateContact(BrainRequest<CreateContact, FilerequestResult<Contact>>),
    UpdateContact(BrainRequest<DisplayContact, FilerequestResult<()>>),
    DeleteContact(BrainRequest<ContactId, FilerequestResult<()>>),

    // System operations
    Shutdown(BrainRequest<(), FilerequestResult<bool>>),
    ClientHeartbeat {
        foreground: bool,
    },

    // P2P operations
    GetNodeStatus(BrainRequest<(), FilerequestResult<NodeStatusInfo>>),
    GetNodePeerId(BrainRequest<(), FilerequestResult<String>>),

    // Remote filerequest actions
    CreateRemoteFilerequest(BrainRequest<CreateRemoteFilerequest, FilerequestResult<FylesId>>),
    GetRemoteFilerequest(BrainRequest<FylesId, FilerequestResult<RemoteFilerequest>>),
    GetRemoteFilerequestsByContact(
        BrainRequest<ContactId, FilerequestResult<Vec<RemoteFilerequest>>>,
    ),
    GetAllRemoteFilerequests(BrainRequest<(), FilerequestResult<Vec<RemoteFilerequest>>>),
    DeleteRemoteFilerequest(BrainRequest<FylesId, FilerequestResult<bool>>),
    UpdateRemoteFilerequest(BrainRequest<(FylesId, String), FilerequestResult<()>>),

    // Pending file actions
    CreatePendingFiles(BrainRequest<CreatePendingFiles, FilerequestResult<Vec<FylesId>>>),
    GetPendingFile(BrainRequest<FylesId, FilerequestResult<PendingFile>>),
    GetPendingFiles(BrainRequest<FylesId, FilerequestResult<Vec<PendingFile>>>),
    GetAllPendingFiles(BrainRequest<(), FilerequestResult<Vec<PendingFile>>>),
    DeletePendingFile(BrainRequest<FylesId, FilerequestResult<()>>),

    // Received file actions
    ListReceivedFilesForRequest(BrainRequest<FylesId, FilerequestResult<Vec<ReceivedFile>>>),
    DeleteReceivedFile(BrainRequest<FylesId, FilerequestResult<()>>),

    /// Resolve a (local) filerequest's on-disk folder for opening files/folders
    /// in the UI. Returns a [`FilerequestLocation`] — an absolute path on hosts
    /// with a directly-accessible filesystem (desktop), else a path relative to
    /// the drop root on scoped-storage hosts (Android/iOS).
    ResolveFilerequestLocation(BrainRequest<FylesId, FilerequestResult<FilerequestLocation>>),

    UpdateSelfContactName(BrainRequest<GivenName, FilerequestResult<()>>),
    UpdateIdentity(BrainRequest<SelfContactInvite, FilerequestResult<()>>),
    GetSelfContactDisplay(BrainRequest<(), FilerequestResult<DisplayContact>>),
    SharePublicSelfContact(BrainRequest<(), FilerequestResult<SignedContactShare>>),
    RegisterContact(BrainRequest<ContactRegistration, FilerequestResult<()>>),
    DecodeSignedContact(BrainRequest<SignedContactShare, Option<DisplayContact>>),

    RegisterSelfContactInviteChallenge(BrainRequest<(), SelfContactInviteChallenge>),
    UnregisterSelfContactInviteChallenge(BrainRequest<SelfContactInviteChallenge, ()>),
    UseSelfContactInviteChallenge(BrainRequest<(SelfContactInviteChallenge, PeerIdWrapper), ()>),

    RegisterContactShareChallenge(BrainRequest<(), ContactShareChallenge>),
    UnregisterContactShareChallenge(BrainRequest<ContactShareChallenge, ()>),
    UseContactShareChallenge(BrainRequest<(ContactShareChallenge, PeerIdWrapper), ()>),

    RenameThisDevice(BrainRequest<GivenName, FilerequestResult<()>>),
    RemoveOwnedDevice(BrainRequest<PeerIdWrapper, FilerequestResult<()>>),
    ListOwnedDevices(BrainRequest<(), FilerequestResult<Vec<(PeerIdWrapper, String)>>>),

    BackupData(BrainRequest<(), FilerequestResult<BackupOutcome>>),
    RestoreData(BrainRequest<RestoreRequest, FilerequestResult<RestoreOutcome>>),
    GetBackupPasswordStatus(BrainRequest<(), FilerequestResult<BackupPasswordStatus>>),
    SetBackupPassword(BrainRequest<NewBackupPassword, FilerequestResult<()>>),
    VerifyBackupPassword(BrainRequest<Password, FilerequestResult<bool>>),
    SnoozeBackupPasswordVerification(BrainRequest<(), FilerequestResult<()>>),

    GetSettings(BrainRequest<(), FilerequestResult<SettingsView>>),
    MutateSettings(BrainRequest<SettingsMutation, FilerequestResult<SettingsView>>),

    GetListenStatus(BrainRequest<(), ListenReport>),
    SetListenPort(BrainRequest<ListenPort, FilerequestResult<()>>),

    GetEntitlements(BrainRequest<(), FilerequestResult<Entitlements>>),
    GetLicenseIdentity(BrainRequest<(), FilerequestResult<String>>),
    PrepareLicenseCheck(BrainRequest<bool, FilerequestResult<Option<PreparedLicenseRequest>>>),
    SubmitLicenseCheckResponse(BrainRequest<(u16, Vec<u8>), FilerequestResult<Entitlements>>),
}
