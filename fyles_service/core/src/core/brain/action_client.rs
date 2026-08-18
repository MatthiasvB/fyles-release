use std::collections::HashMap;

use crate::{
    core::{
        brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
        domain_models::{
            Contact, ContactId, CreateFilerequest, CreatePendingFiles, CreateRemoteFilerequest,
            DisplayContact, Filerequest, FilerequestLocation, FylesId, PeerIdWrapper, PendingFile,
            ReceivedFile, RemoteFilerequest, SelfContactInvite,
        },
        p2p::NodeStatusInfo,
    },
    io_controller::Uri,
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
    ClientHeartbeat { foreground: bool },

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

    UpdateSelfContactName(BrainRequest<String, FilerequestResult<()>>),
    UpdateIdentity(BrainRequest<SelfContactInvite, FilerequestResult<()>>),
    GetSelfContactDisplay(BrainRequest<(), FilerequestResult<DisplayContact>>),
    SharePublicSelfContact(BrainRequest<(), FilerequestResult<Contact>>),
    RegisterContact(BrainRequest<Contact, FilerequestResult<()>>),

    RegisterSelfContactInviteChallenge(BrainRequest<(), SelfContactInviteChallenge>),
    UnregisterSelfContactInviteChallenge(BrainRequest<SelfContactInviteChallenge, ()>),
    UseSelfContactInviteChallenge(BrainRequest<(SelfContactInviteChallenge, PeerIdWrapper), ()>),

    RegisterContactShareChallenge(BrainRequest<(), ContactShareChallenge>),
    UnregisterContactShareChallenge(BrainRequest<ContactShareChallenge, ()>),
    UseContactShareChallenge(BrainRequest<(ContactShareChallenge, PeerIdWrapper), ()>),

    RenameThisDevice(BrainRequest<String, FilerequestResult<()>>),
    RemoveOwnedDevice(BrainRequest<PeerIdWrapper, FilerequestResult<()>>),
    ListOwnedDevices(BrainRequest<(), FilerequestResult<Vec<(PeerIdWrapper, String)>>>),

    BackupData(BrainRequest<(), FilerequestResult<()>>),
    RestoreData(BrainRequest<Uri, FilerequestResult<()>>),

    /// Opaque settings request/response relay.
    Settings(BrainRequest<Vec<u8>, FilerequestResult<Vec<u8>>>),
}
