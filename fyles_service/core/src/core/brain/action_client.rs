use std::collections::HashMap;

use crate::{
    core::{
        brain::types::{ContactShareChallenge, SelfContactInviteChallenge},
        domain_models::{
            Contact, ContactId, CreateFilerequest, CreatePendingFiles, CreateRemoteFilerequest,
            DisplayContact, Filerequest, FylesId, PeerIdWrapper, PendingFile, ReceivedFile,
            RemoteFilerequest, SelfContact,
        },
        p2p::NodeStatusInfo,
    },
    io_controller::Uri,
};

use super::types::{BrainRequest, CreationResult, FilerequestResult};

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
    GetFullSelfContact(BrainRequest<(), FilerequestResult<SelfContact>>),
    ListContacts(BrainRequest<(), FilerequestResult<Vec<DisplayContact>>>),
    // CreateContact(BrainRequest<CreateContact, FilerequestResult<Contact>>),
    UpdateContact(BrainRequest<DisplayContact, FilerequestResult<()>>),
    DeleteContact(BrainRequest<ContactId, FilerequestResult<()>>),

    // System operations
    Shutdown(BrainRequest<(), FilerequestResult<bool>>),

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

    UpdateSelfContactName(BrainRequest<String, FilerequestResult<()>>),
    UpdateIdentity(BrainRequest<SelfContact, FilerequestResult<()>>),
    GetSelfContactDisplay(BrainRequest<(), FilerequestResult<DisplayContact>>),
    SharePublicSelfContact(BrainRequest<(), FilerequestResult<Contact>>),
    RegisterContact(BrainRequest<Contact, FilerequestResult<()>>),

    RegisterSelfContactInviteChallenge(BrainRequest<(), SelfContactInviteChallenge>),
    UnregisterSelfContactInviteChallenge(BrainRequest<SelfContactInviteChallenge, ()>),
    UseSelfContactInviteChallenge(BrainRequest<(SelfContactInviteChallenge, PeerIdWrapper), ()>),

    RegisterContactShareChallenge(BrainRequest<(), ContactShareChallenge>),
    UnregisterContactShareChallenge(BrainRequest<ContactShareChallenge, ()>),
    UseContactShareChallenge(BrainRequest<(ContactShareChallenge, PeerIdWrapper), ()>),

    BackupData(BrainRequest<(), FilerequestResult<()>>),
    RestoreData(BrainRequest<Uri, FilerequestResult<()>>),

    UpdateSettings(BrainRequest<Vec<u8>, FilerequestResult<()>>),
    GetSettings(BrainRequest<(), FilerequestResult<Vec<u8>>>),
}
