use std::cmp::min;
use std::fmt::Display;
use std::time::Duration;

use autosurgeon::{Hydrate, Reconcile};
use crypto::{ContactKeys, ContactPublicKeys};
use derive_more::{AsRef, Deref, Display, FromStr};
use serde::{Deserialize, Serialize};
#[cfg(any(test, feature = "test-support"))]
use tempfile::TempDir;
use tracing::trace;
use uuid::Uuid;

use crate::library::util::duration_ext::DurationExt;

macro_rules! creatable {
    // Match variant with doc strings for fields
    ($createname:ident $name:ident {
        $(
            $(#[doc = $doc:literal])*
            $pub:vis $field:ident : $ty:ty
        ),* $(,)?
    }) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            pub id: FylesId,
            $(
                $(#[doc = $doc])*
                $pub $field: $ty
            ),*
        }

        #[allow(unused)]
        #[derive(Debug, Clone)]
        pub struct $createname {
            $(
                $(#[doc = $doc])*
                $pub $field: $ty
            ),*
        }
    };
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Display,
    FromStr,
    AsRef,
    Serialize,
    Deserialize,
    Reconcile,
    Hydrate,
)]
#[as_ref(forward)]
#[repr(transparent)]
pub struct ContactId(pub String);
impl From<String> for ContactId {
    fn from(id: String) -> Self {
        ContactId(id)
    }
}
impl From<&str> for ContactId {
    fn from(id: &str) -> Self {
        ContactId(id.into())
    }
}

impl Default for ContactId {
    fn default() -> Self {
        Self::new()
    }
}

impl ContactId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

#[derive(
    Debug,
    Deref,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Display,
    FromStr,
    AsRef,
    Serialize,
    Deserialize,
    Reconcile,
    Hydrate,
)]
#[as_ref(forward)]
#[repr(transparent)]
pub struct FylesId(pub String);
impl From<String> for FylesId {
    fn from(id: String) -> Self {
        FylesId(id)
    }
}
impl From<&str> for FylesId {
    fn from(id: &str) -> Self {
        FylesId(id.into())
    }
}

impl Default for FylesId {
    fn default() -> Self {
        Self::new()
    }
}

impl FylesId {
    pub fn new() -> Self {
        FylesId(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Display, FromStr, AsRef, Deref, Reconcile, Hydrate)]
#[as_ref(forward)]
#[repr(transparent)]
pub struct PeerIdWrapper(pub String);
impl From<String> for PeerIdWrapper {
    fn from(id: String) -> Self {
        PeerIdWrapper(id)
    }
}
impl From<&str> for PeerIdWrapper {
    fn from(id: &str) -> Self {
        PeerIdWrapper(id.into())
    }
}

#[cfg(any(test, feature = "test-support"))]
impl PeerIdWrapper {
    pub fn for_test() -> Self {
        PeerIdWrapper(Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone)]
pub enum FilerequestAccess {
    Public,
    Audience { contact_ids: Vec<ContactId> },
}

impl FilerequestAccess {
    pub fn is_accessible_by(&self, contact_id: Option<ContactId>) -> bool {
        trace!("Checking access for contact_id: {:?}", contact_id);
        match self {
            FilerequestAccess::Public => true,
            FilerequestAccess::Audience { contact_ids } => contact_id
                .map(|id| contact_ids.contains(&id))
                .unwrap_or(false),
        }
    }
}

creatable!(CreateFilerequest Filerequest {
    pub title: String,
    pub description: String,
    pub access: FilerequestAccess,
    pub is_active: bool,
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contact {
    pub id: ContactId,
    pub name: String,
    pub public_keys: ContactPublicKeys,
}

#[cfg(any(test, feature = "test-support"))]
impl Contact {
    pub fn for_test() -> Self {
        Contact {
            id: ContactId::new(),
            name: "Test Contact".into(),
            public_keys: ContactPublicKeys::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DisplayContact {
    pub id: ContactId,
    pub name: String,
}

impl From<Contact> for DisplayContact {
    fn from(contact: Contact) -> Self {
        DisplayContact {
            id: contact.id,
            name: contact.name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfContact {
    pub id: ContactId,
    pub name: String,
    pub keys: ContactKeys,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransferData {
    pub progress_bytes: u64,
    pub file_size_bytes: u64,
    pub transfer_id: FylesId,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SendStatus {
    Pending,
    InProgress(InProgress),
    Sent,
    Rejected,
    Failed,
    /// This is a catch-all for any status that is not recognized. This is useful
    /// for example when the status is read from a database and the status has been
    /// updated in a newer version of the software.
    Unknown(String),
}

impl From<InProgress> for SendStatus {
    fn from(value: InProgress) -> Self {
        Self::InProgress(value)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InProgressSendStatus {
    Prepared,
    Sending,
    Interrupted,
    /// All bytes transferred, but pending acknowledgement for receiving peer.
    /// This status is ephemeral and must NOT be persisted to the database.
    PendingSent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InProgress {
    pub status: InProgressSendStatus,
    pub transfer_data: TransferData,
}

impl SendStatus {
    /// Returns the status tag string used for DB persistence.
    ///
    /// Returns `Err` for statuses that must not be persisted (`PendingSent`, `Unknown`).
    pub fn status_tag(&self) -> Result<&'static str, String> {
        match self {
            SendStatus::Pending => Ok("Pending"),
            SendStatus::InProgress(status) => match status.status {
                InProgressSendStatus::Prepared => Err("Prepared is an ephemeral status and must not be persisted".into()),
                InProgressSendStatus::Sending => Ok("Sending"),
                InProgressSendStatus::Interrupted => Ok("Interrupted"),
                InProgressSendStatus::PendingSent => {
                    Err("PendingSent is an ephemeral status and must not be persisted".into())
                }
            },
            SendStatus::Sent => Ok("Sent"),
            SendStatus::Rejected => Ok("Rejected"),
            SendStatus::Failed => Ok("Failed"),
            SendStatus::Unknown(s) => Err(format!("Unknown status '{s}' must not be persisted")),
        }
    }

    /// Returns the transfer data associated with this status, if any.
    pub fn transfer_data(&self) -> Option<&TransferData> {
        match self {
            SendStatus::InProgress(status) => Some(&status.transfer_data),
            _ => None,
        }
    }

    /// Reconstruct a `SendStatus` from its DB columns.
    ///
    /// Returns `Err` if the stored data is inconsistent (e.g. a status that
    /// requires transfer data but the columns are NULL).
    pub fn from_db_columns(
        status_tag: &str,
        progress_bytes: Option<u64>,
        file_size_bytes: Option<u64>,
        transfer_id: Option<String>,
    ) -> Result<Self, String> {
        let transfer_data = || -> Option<TransferData> {
            Some(TransferData {
                progress_bytes: progress_bytes?,
                file_size_bytes: file_size_bytes?,
                transfer_id: transfer_id.clone()?.into(),
            })
        };

        match status_tag {
            "Pending" => Ok(SendStatus::Pending),
            "Prepared" => Err("Prepared status is ephemeral and must not be persisted to DB".into()),
            "Sending" => transfer_data()
                .map(|it| {
                    SendStatus::InProgress(InProgress {
                        status: InProgressSendStatus::Sending,
                        transfer_data: it,
                    })
                })
                .ok_or_else(|| "Sending status requires transfer data but columns are NULL".into()),
            "Interrupted" => transfer_data()
                .map(|it| {
                    SendStatus::InProgress(InProgress {
                        status: InProgressSendStatus::Interrupted,
                        transfer_data: it,
                    })
                })
                .ok_or_else(|| {
                    "Interrupted status requires transfer data but columns are NULL".into()
                }),
            "Sent" => Ok(SendStatus::Sent),
            "Rejected" => Ok(SendStatus::Rejected),
            "Failed" => Ok(SendStatus::Failed),
            other => Ok(SendStatus::Unknown(other.into())),
        }
    }
}

impl Display for SendStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendStatus::Pending => write!(f, "Pending"),
            SendStatus::Sent => write!(f, "Sent"),
            SendStatus::Rejected => write!(f, "Rejected"),
            SendStatus::Failed => write!(f, "Failed"),
            SendStatus::InProgress(progress) => match progress.status {
                InProgressSendStatus::Prepared => write!(f, "Prepared"),
                InProgressSendStatus::Sending => write!(f, "Sending"),
                InProgressSendStatus::Interrupted => write!(f, "Interrupted"),
                InProgressSendStatus::PendingSent => write!(f, "PendingSent"),
            },
            SendStatus::Unknown(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateRemoteFilerequest {
    pub peer_id: PeerIdWrapper,
    pub filerequest_id: String,
    /// human readable identifier of the filerequest
    pub name: String,
    // pub requires_authentication: bool,
    pub contact_id: ContactId,
}

#[derive(Debug, Clone)]
pub struct RemoteFilerequest {
    pub id: FylesId,
    pub peer_id: PeerIdWrapper,
    pub filerequest_id: FylesId,
    pub contact_id: ContactId,
    /// human readable identifier of the filerequest
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ReceivedFile {
    pub id: FylesId,
    pub contact_id: Option<ContactId>,
    pub peer_id: String,
    pub filerequest_id: FylesId,
    pub transfer_id: Option<FylesId>,
    pub file_name: String,
    /// `None` while the transfer is still in progress.
    pub file_path: Option<String>,
    pub file_size_bytes: u64,
    pub progress_bytes: u64,
    pub status: ReceiveStatus,
    /// Milliseconds since UNIX epoch when the transfer started (used for .part file naming).
    pub started_at_ms: i64,
    /// `None` while the transfer is still in progress.
    pub received_at_ms: Option<i64>,
}

/// Payload for creating a new in-progress receive entry (status = Receiving).
#[derive(Debug, Clone)]
pub struct CreateIncomingFile {
    pub filerequest_id: FylesId,
    pub transfer_id: FylesId,
    pub contact_id: Option<ContactId>,
    pub peer_id: String,
    pub file_name: String,
    pub file_size_bytes: u64,
    /// Milliseconds since UNIX epoch when the transfer started (used for .part file naming).
    pub started_at_ms: i64,
}

/// Payload for completing an in-progress receive (sets file_path, received_at_ms, status = Completed).
#[derive(Debug, Clone)]
pub struct CompleteReceivedFile {
    pub transfer_id: FylesId,
    pub file_path: String,
    pub received_at_ms: i64,
}

// ── Receiver-side transfer status tracking ──

/// Status of a file being received from a peer.
#[derive(Debug, Clone, PartialEq)]
pub enum ReceiveStatus {
    /// Transfer has been accepted and we are actively receiving chunks.
    Receiving,
    /// Transfer was interrupted (e.g. connection lost) but can be continued.
    Interrupted,
    /// Transfer completed successfully — the file has been finalised.
    Completed,
    /// Transfer failed irrecoverably.
    Failed,
    /// Catch-all for unrecognised values read from DB (forward compat).
    Unknown(String),
}

impl ReceiveStatus {
    /// Returns the status tag string used for DB persistence.
    ///
    /// Returns `Err` for statuses that must not be persisted (`Unknown`).
    pub fn status_tag(&self) -> Result<&'static str, String> {
        match self {
            ReceiveStatus::Receiving => Ok("Receiving"),
            ReceiveStatus::Interrupted => Ok("Interrupted"),
            ReceiveStatus::Completed => Ok("Completed"),
            ReceiveStatus::Failed => Ok("Failed"),
            ReceiveStatus::Unknown(s) => Err(format!("Unknown status '{s}' must not be persisted")),
        }
    }

    /// Reconstruct a `ReceiveStatus` from its DB column.
    pub fn from_db_column(tag: &str) -> Self {
        match tag {
            "Receiving" => ReceiveStatus::Receiving,
            "Interrupted" => ReceiveStatus::Interrupted,
            "Completed" => ReceiveStatus::Completed,
            "Failed" => ReceiveStatus::Failed,
            other => ReceiveStatus::Unknown(other.into()),
        }
    }
}

impl Display for ReceiveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReceiveStatus::Receiving => write!(f, "Receiving"),
            ReceiveStatus::Interrupted => write!(f, "Interrupted"),
            ReceiveStatus::Completed => write!(f, "Completed"),
            ReceiveStatus::Failed => write!(f, "Failed"),
            ReceiveStatus::Unknown(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingFile {
    pub id: FylesId,
    pub contact_id: ContactId,
    pub file_path: String,
    pub retry_count: usize,
    /// The _internal_ ID of the filerequest that this file is associated with. This is
    /// not the same as the filerequest ID, which is the ID that is shared with peers.
    pub target_filerequest_id: FylesId,
    pub status: SendStatus,
    pub display_name: Option<String>,
    pub interruption_reasons: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CreatePendingFiles {
    pub file_infos: Vec<FileInfo>,
    pub target_filerequest_id: FylesId,
}

#[cfg(any(test, feature = "test-support"))]
impl CreatePendingFiles {
    pub async fn for_test(
        target_filerequest_id: FylesId,
        file_sizes: Vec<usize>,
    ) -> (TempDir, Self) {
        use futures::future::join_all;

        let tempdir = tempfile::tempdir().expect("Failed to create temp dir");
        let file_info_futures = file_sizes.into_iter().enumerate().map(async |(i, size)| {
            use tokio::io::AsyncWriteExt;

            use crate::library::util::util::generate_random_bytes;

            let file_name = format!("test_file_{}.txt", i);
            let file_path = tempdir.path().join(file_name.clone());
            let mut file = tokio::fs::File::create(&file_path)
                .await
                .expect("Failed to create test file");
            tokio::io::AsyncWriteExt::write_all(&mut file, &generate_random_bytes(size))
                .await
                .expect("Failed to write to test file");
            file.flush().await.expect("Failed to flush test file");
            FileInfo {
                path: file_path.to_str().unwrap().to_string(),
                display_name: Some(file_name)
            }
        });
        let file_infos = join_all(file_info_futures).await;
        (
            tempdir,
            CreatePendingFiles {
                file_infos,
                target_filerequest_id,
            },
        )
    }
}

/// Things you may want to send data to
#[allow(dead_code)]
pub enum Dialable {
    /// A single, specific device, like Ann's smartphone or XY-company's server
    Device(PeerIdWrapper, PushStrategy),
    /// A specific person or entity, which may or may not be represented by multiple devices
    Contact(Contact, MustReach),
    /// A combination of several Dialables, like several people, several devices, or a mix of both
    Collection(Vec<Dialable>, MustReach),
}

/// How many of the Dialables must be reached. Some messages should perhaps reach all
/// specified Dialables, like a message to a group chat. Others should reach at least one,
/// like a request for the current time. Yet others must explicitly reach only one, like
/// a scecret, ephemeral message that needs to be read only once.
#[allow(dead_code)]
pub enum MustReach {
    /// Try to send messages to all Dialables, but don't ensure that all of them receive it.
    /// Useful for example for real time broadcast messages.
    ///
    /// This may not send at all if your Push strategy is one tailored to ensure reception.
    None(AdHocPushStrategy),
    /// Explicitly make sure this message is only sent to a single Dialable. This will sequentially
    /// try to send the message to each Dialable until one of them accepts it. This does not guarantee
    /// that a Diaalable does not misbehave, like receiving the message without acknowledging it. So this is only
    /// semantically accurate, but not from a security standpoint.
    ExactlyOne(PushStrategy),
    /// Ensure at least one of the Dialables receives the message. This will try to send the message
    /// to all Dialables until at least one of them accepts it. Sending attempts may run in parallel resulting
    /// in multiple Dialables receiving the message.
    AtLeastOne(ReliablePushStrategy),
    /// Ensure all Dialables receive the message. This will try to send the message to all Dialables until
    /// all of them accept it.
    All(ReliablePushStrategy),
}

impl Default for MustReach {
    fn default() -> Self {
        MustReach::AtLeastOne(Default::default())
    }
}

/// A `PushStrategy` defines the method that is used to attempt to send data to a `Dialable`.
/// It governs things like amount of retries, timing and the amount of peers that a sent to is attempted,
/// if there are multiple. Extremes may be "attempt right now, only once, to send this to at least one of these peers",
/// while something that is more specific may be "from now on, every 5 seconds, try to send this exactly one of these peers, until the first
/// attempt succeeds, then stop".
#[allow(dead_code)]
pub enum PushStrategy {
    /// Try sending if possible, but do not guarantee success. Think UDP
    Unreliable(AdHocPushStrategy),
    /// Retry as often as necessary, until a send was successful. Think TCP
    Reliable(ReliablePushStrategy),
}

impl Default for PushStrategy {
    fn default() -> Self {
        PushStrategy::Reliable(Default::default())
    }
}

/// Strategies that do not enforce that (every) sending attempt is successful.
#[allow(dead_code)]
#[derive(Default)]
pub enum AdHocPushStrategy {
    /// Send right now, do not care about the result. This is useful for things like "hello, I'm here" messages
    #[default]
    RightNow,
    /// Send messages in regular intervals, but do not care about the result. This is useful for things like "keep-alive" messages.
    Interval { interval_ms: Duration },
}

/// Strategies that enforce that the sending attempt is successful.
#[allow(dead_code)]
pub enum ReliablePushStrategy {
    /// Retry until success, at regular intervals. Simple, fool proof, but may make cold starts of peers that were down harder
    RetryInterval { interval_ms: Duration },
    /// Retry until success, with an exponential backoff, but capped at a maximum interval. Makes cold starts of peers that were down easier, but may cause delays in sending.
    CappedExponentialRetry(CappedExponentialRetry),
    /// Retry until success, with an adaptive interval that is determined by the nth attempt. This allows for more fine-grained control over the retry intervals.
    /// The `nth_interval` function should return the interval for the nth attempt, where n starts at 0.
    Adaptive {
        nth_interval: Box<dyn Fn(u32) -> Duration>,
    },
}

impl Default for ReliablePushStrategy {
    fn default() -> Self {
        ReliablePushStrategy::CappedExponentialRetry(Default::default())
    }
}

/// A retry strategy that uses an exponential backoff, but caps the maximum interval to avoid excessive delays.
pub struct CappedExponentialRetry {
    base_interval: Duration,
    max_interval: Duration,
}

impl Default for CappedExponentialRetry {
    fn default() -> Self {
        Self {
            base_interval: 5.seconds(),
            // try at least four times per day. That's not often enough
            // to cause significant resource drain, but often enough to
            // ensure at least one attempt is made during the waking hours
            // of a normal person, which is when most devices are likely to
            // be online.
            max_interval: 6.hours(),
        }
    }
}

#[allow(dead_code)]
impl CappedExponentialRetry {
    pub fn new(base_interval: Duration, max_interval: Duration) -> Self {
        Self {
            base_interval,
            max_interval,
        }
    }

    pub fn n_th_interval(&self, n: u32) -> Duration {
        match n {
            0 => 0.millis(),
            _ => min(
                self.base_interval * (2usize.pow(n - 1) as u32),
                self.max_interval,
            ),
        }
    }
}

/// Strategies to internally redistribute data between "my" peers. Perhaps you want your phone
/// to accept files from your friends, but push those to your laptop as soon as possible. Or you
/// want chat messages received on your home server to be pushed to your phone, when it comes online.
#[allow(dead_code)]
pub enum DataRedistributionModel {
    /// Keep the data on the initial peer, and don't push it to any other peers.
    KeepOnInitialPeer,
    /// Push the data to all peers. The meaning of "all" depends on the context.
    PushToAll,
    /// Move the data to a specific peer, so that the initial peer acts as a relay.
    MoveToSpecificPeer(PeerIdWrapper),
    /// Push the data to a specific set of peers.
    PushToPeers(Vec<PeerIdWrapper>),
}
