#[derive(Debug)]
pub enum Notification {
    FileReceived {
        sender: FileSender,
        filerequest_name: String,
        file_name: String,
        file_size: u64,
    },
}

/// Who sent a received file, as far as the app could resolve it.
#[derive(Debug)]
pub enum FileSender {
    /// A known contact, carrying their resolved display name.
    Contact(String),
    /// The user themselves, from another of their own devices.
    SelfDevice,
    /// An unverified sender we couldn't resolve to a contact.
    Unknown,
}
