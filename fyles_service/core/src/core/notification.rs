#[derive(Debug)]
pub enum Notification {
    FileReceived {
        contact_name: Option<String>,
        filerequest_name: String,
        file_name: String,
        file_size: u64,
    },
}
