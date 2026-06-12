use crate::core::domain_models::ContactId;

/// Build the deterministic `.part` filename for a received file.
///
/// The `started_at_ms` value is persisted in the `received_files` table,
/// so this function can reconstruct the exact same filename at any later
/// point (continuation, completion, abort, cleanup).
pub fn get_part_file_name(
    started_at_ms: i64,
    transfer_id: &str,
    peer_id: &str,
    contact_id: &Option<ContactId>,
) -> String {
    let contact_string = contact_id
        .as_ref()
        .map(|id| id.0.as_str())
        .unwrap_or("unknown-contact");
    format!("{started_at_ms}_{contact_string}_{peer_id}_{transfer_id}.part")
}

/// Parses the `started_at_ms` value from a `.part` filename.
pub fn parse_part_file_started_at_ms(filename: &str) -> Option<i64> {
    if !filename.ends_with(".part") {
        return None;
    }
    
    let parts: Vec<&str> = filename.split('_').collect();
    if parts.is_empty() {
        return None;
    }
    
    parts[0].parse::<i64>().ok()
}