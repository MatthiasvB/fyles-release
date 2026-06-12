use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::core::db::FilerequestDb;
use crate::io_controller::HostController;
use crate::core::domain_models::ReceiveStatus;
use crate::library::util::epoch::unix_epoch_millis;
use crate::library::util::part_file::parse_part_file_started_at_ms;

/// Files (stale `.part` partials and orphaned outgoing copies) older than this are swept.
const DEFAULT_STALE_THRESHOLD: Duration = Duration::from_secs(7 * 24 * 60 * 60); // 7 days

pub async fn run_cleanups(db: Arc<dyn FilerequestDb>, hc: Arc<dyn HostController>) {
    info!("Running storage cleanups");
    
    // 1. Structured partial cleanup
    structured_partial_cleanup(db.clone(), hc.clone()).await;
    
    // 2. Sweep partial cleanup
    sweep_partial_cleanup(hc.clone()).await;
    
    // 3. Sweep outgoing files cleanup
    sweep_outgoing_files_cleanup(hc.clone()).await;
}

async fn structured_partial_cleanup(db: Arc<dyn FilerequestDb>, hc: Arc<dyn HostController>) {
    let now = match unix_epoch_millis() {
        Ok(ms) => ms,
        Err(_) => return,
    };
    
    let threshold = now - DEFAULT_STALE_THRESHOLD.as_millis() as i64;
    let stale_files = match db.get_stale_received_files(threshold).await {
        Ok(files) => files,
        Err(e) => {
            error!(?e, "Failed to fetch stale received files");
            return;
        }
    };
    
    for file in stale_files {
        let Some(transfer_id) = &file.transfer_id else {
            warn!("Skipping stale received file with no transfer_id: {:?}", file.id);
            continue;
        };

        let part_filename = crate::library::util::part_file::get_part_file_name(
            file.started_at_ms,
            &transfer_id.0,
            &file.peer_id,
            &file.contact_id,
        );

        let _ = hc.remove_partial_file(&part_filename).await;
        let _ = db.update_received_file_status(transfer_id, &ReceiveStatus::Failed, file.progress_bytes).await;
    }
}

async fn sweep_partial_cleanup(hc: Arc<dyn HostController>) {
    let now = match unix_epoch_millis() {
        Ok(ms) => ms,
        Err(_) => return,
    };
    
    let threshold = now - DEFAULT_STALE_THRESHOLD.as_millis() as i64;
    let partial_files = match hc.list_partial_files().await {
        Ok(files) => files,
        Err(e) => {
            error!(?e, "Failed to list partial files");
            return;
        }
    };
    
    for file in partial_files {
        if parse_part_file_started_at_ms(&file).is_some_and(|started_at| started_at < threshold) {
            warn!("Found orphaned stale partial file: {}", file);
            let _ = hc.remove_partial_file(&file).await;
        }
    }
}

async fn sweep_outgoing_files_cleanup(hc: Arc<dyn HostController>) {
    let now = match unix_epoch_millis() {
        Ok(ms) => ms,
        Err(_) => return,
    };
    
    let threshold = now - DEFAULT_STALE_THRESHOLD.as_millis() as i64;
    let temp_files = match hc.list_temporary_outgoing_files().await {
        Ok(files) => files,
        Err(e) => {
            error!(?e, "Failed to list temporary outgoing files");
            return;
        }
    };
    
    for file in temp_files {
        if file.modified_ms < threshold {
            warn!("Found stale outgoing temporary file: {}", file.path);
            let _ = hc.remove_source_file_if_temporary(&file.path).await;
        }
    }
}
