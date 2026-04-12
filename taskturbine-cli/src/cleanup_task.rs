use std::time::Duration;

use crate::CliError;
use chrono::Utc;
use taskturbine_core::storage::Storage;

pub async fn execute(storage: Storage) -> Result<(), CliError> {
    // TODO cutoff_secs and cleanup_limit should come from CLI args.
    let config = storage.get_config();
    let cutoff = Duration::from_secs(config.worker_cleanup_cutoff_secs as u64);
    let older_than = Utc::now() - cutoff;
    let limit = config.worker_cleanup_limit;

    log::info!("Cleaning up task data up to {limit} records older than {older_than}");

    match storage.cleanup_tasks(older_than, limit).await {
        Ok(removed) => {
            log::info!("Cleanup complete. Removed {removed}");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
