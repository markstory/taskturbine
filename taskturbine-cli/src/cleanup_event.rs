use std::time::Duration;

use crate::CliError;
use chrono::Utc;
use taskturbine_core::storage::Storage;

pub async fn execute(storage: Storage) -> Result<(), CliError> {
    // Get the configuration from storage.
    // In userland code they would define the config, and apply to the App.
    let config = storage.get_config();

    // This code is tested at the Worker layer
    let cutoff = Duration::from_secs(config.worker_cleanup_cutoff_secs as u64);
    let older_than = Utc::now() - cutoff;
    let limit = config.worker_cleanup_limit;

    // TODO this should just be a one-shot on the retention cleanup methods
    // not a long running worker.
    log::info!("Cleaning up event data up to {limit} records older than {older_than}");

    match storage.cleanup_events(older_than, limit).await {
        Ok(removed) => {
            log::info!("Cleanup complete. Removed {removed}");
            Ok(())
        },
        Err(e) => Err(e.into()),
    }
}
