//! Run a worker that performs upkeep operations on a taskturbine database.
//!
//! Every `config.worker_upkeep_interval` a upkeep operation will run
//! which deletes completed tasks and expired events from the database.
use crate::CliError;
use taskturbine::app::{TaskturbineApp, run_upkeep_worker};
use taskturbine::config::Config;
use taskturbine_core::storage::Storage;

/// Command for running a worker that performs upkeep operations.
pub async fn upkeep(storage: Storage, config: Config) -> Result<(), CliError> {
    log::info!("Starting upkeep worker");

    let app = TaskturbineApp::with_storage(config, storage);
    let worker = app.create_worker("cleanup-worker-1", vec![]);
    run_upkeep_worker(worker).await;
    log::info!("Shutdown upkeep worker");

    Ok(())
}
