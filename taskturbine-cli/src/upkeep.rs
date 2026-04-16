use crate::CliError;
use taskturbine_core::app::{TaskturbineApp, run_upkeep_worker};
use taskturbine_core::storage::Storage;

/// Perform periodic upkeep operations on all channels in a usecase.
pub async fn upkeep(storage: Storage) -> Result<(), CliError> {
    log::info!("Starting upkeep worker");

    let app = TaskturbineApp::from_storage(storage);
    let worker = app.create_worker("cleanup-worker-1", vec![]);
    run_upkeep_worker(worker).await;
    log::info!("Shutdown upkeep worker");

    Ok(())
}
