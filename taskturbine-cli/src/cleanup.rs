use crate::CliError;
use taskturbine_core::app::{TaskturbineApp, run_cleanup_worker};
use taskturbine_core::storage::Storage;

pub async fn cleanup(storage: Storage) -> Result<(), CliError> {
    // Get the configuration from storage.
    // In userland code they would define the config, and apply to the App.
    let config = storage.get_config();

    // TODO this should just be a one-shot on the retention cleanup methods
    // not a long running worker.
    log::info!("Starting cleanup worker");

    // Create an application instance, and a Worker.
    let app = TaskturbineApp::new(config.clone());
    let worker = app.create_worker("cleanup-worker-1", vec![]);
    run_cleanup_worker(worker).await;

    Ok(())
}
