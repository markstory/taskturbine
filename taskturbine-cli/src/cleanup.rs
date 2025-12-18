use crate::CliError;
use simple_logger::SimpleLogger;
use taskturbine_core::storage::Storage;
use taskturbine_core::app::{run_cleanup_worker, TaskturbineApp};

pub async fn cleanup(storage: Storage) -> Result<(), CliError> {
    // Get the configuration from storage.
    // In userland code they would define the config, and apply to the App.
    let config = storage.get_config();

    // Setup basic logger
    SimpleLogger::new().init().unwrap();
    log::info!("Starting cleanup worker");

    // Create an application instance, and a Worker.
    let app = TaskturbineApp::new(config.clone());
    let worker = app.create_worker("cleanup-worker-1", vec![]);
    run_cleanup_worker(worker).await;

    Ok(())
}
