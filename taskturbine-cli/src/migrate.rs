use crate::CliError;
use simple_logger::SimpleLogger;
use taskturbine_core::storage::Storage;

pub async fn run_migrations(storage: Storage) -> Result<(), CliError> {
    // Setup basic logger
    SimpleLogger::new().init().unwrap();
    log::info!("Running migrations for taskturbine");

    let _ = storage
        .update_schema()
        .await
        .map_err(|e| CliError::Message(format!("Failed to run migrations: {e:?}")));

    Ok(())
}
