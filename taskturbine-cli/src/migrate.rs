use crate::CliError;
use taskturbine_core::storage::Storage;

pub async fn run_migrations(storage: Storage) -> Result<(), CliError> {
    log::info!("Running migrations for taskturbine");

    let _ = storage
        .update_schema()
        .await
        .map_err(|e| CliError::Message(format!("Failed to run migrations: {e:?}")));

    Ok(())
}
