use crate::CliError;
use taskturbine_core::storage::Storage;

pub async fn run_migrations(storage: Storage) -> Result<(), CliError> {
    log::info!("Running migrations for taskturbine");

    storage
        .update_schema()
        .await
        .map_err(|e| CliError(format!("Failed to run migrations: {e:?}")))
}
