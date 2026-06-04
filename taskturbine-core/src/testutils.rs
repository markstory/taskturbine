use uuid::Uuid;

use crate::{app::TaskturbineApp, config::Config, models::SpawnResult, storage::{Storage, StorageError}};

/// Module of test helpers and utilities.
///
pub fn create_config() -> Config {
    let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
        .expect("Missing required TASKTURBINE_DATABASE_URL env var");
    Config {
        usecase: format!("taskturbine-test-{}", Uuid::now_v7()),
        database_url: db_url,
        default_channel: "taskturbine-test".into(),
        database_log_queries: true,
        ..Config::default()
    }
}


pub async fn create_storage() -> Storage {
    let config = create_config();
    let storage = Storage::new(config);

    // Ensure migrations have been applied and that storage is cleared.
    storage.update_schema().await.unwrap();

    storage
}

pub async fn create_task() -> Result<(Storage, SpawnResult), StorageError> {
    let storage = create_storage().await;
    let channel = "demo";
    let task_name = "say_hello";
    let payload = b"{\"key\": \"value\"}";

    let result = storage.spawn_task(channel, task_name, payload, None).await;
    assert!(result.is_ok(), "Failed to spawn task {:?}", result.err());
    let spawned = result.unwrap();

    Ok((storage, spawned))
}

pub async fn create_app() -> TaskturbineApp {
    let config = create_config();
    let app = TaskturbineApp::new(config);
    app.storage.update_schema().await.unwrap();

    app
}
