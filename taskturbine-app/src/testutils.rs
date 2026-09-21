use uuid::Uuid;

use crate::app::TaskturbineApp;
use crate::config::Config;

pub fn create_config() -> Config {
    let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
        .expect("Missing required TASKTURBINE_DATABASE_URL env var");
    Config {
        usecase: format!("taskturbine-test-{}", Uuid::now_v7()),
        database_url: db_url,
        database_log_queries: true,
        default_channel: "taskturbine-test".to_owned(),
        ..Config::default()
    }
}

pub async fn create_app() -> TaskturbineApp {
    let config = create_config();
    let app = TaskturbineApp::new(config);
    app.storage.update_schema().await.unwrap();

    app
}
