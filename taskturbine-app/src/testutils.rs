use crate::app::TaskturbineApp;
use taskturbine_core::testutils::create_config;

pub async fn create_app() -> TaskturbineApp {
    let config = create_config();
    let app = TaskturbineApp::new(config);
    app.storage.update_schema().await.unwrap();

    app
}
