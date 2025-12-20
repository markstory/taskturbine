use taskturbine_core::{app::TaskturbineApp, context::{FlowControl, TaskContext}, config::Config};

use crate::db::create_db;

enum TaskError {
    Message(String),
}

pub fn make_task_app() -> TaskturbineApp {
    let task_config = Config {
        database_url: "postgresql://apps:password@localhost/test_taskturbine".into(),
        ..Config::default()
    };

    let app = TaskturbineApp::new(task_config)
        .add_channel("mail")
        .register_task("register-user", register_user);

    app
}

pub async fn register_user(mut ctx: TaskContext) -> Result<(), FlowControl> {
    log::info!("starting register task");
    let db = create_db().await;

    async fn create_user() -> Result<Vec<u8>, TaskError> {
        Ok(vec![])
    }
    let create_user = ctx.async_step("create-user", create_user).await;

    Ok(())
}
