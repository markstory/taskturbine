use serde::{Deserialize, Serialize};
use taskturbine_core::{app::TaskturbineApp, context::{FlowControl, TaskContext}, config::Config};

use crate::db::create_db;

enum TaskError {
    Message(String),
}

/// Make the task code simpler.
impl From<serde_json::Error> for TaskError {
    fn from(value: serde_json::Error) -> Self {
        TaskError::Message(format!("serialization/deserialization error: {:?}", value))
    }
}


/// Factory method for the task application with all tasks bound in.
/// In more complex applications, tasks would be defined in module files, and imported here.
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

#[derive(sqlx::FromRow, Debug, PartialEq, Deserialize, Serialize)]
pub struct User {
    pub name: String,
    pub email: String,
    pub verified: bool,
}

pub async fn register_user(mut ctx: TaskContext) -> Result<(), FlowControl> {
    log::info!("starting register task");

    async fn create_user(ctx: TaskContext) -> Result<Vec<u8>, TaskError> {
        let db = create_db().await;
        let payload: User = serde_json::from_slice(ctx.param_bytes())?;

        sqlx::query(
            "INSERT INTO users (name, email, verified) VALUES ($1, $2, false)"
        )
        .bind(payload.name)
        .bind(payload.email)
        .execute(&db)
        .await
        .map_err(|e| TaskError::Message(format!("Could not save user: {e}")))?;

        Ok(vec![])
    }
    let create_user = ctx.async_step("create-user", create_user).await;

    Ok(())
}
