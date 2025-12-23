use std::time::Duration;

use serde::{Deserialize, Serialize};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use sqlx::Row;
use taskturbine_core::{app::TaskturbineApp, config::Config, context::{FlowControl, TaskContext}};

use crate::db::{create_db, SALT};

type HmacSha256 = Hmac<Sha256>;

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
pub struct UserCreate {
    pub name: String,
    pub email: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct UserRegister {
    pub user_id: i64,
    pub email: String,
}

pub async fn register_user(mut ctx: TaskContext) -> Result<(), FlowControl> {
    log::info!("starting register task");

    /// Steps can be defined as standard functions
    /// Store the user in the database.
    async fn create_user(ctx: TaskContext) -> Result<Vec<u8>, TaskError> {
        let db = create_db().await;
        let payload: UserCreate = serde_json::from_slice(ctx.param_bytes())?;

        let row = sqlx::query(
            "INSERT INTO users (name, email, verified) VALUES ($1, $2, false)
            RETURNING *
            "
        )
        .bind(payload.name)
        .bind(payload.email)
        .fetch_one(&db)
        .await
        .map_err(|e| TaskError::Message(format!("Could not save user: {e}")))?;

        let user = UserRegister {
            email: row.get("email"),
            user_id: row.get("user_id"),
        };
        let blob = serde_json::to_string(&user)?;

        Ok(blob.into())
    }
    let create_user_json = ctx.async_step("create-user", create_user).await?;

    // Steps can also be async blocks if you need to capture values from previous steps
    let event_name = ctx.async_step("send-verification-code", async |_ctx: TaskContext| -> Result<Vec<u8>, TaskError> {
        // This simulates an email verification flow.
        let user_data: UserRegister = serde_json::from_slice(create_user_json.as_slice())?;
        let mut mac = HmacSha256::new_from_slice(SALT.as_bytes()).unwrap();
        mac.update(user_data.email.as_bytes());
        let hex_code = hex::encode(mac.finalize().into_bytes());

        // Ideally this would be sent in an email, but this is a prototype.
        println!("------------------------------------");
        println!("User registration verification code");
        println!("");
        println!("Click the link to continue");
        println!("http://localhost:3000/verify/{}/{}", user_data.user_id, hex_code);
        println!("");
        println!("------------------------------------");

        let event_name = format!("email-verify-{}", user_data.email);

        Ok(event_name.into())
    }).await?;

    // Wait for the link to be clicked.
    let verification_data = ctx.await_event(str::from_utf8(event_name.as_slice()).unwrap(), Some(Duration::from_secs(60 * 10))).await?;

    Ok(())
}
