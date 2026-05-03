use std::{env, time::Duration};

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use sqlx::Row;
use taskturbine_core::{
    app::{ResultData, TaskResult, TaskturbineApp},
    config::Config,
    context::{FlowControl, TaskContext},
};

use crate::db::{SALT, create_db};

pub type HmacSha256 = Hmac<Sha256>;

#[allow(dead_code)]
#[derive(Debug)]
enum TaskError {
    Message(String),
}

/// Make the task code simpler.
impl From<serde_json::Error> for TaskError {
    fn from(value: serde_json::Error) -> Self {
        TaskError::Message(format!("serialization/deserialization error: {value:?}"))
    }
}

/// Factory method for the task application with all tasks bound in.
/// In more complex applications, tasks would be defined in module files, and imported here.
pub fn make_task_app() -> TaskturbineApp {
    let database_url = env::var("DEMO_DATABASE_URL").expect("Missing DEMO_DATABASE_URL in env");
    let task_config = Config {
        database_url,
        ..Config::default()
    };

    TaskturbineApp::new(task_config)
        .add_channel("mail")
        .register_task("err-fail", err_failure)
        .register_task("panic-fail", panic_failure)
        .register_task("register-user", register_user)
        .register_task("sleep-time", sleep_time)
}

#[derive(sqlx::FromRow, Debug, PartialEq, Deserialize, Serialize)]
pub struct RegisterUserParams {
    pub name: String,
    pub email: String,
    pub org_name: String,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct UserRegister {
    pub id: i64,
    pub email: String,
}

pub async fn register_user(mut ctx: TaskContext) -> TaskResult {
    log::info!("starting register task");

    /// Steps can be defined as standard functions
    /// Store the user in the database.
    async fn create_user(ctx: TaskContext) -> Result<ResultData, TaskError> {
        log::info!("create the user in the database");
        let db = create_db().await;
        let payload: RegisterUserParams = serde_json::from_slice(ctx.param_bytes())?;

        let row = sqlx::query(
            "INSERT INTO users (name, email, verified) VALUES ($1, $2, false)
            RETURNING *
            ",
        )
        .bind(payload.name)
        .bind(payload.email)
        .fetch_one(&db)
        .await
        .map_err(|e| TaskError::Message(format!("Could not save user: {e}")))?;

        let user = UserRegister {
            email: row.get("email"),
            id: row.get("id"),
        };
        let blob = serde_json::to_string(&user)?;

        Ok(blob.into())
    }
    let create_user_json = ctx.async_step("create-user", create_user).await?;

    // Steps can also be async blocks if you need to capture values from previous steps
    let event_name = ctx
        .async_step(
            "send-verification-code",
            async |_ctx: TaskContext| -> Result<ResultData, TaskError> {
                log::info!("Generate and log a verification code");

                // This simulates an email verification flow.
                let user_data: UserRegister = serde_json::from_slice(create_user_json.as_slice())?;
                let mut mac = HmacSha256::new_from_slice(SALT.as_bytes()).unwrap();
                mac.update(user_data.email.as_bytes());
                let hex_code = hex::encode(mac.finalize().into_bytes());

                // Ideally this would be sent in an email, but this is a prototype.
                println!("------------------------------------");
                println!("User registration verification code");
                println!();
                println!("Click the link to continue");
                println!("http://localhost:3000/verify/{}/{}", user_data.id, hex_code);
                println!();
                println!("------------------------------------");

                let event_name = format!("email-verify-{}", user_data.email);

                Ok(event_name.into())
            },
        )
        .await?;

    log::info!("Wait for the user to verify their email");
    let _ = ctx
        .await_event(
            str::from_utf8(event_name.as_slice()).unwrap(),
            Some(Duration::from_secs(60 * 10)),
        )
        .await?;

    let _ = ctx
        .async_step(
            "verification-complete",
            async |_ctx: TaskContext| -> Result<ResultData, TaskError> {
                log::info!("Verification complete, update the user.");
                let db = create_db().await;
                let user_data: UserRegister = serde_json::from_slice(create_user_json.as_slice())?;
                let _ = sqlx::query("UPDATE users SET verified = true WHERE id = $1")
                    .bind(user_data.id)
                    .execute(&db)
                    .await
                    .map_err(|e| TaskError::Message(format!("Could not update user: {e}")))?;

                Ok(vec![])
            },
        )
        .await?;

    let _ = ctx.async_step("provision-organization", async |ctx: TaskContext| -> Result<ResultData, TaskError> {
        log::info!("Provision the organization.");
        let params: RegisterUserParams = serde_json::from_slice(ctx.param_bytes())?;
        let user_data: UserRegister = serde_json::from_slice(create_user_json.as_slice())?;

        let db = create_db().await;
        let mut atomic = db.begin().await.unwrap();

        // Note: This should be a more robust slug generation
        let slug = params.org_name.to_lowercase().replace(" ", "-");

        // Note: This should handle slug conflicts and generate unique slugs.
        let res = sqlx::query(
            "INSERT INTO organizations (name, slug, created) VALUES ($1, $2, NOW())
            RETURNING *"
        )
        .bind(&params.org_name)
        .bind(slug)
        .fetch_one(&mut *atomic)
        .await
        .map_err(|e| TaskError::Message(format!("Could not create organization: {e}")))?;

        log::info!("Link the user to the organization as owner.");
        let org_id: i64 = res.get("id");
        let _ = sqlx::query(
            "INSERT INTO organization_members (user_id, organization_id, role) VALUES ($1, $2, 'owner')"
        )
        .bind(user_data.id)
        .bind(org_id)
        .execute(&mut *atomic)
        .await
        .map_err(|e| TaskError::Message(format!("Could not create organization member: {e}")))?;

        atomic.commit().await.unwrap();

        Ok(vec![])
    }).await?;
    log::info!("All steps complete.");

    Ok(None)
}

/// Task that always fail are handled.
pub async fn err_failure(mut _ctx: TaskContext) -> TaskResult {
    log::info!("Starting failure task");
    Err(FlowControl::InvalidValue("something bad".into()))
}

/// Tasks that panic will be handled without killing the worker.
pub async fn panic_failure(mut _ctx: TaskContext) -> TaskResult {
    log::info!("Starting panic_failure task");
    panic!("A task has hit panic!");
}

/// Simulate IO wait workload
pub async fn sleep_time(ctx: TaskContext) -> TaskResult {
    let params = ctx.param_bytes();
    let res: Result<Value, _> = serde_json::from_slice(params.as_slice());
    let delay = match res {
        Ok(params) => {
            params
                .get("duration")
                .map(|v| v.as_f64())
                .unwrap_or(Some(0.1))
                .unwrap()
        },
        Err(_) => 0.1
    };
    log::info!("started sleep_time. Sleeping for {delay}");
    tokio::time::sleep(Duration::from_millis((delay * 1000.0) as u64)).await;
    log::info!("sleep_time complete");
    Ok(None)
}
