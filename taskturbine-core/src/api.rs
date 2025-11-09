use std::collections::HashMap;

use crate::config::Config;
use sqlx::{PgPool, migrate::MigrateError, Row};
use uuid::Uuid;

#[derive(Debug)]
pub enum TaskTurbineError {
    EncodeError(serde_json::Error),
    SqlError(sqlx::Error),
    NotFound(Uuid),
    NotRunning(Uuid),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum TaskState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
    Cancelled,
}

/// Result of spawning a task.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpawnResult {
    pub task_id: Uuid,
    pub run_id: Uuid,
}

/// Options for spawning a task.
/// Default values are drawn from the TaskRuntime and TaskOptions defaults.
pub struct TaskOptions {
    /// Map of headers to include with the task activation
    headers: HashMap<String, String>,

    /// The maximum number of attempts to make on this task
    max_attempts: i32,

    /// The minimum number of seconds to wait between retries.
    retry_seconds: i32,

    /// The multipier to apply to retry delays between attempts.
    /// Use > 1.0 to create exponential backoff.
    retry_factor: f64,

    /// The maximum number of seconds to wait between retries.
    retry_max_seconds: i32,

    /// The maximum age of a task before it should not be run.
    /// Measured in seconds from when the task was first stored.
    cancellation_max_age: i32,
}

impl Default for TaskOptions {
    fn default() -> Self {
        TaskOptions {
            headers: HashMap::new(),
            max_attempts: 5,
            retry_seconds: 10,
            retry_factor: 2.0,
            retry_max_seconds: 300,
            cancellation_max_age: 86400,
        }
    }
}

/// A structure for interacting with the storage layer of TaskTurbine
pub struct Storage {
    config: Config,
    pool: PgPool,
}

impl Storage {
    /// Create a new runtime from the given configuration.
    pub fn new(config: Config) -> Self {
        let pool = PgPool::connect_lazy(&config.database_url)
            .expect("Failed to create database connection pool");
        Self { config, pool }
    }

    // Run migrations to create or update the database schema.
    // Will create a taskturbine schema and add all tables inside that schema.
    pub async fn update_schema(&self) -> Result<(), MigrateError> {
        sqlx::migrate!("./migrations").run(&self.pool).await
    }

    /// Delete all data from the storage tables.
    /// This is a destructive operation that should only really be used in tests.
    pub async fn clear_storage(&self) -> Result<(), TaskTurbineError> {
        let tables = ["events", "waits", "checkpoints", "runs", "tasks"];
        for table in tables.iter() {
            let query = format!("TRUNCATE taskturbine.{table} CASCADE");
            sqlx::query(&query)
                .execute(&self.pool)
                .await
                .map_err(TaskTurbineError::SqlError)?;
        }
        Ok(())
    }

    /// Spawn a task and initialize a run.
    pub async fn spawn_task(
        &self,
        namespace: &str,
        task_name: &str,
        payload: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, TaskTurbineError> {
        let options = options.or_else(|| Some(TaskOptions::default())).unwrap();
        let header_json =
            serde_json::to_vec(&options.headers).map_err(TaskTurbineError::EncodeError)?;

        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;
        let task_id = Uuid::now_v7();
        let res = sqlx::query(
            "INSERT INTO taskturbine.tasks (
                task_id, namespace, task_name, params, headers,
                retry_seconds, retry_factor, retry_max_seconds,
                max_attempts, cancellation_max_age, enqueue_at, state
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11)",
        )
        .bind(task_id)
        .bind(namespace)
        .bind(task_name)
        .bind(payload)
        .bind(header_json)
        .bind(options.retry_seconds)
        .bind(options.retry_factor)
        .bind(options.retry_max_seconds)
        .bind(options.max_attempts)
        .bind(options.cancellation_max_age)
        .bind(TaskState::Pending)
        .execute(&mut *atomic);

        if let Err(e) = res.await {
            return Err(TaskTurbineError::SqlError(e));
        }

        let run_id = Uuid::now_v7();
        let res = sqlx::query(
            "INSERT INTO taskturbine.runs (
                run_id, task_id, attempt, state, available_at
            ) VALUES ($1, $2, 0, $3, NOW())",
        )
        .bind(run_id)
        .bind(task_id)
        .bind(TaskState::Pending)
        .execute(&mut *atomic);

        if let Err(e) = res.await {
            return Err(TaskTurbineError::SqlError(e));
        }
        atomic
            .commit()
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(SpawnResult { task_id, run_id })
    }

    /// Mark a run as completed with the provided state.
    /// When a run is completed, the task is also considered complete.
    pub async fn complete_run(&self, run_id: Uuid, run_result: &[u8]) -> Result<(), TaskTurbineError> {
        let mut atomic = self.pool.begin().await.map_err(TaskTurbineError::SqlError)?;
        let res = sqlx::query(
            "SELECT task_id, state FROM taskturbine.runs WHERE run_id = $1 FOR UPDATE"
        ).bind(run_id)
        .fetch_one(&mut *atomic)
        .await;
        if let Err(_) = res {
            return Err(TaskTurbineError::NotFound(run_id));
        }

        let row = res.unwrap();
        let task_id: Uuid = row.get("task_id");
        let state: TaskState = row.get("state");
        if state != TaskState::Completed {
            // Already completed
            atomic.commit().await.map_err(|e| TaskTurbineError::SqlError(e))?;
            return Err(TaskTurbineError::NotRunning(run_id));
        }
        let res = sqlx::query(
            "UPDATE taskturbine.runs
            SET state = $1, completed_at = NOW(), result = $2
            WHERE run_id = $3",
        )
        .bind(TaskState::Completed)
        .bind(run_result)
        .bind(run_id)
        .execute(&mut *atomic)
        .await;
        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }

        let res = sqlx::query(
            "UPDATE taskturbine.tasks
            SET state = $1, last_attempt_run = $2 WHERE task_id = $3",
        )
        .bind(TaskState::Completed)
        .bind(run_id)
        .bind(task_id)
        .execute(&mut *atomic)
        .await;
        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }

        atomic.commit().await.map_err(|e| TaskTurbineError::SqlError(e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_storage() -> Storage {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL").unwrap();
        let config = Config {
            database_url: db_url,
        };
        let storage = Storage::new(config);

        // Ensure migrations have been applied and that storage is cleared.
        storage.update_schema().await.unwrap();
        storage.clear_storage().await.unwrap();

        storage
    }

    #[tokio::test]
    async fn test_spawn_task_get_task_id() {
        let storage = create_storage().await;
        let namespace = "demo";
        let task_name = "say_hello";
        let payload = b"{\"key\": \"value\"}";

        let result = storage.spawn_task(namespace, task_name, payload, None).await;
        assert!(result.is_ok(), "Failed to spawn job: {result:?}");

        let spawn_res = result.unwrap();
        assert!(!spawn_res.task_id.to_string().is_empty());
        assert!(!spawn_res.run_id.to_string().is_empty());
    }

    #[tokio::test]
    async fn test_complete_run_not_running() {
        let storage = create_storage().await;
        let namespace = "demo";
        let task_name = "say_hello";
        let payload = b"{\"key\": \"value\"}";
        let result = storage.spawn_task(namespace, task_name, payload, None).await;
        assert!(result.is_ok(), "Failed to spawn job: {result:?}");

        let spawn_res = result.unwrap();
        let res = storage.complete_run(spawn_res.run_id, b"{\"result\": \"success\"}").await;
        assert!(res.is_err());
        assert!(matches!(res.err().unwrap(), TaskTurbineError::NotRunning {..}));
    }

    #[tokio::test]
    async fn test_complete_run_success() {
        todo!();
    }
}
