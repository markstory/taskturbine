use sqlx::QueryBuilder;
use sqlx::{ConnectOptions, PgPool, postgres::PgConnectOptions};
use taskturbine_core::config::Config;
use taskturbine_core::models::{Checkpoint, Run, RunId, Task, TaskId, TaskState};
use taskturbine_core::storage::StorageError;

/// Filtering options for task_list()
#[derive(Debug, Clone)]

pub struct RunListOptions {
    pub task_id: Option<TaskId>,
}

pub struct RunGetOptions {
    pub run_id: RunId,
}

/// Filtering options for task_list()
#[derive(Debug, Clone)]
pub struct TaskListOptions {
    /// A regexp pattern to match task names against.
    pub taskname: Option<String>,

    /// The task state to filter by
    pub state: Option<TaskState>,

    /// The channel the task was spawned on
    pub channel: Option<String>,

    /// The number of records to fetch
    pub limit: i32,
}

/// Filtering options for task_get()
#[derive(Debug, Clone)]
pub struct TaskGetOptions {
    pub task_id: TaskId,
}

/// Container for a Task and its relations.
pub struct TaskDetails {
    pub task: Task,
    pub runs: Vec<Run>,
    pub checkpoints: Vec<Checkpoint>,
}

pub struct RunDetails {
    pub run: Run,
    pub checkpoints: Vec<Checkpoint>,
}

/// Administrative storage API. Used by the CLI to access storage with a supported API.
/// If you need to build ad-hoc scripting of storage, this interface will provide backwards
/// compatibility across schema changes.
///
/// Building against the database schema is not recommended.
pub struct AdminStorage {
    config: Config,
    pool: PgPool,
}

impl AdminStorage {
    /// Create a new runtime from the given configuration.
    pub fn new(config: Config) -> Self {
        let pool = PgPool::connect_lazy(&config.database_url)
            .expect("Failed to create database connection pool");

        let options: Result<PgConnectOptions, _> = config.database_url.parse();
        if let Ok(mut opts) = options {
            if config.database_log_queries {
                opts = opts.log_statements(log::LevelFilter::Debug);
            } else {
                opts = opts.disable_statement_logging();
            }
            pool.set_connect_options(opts);
        }
        Self { config, pool }
    }

    /// Get a list of tasks.
    pub async fn task_list(&self, options: TaskListOptions) -> Result<Vec<Task>, StorageError> {
        let mut query = QueryBuilder::new("SELECT * FROM taskturbine.tasks WHERE ");
        let mut clauses = query.separated(" AND ");
        clauses.push("usecase = ");
        clauses.push_bind_unseparated(&self.config.usecase);

        if let Some(name) = options.taskname {
            clauses.push("task_name ~ ");
            clauses.push_bind_unseparated(name);
        }
        if let Some(state) = options.state {
            clauses.push("state = ");
            clauses.push_bind_unseparated(state.to_string());
        }
        if let Some(value) = options.channel {
            clauses.push("channel = ");
            clauses.push_bind_unseparated(value);
        }
        query.push(" ORDER BY created_at DESC");
        query.push(" LIMIT ");
        query.push_bind(options.limit);

        let res: Result<Vec<Task>, sqlx::Error> =
            query.build_query_as().fetch_all(&self.pool).await;

        let tasks = res.map_err(StorageError::SqlError)?;
        Ok(tasks)
    }

    /// Get a task and related runs and checkpoint state
    pub async fn task_get(&self, options: TaskGetOptions) -> Result<TaskDetails, StorageError> {
        let task_id: TaskId = options
            .task_id
            .try_into()
            .map_err(|_| StorageError::ValidationError("invalid task_id".to_string()))?;

        let task: Task =
            sqlx::query_as("SELECT * FROM taskturbine.tasks WHERE task_id = $1 AND usecase = $2")
                .bind(task_id)
                .bind(&self.config.usecase)
                .fetch_one(&self.pool)
                .await
                .map_err(StorageError::SqlError)?;

        let runs: Vec<Run> = sqlx::query_as(
            "SELECT * FROM taskturbine.runs WHERE task_id = $1 ORDER BY attempt, created_at",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::SqlError)?;

        let checkpoints: Vec<Checkpoint> = sqlx::query_as(
            "SELECT * FROM taskturbine.checkpoints WHERE task_id = $1 ORDER BY updated_at",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::SqlError)?;

        Ok(TaskDetails {
            task,
            runs,
            checkpoints,
        })
    }

    /// Get a list of runs based on filtering options
    pub async fn run_list(&self, options: RunListOptions) -> Result<Vec<Run>, StorageError> {
        let mut query = QueryBuilder::new(
            "SELECT runs.* 
            FROM taskturbine.runs AS runs
            INNER JOIN taskturbine.tasks AS tasks on tasks.task_id = runs.task_id
            WHERE ",
        );
        let mut clauses = query.separated(" AND ");
        clauses.push("tasks.usecase = ");
        clauses.push_bind_unseparated(&self.config.usecase);

        if let Some(task_id) = options.task_id {
            clauses.push("runs.task_id = ");
            clauses.push_bind_unseparated(task_id);
        }
        query.push(" ORDER BY runs.created_at DESC");

        let res: Result<Vec<Run>, sqlx::Error> = query.build_query_as().fetch_all(&self.pool).await;

        let runs = res.map_err(StorageError::SqlError)?;
        Ok(runs)
    }

    pub async fn run_get(&self, options: RunGetOptions) -> Result<RunDetails, StorageError> {
        let run: Run = sqlx::query_as(
            "SELECT runs.* FROM taskturbine.runs AS runs
                INNER JOIN taskturbine.tasks AS tasks ON tasks.task_id = runs.task_id
                WHERE runs.run_id = $1 AND tasks.usecase = $2",
        )
        .bind(options.run_id)
        .bind(&self.config.usecase)
        .fetch_one(&self.pool)
        .await
        .map_err(StorageError::SqlError)?;

        let checkpoints: Vec<Checkpoint> = sqlx::query_as(
            "SELECT * FROM taskturbine.checkpoints WHERE owner_run_id = $1 ORDER BY updated_at",
        )
        .bind(options.run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::SqlError)?;

        Ok(RunDetails { run, checkpoints })
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use taskturbine_core::storage::Storage;

    use super::*;

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn create_config() -> Config {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        Config {
            usecase: "test".to_string(),
            database_url: db_url,
            database_log_queries: true,
            ..Config::default()
        }
    }

    async fn create_storage() -> Storage {
        let config = create_config();
        let storage = Storage::new(config);

        // Ensure migrations have been applied and that storage is cleared.
        storage.update_schema().await.unwrap();

        storage
    }

    async fn create_admin_storage() -> AdminStorage {
        let config = create_config();

        AdminStorage::new(config)
    }

    #[tokio::test]
    async fn task_list_name_regexp() {
        let ts = now();
        let channel = format!("task_list_name_regexp_{ts}");
        let storage = create_storage().await;
        let admin_storage = create_admin_storage().await;
        let _ = storage.spawn_task(&channel, "register-user", b"", None).await;
        let _ = storage.spawn_task(&channel, "foo-user", b"", None).await;
        let _ = storage.spawn_task(&channel, "other-task", b"", None).await;

        let options = TaskListOptions {
            taskname: Some(".*-user".to_string()),
            state: None,
            channel: Some(channel.clone()),
            limit: 100,
        };
        let res = admin_storage.task_list(options).await;
        let tasks = res.expect("should be ok");
        assert_eq!(2, tasks.len());

        let options = TaskListOptions {
            taskname: Some(".*-nope".to_string()),
            state: None,
            channel: Some(channel),
            limit: 100,
        };
        let res = admin_storage.task_list(options).await;
        let tasks = res.expect("should be ok");
        assert_eq!(0, tasks.len());
    }

    #[tokio::test]
    async fn task_get() {
        let channel = "task_get";
        let storage = create_storage().await;
        let admin_storage = create_admin_storage().await;
        let spawn_res = storage.spawn_task(&channel, "register-user", b"", None).await;
        let spawned = spawn_res.expect("should be ok");

        let options = TaskGetOptions {
            task_id: spawned.task_id,
        };
        let res = admin_storage.task_get(options).await;
        let details = res.expect("should be ok");
        assert_eq!(details.task.task_id, spawned.task_id);

        let options = TaskListOptions {
            taskname: Some(".*-nope".to_string()),
            state: None,
            channel: Some(channel.to_string()),
            limit: 100,
        };
        let res = admin_storage.task_list(options).await;
        let tasks = res.expect("should be ok");
        assert_eq!(0, tasks.len());
    }

    #[tokio::test]
    async fn run_list_name_regexp() {
        let channel = "run_list_name_regexp";
        let storage = create_storage().await;
        let admin_storage = create_admin_storage().await;
        let spawn_res = storage.spawn_task(channel, "register-user", b"", None).await;
        let spawned = spawn_res.expect("should be ok");
        let _ = storage.spawn_task(channel, "foo-user", b"", None).await;

        let options = RunListOptions {
            task_id: Some(spawned.task_id),
        };
        let res = admin_storage.run_list(options).await;
        let runs = res.expect("should be ok");

        assert_eq!(1, runs.len());
    }
}
