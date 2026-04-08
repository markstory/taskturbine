use sqlx::QueryBuilder;
use sqlx::{ConnectOptions, PgPool, postgres::PgConnectOptions};
use taskturbine_core::config::Config;
use taskturbine_core::models::{Task, TaskState};
use taskturbine_core::storage::StorageError;

/// Filtering options for task_list()
pub struct TaskListOptions {
    /// A substring to match task names against. 
    /// TODO make this a glob pattern
    pub taskname: Option<String>,

    /// The task state to filter by
    pub state: Option<TaskState>,
}

/// Administrative storage API. Used by the CLI to access storage with a supported API.
/// If you need to build ad-hoc scripting of storage, this interface will provide backwards
/// compatibility across schema changes.
///
/// Building against the database schema is not recommended.
pub struct AdminStorage {
    _config: Config,
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
        Self { _config: config, pool }
    }

    /// Get a list of tasks.
    pub async fn task_list(&self, options: TaskListOptions) -> Result<Vec<Task>, StorageError> {
        let mut query = QueryBuilder::new(
            "SELECT * FROM taskturbine.tasks WHERE "
        );

        let mut added = false;
        let mut clauses = query.separated(" AND ");
        if let Some(name) = options.taskname {
            added = true;
            clauses.push(" task_name = ");
            clauses.push_bind_unseparated(name);
        }
        if let Some(state) = options.state {
            added = true;
            clauses.push(" state = ");
            clauses.push_bind_unseparated(state.to_string());
        }
        if !added {
            query.push("1 = 1");
        }
        query.push(" ORDER BY created_at DESC");

        let res: Result<Vec<Task>, sqlx::Error> = query.build_query_as()
                .fetch_all(&self.pool)
                .await;

        let tasks = res.map_err(StorageError::SqlError)?;
        Ok(tasks)
    }
}
