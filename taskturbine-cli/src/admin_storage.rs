use sqlx::{ConnectOptions, PgPool, postgres::PgConnectOptions};
use taskturbine_core::config::Config;
use taskturbine_core::models::Task;
use taskturbine_core::storage::StorageError;

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

    pub async fn task_list(&self) -> Result<Vec<Task>, StorageError> {
        // TODO add filtering
        let res: Result<Vec<Task>, sqlx::Error> =
            sqlx::query_as("SELECT * FROM taskturbine.tasks WHERE 1 = 1 ORDER BY created_at DESC")
                .fetch_all(&self.pool)
                .await;

        let tasks = res.map_err(StorageError::SqlError)?;
        Ok(tasks)
    }
}
