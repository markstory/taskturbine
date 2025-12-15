#[derive(Debug, Clone)]
pub struct Config {
    /// The URI of the database your are connecting to.
    /// Example: postgresql://app:password@localhost/taskturbine
    pub database_url: String,

    /// Enable database logging at DEBUG level
    pub database_log_queries: bool,

    /// The application or client that is connecting.
    /// Workers are bound to a specific usecase and can conditionally
    /// consume from one or more namespaces (aka. queues/topics/channels)
    pub usecase: String,

    /// The number of seconds a worker should sleep when no tasks are available.
    pub worker_sleep_secs: i32,

    /// The maximum number of completed tasks and events
    /// a worker will delete in a single cleanup operation.
    pub worker_cleanup_limit: i32,

    /// The age of completed tasks and events in seconds
    /// after now() that are safe to delete.
    pub worker_cleanup_cutoff_secs: i32,

    /// The probability that a worker will run
    /// the cleanup operations.
    pub worker_cleanup_probability: f64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            database_url: "".to_string(),
            database_log_queries: false,
            usecase: "default".to_string(),
            worker_sleep_secs: 2,
            worker_cleanup_cutoff_secs: 60 * 10,
            worker_cleanup_probability: 0.1,
            worker_cleanup_limit: 1000,
        }
    }
}
