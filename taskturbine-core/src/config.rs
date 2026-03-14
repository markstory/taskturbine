/// Configuration options for TaskTurbine workers and clients.
///
/// TODO make this easier to load from environment variables or a config file.
#[derive(Debug, Clone)]
pub struct Config {
    /// The URI of the database your are connecting to.
    /// Example: postgresql://app:password@localhost/taskturbine
    pub database_url: String,

    /// Enable database logging at DEBUG level
    pub database_log_queries: bool,

    /// The application or client that is connecting.
    /// Workers are bound to a specific usecase and can conditionally
    /// consume from one or more channel (aka. queue/topic)
    pub usecase: String,

    /// The default channel that tasks are spawned into.
    /// This channel will automatically be registered into the application
    /// using a config instance.
    pub default_channel: String,

    /// The number of task execution slots to start.
    /// More slots will enable more tasks to run concurrently.
    pub worker_concurrency: i32,

    /// The number of seconds a worker should sleep when no tasks are available.
    pub worker_sleep_secs: i32,

    /// The maximum number of completed tasks and events
    /// a worker will delete in a single cleanup operation.
    pub worker_cleanup_limit: i32,

    /// The age of completed tasks and events in seconds
    /// after now() that are safe to delete.
    pub worker_cleanup_cutoff_secs: i32,

    /// The minimum number of seconds between each cleanup operation.
    pub worker_cleanup_interval_secs: i32,

    /// Whether or not workers should run cleanup operations inline.
    /// Set to false if you are going to run cleanup workers separately.
    pub worker_cleanup_inline: bool,

    /// The number of seconds that workers will claim tasks for.
    /// Workers are expected to complete tasks within their claim timeout.
    /// After a claim timeout is exceeded, the task will be made pending again.
    /// Default value is 600 (10m)
    pub worker_claim_timeout_secs: i32,

    /// The default number of seconds that events are waited on for.
    pub await_event_default_timeout_secs: i32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            database_url: "".to_string(),
            database_log_queries: false,
            usecase: "default".to_string(),
            default_channel: "default".to_string(),
            worker_concurrency: 3,
            worker_sleep_secs: 2,
            worker_cleanup_inline: true,
            worker_cleanup_interval_secs: 10,
            worker_cleanup_cutoff_secs: 60 * 10,
            worker_cleanup_limit: 1000,
            worker_claim_timeout_secs: 60 * 10,
            await_event_default_timeout_secs: 120,
        }
    }
}
