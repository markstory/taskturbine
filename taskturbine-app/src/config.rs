// TODO make a Config in this package so the core config is slimmer.
// Use Into to convert from app -> core config.
pub use taskturbine_core::config::Config as CoreConfig;


/// Configuration options for Taskturbine rust applications.
///
pub struct Config {
    /// The taskturbine-core Config instance.
    pub core: CoreConfig,

    /// The default channel that tasks are spawned into.
    /// This channel will automatically be registered into the application
    /// using a config instance.
    pub default_channel: String,

    /// The number of task execution slots to start.
    /// More slots will enable more tasks to run concurrently.
    pub worker_concurrency: i32,

    /// The number of milliseconds a worker should sleep when
    //  one of the following happens:
    //
    //  - A worker attempts to claim tasks but none are found.
    //  - A worker claims tasks and can't send them to a worker queue.
    //  - A worker c
    pub worker_sleep_ms: i32,

    /// The maximum number of completed tasks and events
    /// a worker will delete in a single cleanup operation.
    pub worker_cleanup_limit: i32,

    /// The age of completed tasks and events in seconds
    /// after now() that are safe to delete.
    pub worker_cleanup_cutoff_secs: i32,

    /// The minimum number of seconds between each cleanup operation.
    /// It is recommended you do this periodically to ensure that your
    /// database doesn't grow indefinitely.
    pub worker_upkeep_interval_secs: i32,

    /// Whether or not workers should run cleanup operations inline.
    /// Set to false if you are going to run cleanup workers separately.
    pub worker_upkeep_inline: bool,

    /// The number of seconds that workers will claim tasks for.
    /// Workers are expected to complete tasks within their claim timeout.
    /// After a claim timeout is exceeded, the task will be made pending again.
    /// Default value is 600 (10m)
    pub worker_claim_timeout_secs: i32,

    /// Whether or not the worker should shutdown on when it is idle.
    pub worker_shutdown_on_idle: bool,

    /// The number empty claim attempts to make before a worker considers
    /// itself idle. If `[worker_shutdown_on_idle]` is set, the worker
    /// will complete its run loop. This is used for integration testing.
    pub worker_shutdown_idle_max: i32,

    /// The default number of seconds that events are waited on for.
    pub await_event_default_timeout_secs: i32,
}

impl Default for AppConfig {
    fn default() -> Self {
        Config {
            core: CoreConfig::default(),
            default_channel: "default".to_string(),
            worker_concurrency: 3,
            worker_sleep_ms: 100,
            worker_upkeep_inline: true,
            worker_upkeep_interval_secs: 10,
            worker_cleanup_cutoff_secs: 60 * 10,
            worker_cleanup_limit: 1000,
            worker_claim_timeout_secs: 60 * 10,
            worker_shutdown_on_idle: false,
            worker_shutdown_idle_max: 5,
            await_event_default_timeout_secs: 120,
        }
    }
}
