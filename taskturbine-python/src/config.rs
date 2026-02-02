use pyo3::prelude::*;

#[pyclass(module = "taskturbine")]
#[derive(Debug, Clone)]
pub struct Config {
    /// The path to the `package.module:app_var` of the python application to work with.
    /// The worker runtime will import this symbol and use it to lookup and execute tasks
    #[pyo3(get, set)]
    pub app_module: String,

    /// The URI of the database your are connecting to.
    /// Example: postgresql://app:password@localhost/taskturbine
    #[pyo3(get, set)]
    pub database_url: String,

    /// Enable database logging at DEBUG level
    #[pyo3(get, set)]
    pub database_log_queries: bool,

    /// The application or client that is connecting.
    /// Workers are bound to a specific usecase and can conditionally
    /// consume from one or more channel (aka. queue/topic)
    #[pyo3(get, set)]
    pub usecase: String,

    /// The default channel that tasks are spawned into.
    /// This channel will automatically be registered into the application
    /// using a config instance.
    #[pyo3(get, set)]
    pub default_channel: String,

    /// The number of task execution slots to start.
    /// More slots will enable more tasks to run concurrently.
    #[pyo3(get, set)]
    pub worker_concurrency: i32,

    /// The number of seconds a worker should sleep when no tasks are available.
    #[pyo3(get, set)]
    pub worker_sleep_secs: i32,

    /// The maximum number of completed tasks and events
    /// a worker will delete in a single cleanup operation.
    #[pyo3(get, set)]
    pub worker_cleanup_limit: i32,

    /// The age of completed tasks and events in seconds
    /// after now() that are safe to delete.
    #[pyo3(get, set)]
    pub worker_cleanup_cutoff_secs: i32,

    /// The minimum number of seconds between each cleanup operation.
    #[pyo3(get, set)]
    pub worker_cleanup_interval_secs: i32,

    /// Whether or not workers should run cleanup operations inline.
    /// Set to false if you are going to run cleanup workers separately.
    #[pyo3(get, set)]
    pub worker_cleanup_inline: bool,

    /// The default number of seconds that events are waited on for.
    #[pyo3(get, set)]
    pub await_event_default_timeout_secs: i32,

    /// The number of seconds that workers will claim tasks for.
    /// Workers are expected to complete tasks within their claim timeout.
    /// After a claim timeout is exceeded, the task will be made pending again.
    /// Default value is 600 (10m)
    #[pyo3(get, set)]
    pub worker_claim_timeout_secs: i32,
}

/// Convert from the python module to the core struct.
impl From<Config> for taskturbine_core::config::Config {
    fn from(value: Config) -> Self {
        // Jank! This is gross but I'm hacking to learn more.
        let mut core_config = taskturbine_core::config::Config::default();
        core_config.database_url = value.database_url;
        core_config.database_log_queries = value.database_log_queries;
        core_config.usecase = value.usecase;
        core_config.default_channel = value.default_channel;
        core_config.worker_concurrency = value.worker_concurrency;
        core_config.worker_sleep_secs = value.worker_sleep_secs;
        core_config.worker_cleanup_limit = value.worker_cleanup_limit;
        core_config.worker_cleanup_interval_secs = value.worker_cleanup_interval_secs;
        core_config.worker_cleanup_inline = value.worker_cleanup_inline;
        core_config.worker_cleanup_cutoff_secs = value.worker_cleanup_cutoff_secs;
        core_config.worker_claim_timeout_secs = value.worker_claim_timeout_secs;
        core_config.await_event_default_timeout_secs = value.await_event_default_timeout_secs;

        core_config
    }
}

#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (
        app_module,
        database_url,
        database_log_queries=false,
        usecase="default",
        default_channel="default",
        worker_concurrency=3,
        worker_sleep_secs=2,
        worker_cleanup_limit=1000,
        worker_cleanup_interval_secs=30,
        worker_cleanup_inline=true,
        worker_cleanup_cutoff_secs=600,
        worker_claim_timeout_secs=600,
        await_event_default_timeout_secs=120,
    ))]
    fn __new__(
        app_module: &str,
        database_url: &str,
        database_log_queries: bool,
        usecase: &str,
        default_channel: &str,
        worker_concurrency: i32,
        worker_sleep_secs: i32,
        worker_cleanup_limit: i32,
        worker_cleanup_interval_secs: i32,
        worker_cleanup_inline: bool,
        worker_cleanup_cutoff_secs: i32,
        worker_claim_timeout_secs: i32,
        await_event_default_timeout_secs: i32,
    ) -> PyResult<Self> {
        let config = Config {
            app_module: app_module.to_string(),
            database_url: database_url.to_string(),
            database_log_queries,
            usecase: usecase.to_string(),
            default_channel: default_channel.to_string(),
            worker_concurrency,
            worker_sleep_secs,
            worker_cleanup_limit,
            worker_cleanup_interval_secs,
            worker_cleanup_inline,
            worker_cleanup_cutoff_secs,
            worker_claim_timeout_secs,
            await_event_default_timeout_secs,
        };

        Ok(config)
    }
}
