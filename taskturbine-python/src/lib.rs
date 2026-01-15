use pyo3::prelude::*;
use taskturbine_core;

#[pyclass(module="taskturbine")]
#[derive(Debug, Clone)]
struct Config {
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

    /// The default number of seconds that events are waited on for.
    pub await_event_default_timeout_secs: i32,
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
        core_config.await_event_default_timeout_secs = value.await_event_default_timeout_secs;

        core_config
    }
}


#[pyclass(module="taskturbine_ext")]
struct TaskturbineApp {
    config: Config,
    channels: Vec<String>,
    // TODO add tasks
}


#[pymethods]
impl TaskturbineApp {
    #[new]
    fn py_new(config: Config) -> Self {
        TaskturbineApp {
            config,
            channels: vec![],
        }
    }

    /// Add a channel to the list of channels this application can publish and consume from.
    fn add_channel(&mut self, value: String, _py: Python<'_>) {
        self.channels.push(value);
    }

    /*
    /// Create a worker by consuming the app.
    ///
    /// A worker will only claim tasks in `channels` if channels is not-empty.
    /// If `channels` is empty, tasks in all channels will be processed.
    ///
    /// ```rust
    /// // Create a worker that consumes from all channels
    /// // in the application.
    /// let worker = app.create_worker("worker-1", vec![]);
    ///
    /// // Create a worker that only consumes `reports` tasks.
    /// let worker = app.create_worker("worker-1", vec!["reports"]);
    /// ```
    pub fn create_worker(self, worker_id: &str, channels: Vec<String>) -> Worker {
        let arc_self = Arc::new(self);
        Worker::new(arc_self, worker_id.to_string(), channels)
    }

    /// Spawn a task on the default channel and initialize the first run.
    ///
    /// An error is returned if the task name is not registered.
    pub async fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, TaskTurbineError> {
        if !self.tasks.contains_key(task_name) {
            return Err(TaskTurbineError::ValidationError(format!(
                "No task named {task_name} is registered."
            )));
        }
        self.storage
            .spawn_task(&self.config.default_channel, task_name, params, options)
            .await
    }

    /// Record an event as having completed.
    /// Events allow you to synchronize tasks with external actions
    /// that can be recorded as events. Events can have a Payload of bytes.
    /// How those bytes are encoded is an application concern.
    ///
    /// ```rust
    /// app.emit_event("email-verify-foo@example.com", payload.as_bytes()).await;
    /// ```
    pub async fn emit_event(&self, event_name: &str, payload: &[u8]) -> Result<(), FlowControl> {
        let res = self.storage.emit_event(event_name, payload).await;

        if let Err(err) = res {
            return Err(FlowControl::Failure(format!(
                "Could not store event {err:?}"
            )));
        }
        Ok(())
    }
    */
}


/// Notes
/// -------
///
/// The worker will likely need to be re-implemented as the python methods
/// won't be callable from the rust worker, the types won't work out.
///
/// Should the app also be python only? Perhaps config, storage, and models are the key parts to
/// reuse.


/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule(name = "taskturbine_ext")]
mod taskturbine {
    #[pymodule_export]
    use super::Config;
    #[pymodule_export]
    use super::TaskturbineApp;
}

// #[pymodule(name = "taskturbine")]
// fn taskturbine_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
//     // m.add_function(wrap_pyfunction!(guess_the_number, m)?)?;

//     Ok(())
// }
