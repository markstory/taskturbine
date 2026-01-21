use std::collections::HashMap;

use pyo3::prelude::*;
use taskturbine_core;

#[pyclass(module="taskturbine")]
#[derive(Debug, Clone)]
struct Config {
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
            await_event_default_timeout_secs,
        };

        Ok(config)
    }
}


#[pyclass(module="taskturbine_ext")]
struct TaskturbineApp {
    #[pyo3(get)]
    config: Config,

    #[pyo3(get)]
    channels: Vec<String>,

    tasks: HashMap<String, Task>
}


#[pymethods]
impl TaskturbineApp {
    #[new]
    fn py_new(config: Config) -> Self {
        let channels = vec![config.default_channel.clone()];

        TaskturbineApp {
            config,
            channels,
            tasks: HashMap::new(),
        }
    }

    /// Add a channel to the list of channels this application can publish and consume from.
    fn add_channel(&mut self, value: String, _py: Python<'_>) {
        self.channels.push(value);
    }

    /// Register task metadata with the rust extension
    /// Task metadata is used to generate python code that is executed by workers.
    fn register_task(&mut self, task: Task) {
        self.tasks.insert(task.task_name.clone(), task);
    }

    /// Check if a task has been registered.
    fn has_task(&self, name: &str) -> bool {
        self.tasks.contains_key(name)
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

/// An individual decorated python task. The expected task function signature is
///
/// ```
/// def __call__(self, *args, **kwargs) -> str | None
/// ```
///
/// The function bindings are held in python, and this struct enables
/// the worker runtime to operate python tasks by using the metadata from this struct
/// to generate code snippets of python that are executed.
#[pyclass]
#[derive(Debug, PartialEq, Clone)]
struct Task {
    /// The python module name of the task. This module is expected to be within
    /// `[Config.app_module]`. This module will be imported when running the task.
    #[pyo3(get, set)]
    pub module_name: String,

    /// The unique name of the task. Tasks having unique names helps ease refactoring
    /// operations as module names are not persisted in task records.
    #[pyo3(get, set)]
    pub task_name: String,
}

#[pymethods]
impl Task {
    #[pyo3(signature = (module_name, task_name))]
    #[new]
    fn new(module_name: &str, task_name: &str) -> PyResult<Self> {
        let task = Task {
            module_name: module_name.to_string(),
            task_name: task_name.to_string(),
        };

        Ok(task)
    }
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
#[pymodule(name = "taskturbine")]
mod taskturbine {
    #[pymodule_export]
    use super::Config;
    #[pymodule_export]
    use super::TaskturbineApp;
    #[pymodule_export]
    use super::Task;
}
