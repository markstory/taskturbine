use std::collections::HashMap;

use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core;

#[pyclass(module = "taskturbine")]
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

/// Internal blocking storage adapter.
/// Bridges between the tokio based runtime of the rust library
/// with sync python.
struct BlockingStorage {
    /// The Storage interface. This struct generally needs to be run
    /// in a tokio runtime.
    inner: taskturbine_core::storage::Storage,
    /// The tokio runtime for interacting with taskturbine_core
    /// which is tokio based.
    rt: tokio::runtime::Runtime,
}

impl BlockingStorage {
    /// Create a new BlockingStorage instance
    pub fn new(config: Config) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inner = rt.block_on(taskturbine_core::storage::Storage::new_fut(config.into()));

        Self { inner, rt }
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.spawn_task()`]
    pub fn spawn_task(
        &self,
        channel: &str,
        task_name: &str,
        params: &[u8],
        options: Option<taskturbine_core::storage::TaskOptions>,
    ) -> Result<taskturbine_core::models::SpawnResult, taskturbine_core::storage::TaskTurbineError>
    {
        self.rt
            .block_on(self.inner.spawn_task(channel, task_name, params, options))
    }
}

#[pyclass(module = "taskturbine_ext")]
struct TaskturbineApp {
    #[pyo3(get)]
    config: Config,

    /// The set of channels that have been defined.
    #[pyo3(get)]
    channels: Vec<String>,

    /// A map of all registered tasks.
    tasks: HashMap<String, Task>,

    /// A blocking wrapper on taskturbine_core::storage::Storage
    storage: BlockingStorage,
}

#[pymethods]
impl TaskturbineApp {
    #[new]
    fn py_new(config: Config) -> Self {
        let channels = vec![config.default_channel.clone()];
        let storage = BlockingStorage::new(config.clone());

        TaskturbineApp {
            config,
            channels,
            tasks: HashMap::new(),
            storage,
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

    /// Spawn a task on the default channel and initialize the first run.
    ///
    /// An error is returned if the task name is not registered.
    fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: TaskOptions,
    ) -> PyResult<SpawnResult> {
        if !self.tasks.contains_key(task_name) {
            return Err(PyValueError::new_err(format!(
                "The task `{task_name}` is not registered."
            )));
        }
        let result = self.storage.spawn_task(
            &self.config.default_channel,
            task_name,
            params,
            Some(options.into()),
        );

        result
            .map(|v| v.into())
            .map_err(|v| PyValueError::new_err(format!("Could not spawn task: {v:?}")))
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

/// The result of spawning a task.
#[pyclass]
#[derive(Debug, PartialEq, Clone)]
struct SpawnResult {
    #[pyo3(get)]
    run_id: String,
    #[pyo3(get)]
    task_id: String,
}

/// Convert from the python module to the core struct.
impl TryFrom<SpawnResult> for taskturbine_core::models::SpawnResult {
    type Error = PyErr;

    fn try_from(value: SpawnResult) -> Result<Self, Self::Error> {
        let Ok(task_uuid): Result<uuid::Uuid, _> = value.task_id.try_into() else {
            return Err(PyValueError::new_err("Invalid task_id"));
        };
        let Ok(run_uuid): Result<uuid::Uuid, _> = value.run_id.try_into() else {
            return Err(PyValueError::new_err("Invalid task_id"));
        };
        let task_id = taskturbine_core::models::TaskId(task_uuid);
        let run_id = taskturbine_core::models::RunId(run_uuid);

        Ok(taskturbine_core::models::SpawnResult { task_id, run_id })
    }
}

/// Convert from storage API to python binding
impl From<taskturbine_core::models::SpawnResult> for SpawnResult {
    fn from(value: taskturbine_core::models::SpawnResult) -> SpawnResult {
        let task_id = value.task_id.0.into();
        let run_id = value.run_id.0.into();

        SpawnResult { task_id, run_id }
    }
}

#[pyclass]
#[derive(Debug, PartialEq, Clone)]
struct TaskOptions {
    /// Map of headers to include with the task activation
    pub headers: HashMap<String, String>,

    /// The maximum number of attempts to make on this task
    pub max_attempts: i32,

    /// The minimum number of seconds to wait between retries.
    pub retry_seconds: i32,

    /// The multipier to apply to retry delays between attempts.
    /// Use > 1.0 to create exponential backoff.
    pub retry_factor: f64,

    /// The maximum number of seconds to wait between retries.
    pub retry_max_seconds: i32,

    /// The maximum age of a task before it should not be run.
    /// Measured in seconds from when the task was created.
    pub cancellation_max_age: i32,
}

/// Convert from python to taskturbine_core
impl From<TaskOptions> for taskturbine_core::storage::TaskOptions {
    fn from(value: TaskOptions) -> taskturbine_core::storage::TaskOptions {
        let mut out = taskturbine_core::storage::TaskOptions::default();
        out.headers = value.headers;
        out.max_attempts = value.max_attempts;
        out.retry_seconds = value.retry_seconds;
        out.retry_factor = value.retry_factor;
        out.retry_max_seconds = value.retry_max_seconds;
        out.cancellation_max_age = value.cancellation_max_age;
        out
    }
}

#[pymethods]
impl TaskOptions {
    #[new]
    #[pyo3(signature = (
        *,
        max_attempts,
        retry_seconds,
        retry_factor,
        retry_max_seconds,
        cancellation_max_age
    ))]
    fn __new__(
        max_attempts: i32,
        retry_seconds: i32,
        retry_factor: f64,
        retry_max_seconds: i32,
        cancellation_max_age: i32
    ) -> Self {
        Self {
            headers: HashMap::new(),
            max_attempts,
            retry_seconds,
            retry_factor,
            retry_max_seconds,
            cancellation_max_age,
        }
    }

    #[pyo3(signature = (
        *,
        headers,
        max_attempts,
        retry_seconds,
        retry_factor,
        retry_max_seconds,
        cancellation_max_age
    ))]
    fn copy_with(
        &self,
        headers: Option<HashMap<String, String>>,
        max_attempts: Option<i32>,
        retry_seconds: Option<i32>,
        retry_factor: Option<f64>,
        retry_max_seconds: Option<i32>,
        cancellation_max_age: Option<i32>,
    ) -> Self {
        let mut copied = self.clone();
        if let Some(value) = headers {
            copied.headers = value;
        }
        if let Some(value) = max_attempts {
            copied.max_attempts = value;
        }
        if let Some(value) = retry_seconds {
            copied.retry_seconds = value;
        }
        if let Some(value) = retry_factor {
            copied.retry_factor = value;
        }
        if let Some(value) = retry_max_seconds {
            copied.retry_max_seconds = value;
        }
        if let Some(value) = cancellation_max_age {
            copied.cancellation_max_age = value;
        }
        copied
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
    use super::SpawnResult;
    #[pymodule_export]
    use super::Task;
    #[pymodule_export]
    use super::TaskOptions;
    #[pymodule_export]
    use super::TaskturbineApp;
}
