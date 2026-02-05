use std::{collections::HashMap, sync::Arc, time::Duration};

use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::{
    self,
    models::{RunId, TaskId},
};
use uuid::Uuid;

mod config;
mod models;

use config::Config;
use models::{AwaitResult, Checkpoint, ClaimedTask, SpawnResult, Task};

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
    pub fn new(config: taskturbine_core::config::Config) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inner = rt.block_on(taskturbine_core::storage::Storage::new_fut(config));

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

    /// Make a blocking call to [`taskturbine_core::storage::Storage.spawn_task()`]
    pub fn emit_event(
        &self,
        event_name: &str,
        payload: &[u8],
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt.block_on(self.inner.emit_event(event_name, payload))
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.spawn_task()`]
    pub fn await_event(
        &self,
        task_id: TaskId,
        run_id: RunId,
        step_name: &str,
        event_name: &str,
        timeout: Option<u64>,
    ) -> Result<taskturbine_core::storage::AwaitResult, taskturbine_core::storage::TaskTurbineError>
    {
        self.rt.block_on(
            self.inner
                .await_event(task_id, run_id, step_name, event_name, timeout),
        )
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.claim_task()`]
    pub fn claim_task(
        &self,
        channels: Vec<&str>,
        worker_id: &str,
        claim_timeout: Duration,
        qty: i32,
    ) -> Result<
        Vec<taskturbine_core::models::ClaimedTask>,
        taskturbine_core::storage::TaskTurbineError,
    > {
        self.rt.block_on(
            self.inner
                .claim_task(channels, worker_id, claim_timeout, qty),
        )
    }

    pub fn get_checkpoint(
        &self,
        task_id: TaskId,
        step_name: &str,
    ) -> Result<
        Option<taskturbine_core::models::Checkpoint>,
        taskturbine_core::storage::TaskTurbineError,
    > {
        self.rt
            .block_on(self.inner.get_checkpoint(task_id, step_name))
    }

    pub fn set_checkpoint(
        &self,
        task_id: TaskId,
        run_id: RunId,
        step_name: &str,
        state: &[u8],
        extend_claim: Option<Duration>,
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt.block_on(
            self.inner
                .set_checkpoint(task_id, run_id, step_name, state, extend_claim),
        )
    }

    /// Get the config of the application
    pub fn get_config(&self) -> taskturbine_core::config::Config {
        self.inner.get_config()
    }
}

#[pyclass]
struct TaskturbineApp {
    #[pyo3(get)]
    config: Config,

    /// The set of channels that have been defined.
    #[pyo3(get)]
    channels: Vec<String>,

    /// A map of all registered tasks.
    tasks: HashMap<String, Task>,

    /// A blocking wrapper on taskturbine_core::storage::Storage
    storage: Arc<BlockingStorage>,
}

#[pymethods]
impl TaskturbineApp {
    #[new]
    fn py_new(config: Config) -> Self {
        let channels = vec![config.default_channel.clone()];
        let storage = BlockingStorage::new(config.clone().into());

        TaskturbineApp {
            config,
            channels,
            tasks: HashMap::new(),
            storage: Arc::new(storage),
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

    /// Record an event as having completed.
    /// Events allow you to synchronize tasks with external actions
    /// that can be recorded as events. Events can have a Payload of bytes.
    /// How those bytes are encoded is an application concern.
    ///
    /// ```rust
    /// app.emit_event("email-verify-foo@example.com", payload.as_bytes()).await;
    /// ```
    fn emit_event(&self, event_name: &str, payload: &[u8]) -> PyResult<()> {
        let res = self.storage.emit_event(event_name, payload);

        res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    fn claim_task(
        &self,
        channels: Vec<String>,
        worker_id: &str,
        claim_timeout: Duration,
        qty: i32,
    ) -> PyResult<Vec<ClaimedTask>> {
        let channels = channels.iter().map(|chan| chan.as_ref()).collect();
        let res = self
            .storage
            .claim_task(channels, worker_id, claim_timeout, qty);

        res.map(|v| {
            let mapped: Vec<ClaimedTask> = v.into_iter().map(|task| task.into()).collect();
            mapped
        })
        .map_err(|v| PyValueError::new_err(format!("Could not claim tasks: {v:?}")))
    }

    /// Create a worker for the application tasks
    ///
    /// A worker will only claim tasks in `channels` if channels is not-empty.
    /// If `channels` is empty, tasks in all channels will be processed.
    fn create_worker(&self, worker_id: String, channels: Vec<String>) -> WorkerInner {
        WorkerInner {
            storage: self.storage.clone(),
            claim_count: self.config.worker_concurrency,
            worker_id,
            channels,
        }
    }

    /// Create a ContextInner which bridges into the python client.
    fn create_context(&self, claimed_task: ClaimedTask) -> ContextInner {
        ContextInner {
            storage: self.storage.clone(),
            claimed_task,
        }
    }
}

/// Expose the minimal worker API to be used by the python worker.
#[pyclass]
struct WorkerInner {
    storage: Arc<BlockingStorage>,
    channels: Vec<String>,
    worker_id: String,
    claim_count: i32,
}

#[pymethods]
impl WorkerInner {
    /// Claim a collection tasks for timeout seconds.
    fn claim_tasks(&self, timeout: Duration) -> Vec<ClaimedTask> {
        vec![]
    }

    /// Run all the cleanup operations on the database.
    fn run_cleanup(&self) -> PyResult<()> {
        Ok(())
    }

    /// Mark a run as failed.
    fn fail_run(&self, run_id: String, retry_at: Option<Duration>) -> PyResult<()> {
        Ok(())
    }

    /// Mark a run as complete.
    fn complete_run(&self, run_id: String) -> PyResult<()> {
        Ok(())
    }

    /// Re-schedule a task to run in the future.
    fn schedule_run(&self, run_id: String, wait_for: Duration) -> PyResult<()> {
        Ok(())
    }

}

/// Expose a minimal interface to the python client.
#[pyclass]
struct ContextInner {
    storage: Arc<BlockingStorage>,
    claimed_task: ClaimedTask,
}
#[pymethods]
impl ContextInner {
    /// Proxy to the config value.
    fn await_event_default_timeout_secs(&self) -> i32 {
        self.storage.get_config().await_event_default_timeout_secs
    }

    /// Record an event taking place.
    fn emit_event(&self, event_name: String, payload: &[u8]) -> PyResult<()> {
        let res = self.storage.emit_event(&event_name, payload);

        res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    /// Get a checkpoint by name for a task.
    /// `checkpoint_name` is expected to be a unique name.
    fn get_checkpoint(&self, checkpoint_name: String) -> PyResult<Checkpoint> {
        // TODO this unwrap() is yolo
        let task_id = Uuid::parse_str(&self.claimed_task.task_id).unwrap();
        let res = self
            .storage
            .get_checkpoint(TaskId(task_id), &checkpoint_name);

        // TODO this is masking a storage error
        if let Ok(Some(checkpoint)) = res {
            Ok(checkpoint.into())
        } else {
            Err(PyValueError::new_err(
                "Checkpoint not found, or read failed",
            ))
        }
    }

    /// Set the state for a named checkpoint.
    /// The caller is responsible for making checkpoint_names unique.
    fn set_checkpoint(
        &self,
        checkpoint_name: &str,
        state: &[u8],
        extend_claim: Option<Duration>,
    ) -> PyResult<()> {
        let task_id = Uuid::parse_str(&self.claimed_task.task_id).unwrap();
        let run_id = Uuid::parse_str(&self.claimed_task.run_id).unwrap();

        let res = self.storage.set_checkpoint(
            TaskId(task_id),
            RunId(run_id),
            checkpoint_name,
            state,
            extend_claim,
        );

        res.map_err(|v| PyValueError::new_err(format!("Could not store checkpoint {v:?}")))
    }

    /// Read the payload for an event.
    /// Will raise an exception if the read fails
    fn get_event_payload(&self, event_name: String, timeout_secs: u64) -> PyResult<AwaitResult> {
        // TODO this is yolo. Should raise errors on invalid values.
        let task_id = Uuid::parse_str(&self.claimed_task.task_id).unwrap();
        let run_id = Uuid::parse_str(&self.claimed_task.run_id).unwrap();

        let step_name = format!("$awaitEvent:{event_name}");
        let payload_res = self.storage.await_event(
            TaskId(task_id),
            RunId(run_id),
            &step_name,
            event_name.as_ref(),
            Some(timeout_secs),
        );
        match payload_res {
            Ok(result) => Ok(result.into()),
            Err(err) => Err(PyValueError::new_err(format!(
                "Could not await_event: {err:?}"
            ))),
        }
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
        cancellation_max_age: i32,
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
    use super::WorkerInner;
    #[pymodule_export]
    use super::ClaimedTask;
    #[pymodule_export]
    use super::Config;
    #[pymodule_export]
    use super::ContextInner;
    #[pymodule_export]
    use super::SpawnResult;
    #[pymodule_export]
    use super::Task;
    #[pymodule_export]
    use super::TaskOptions;
    #[pymodule_export]
    use super::TaskturbineApp;
}
