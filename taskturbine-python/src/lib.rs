use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::{
    self,
    models::{RunId, TaskId},
    storage::Storage,
};

mod asynclib;
mod config;
mod models;

use config::Config;
use models::{AwaitResult, Checkpoint, ClaimedTask, SpawnResult};

///! See taskturbine.pyi for docstrings

// Container for configuration, storage and tokio runtime.
#[pyclass(skip_from_py_object)]
struct AppInner {
    #[pyo3(get)]
    config: Config,

    /// The set of channels that have been defined.
    #[pyo3(get)]
    channels: HashSet<String>,

    /// A blocking wrapper on taskturbine_core::storage::Storage
    storage: Arc<Storage>,

    /// Tokio runtime for running storage operations.
    runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl AppInner {
    #[new]
    fn py_new(config: Config) -> Self {
        let mut channels = HashSet::new();
        channels.insert(config.default_channel.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let storage = runtime.block_on(async { Storage::new(config.clone().into()) });

        AppInner {
            config,
            channels,
            storage: Arc::new(storage),
            runtime: Arc::new(runtime),
        }
    }

    fn add_channel(&mut self, name: String) {
        self.channels.insert(name);
    }

    fn set_channels(&mut self, names: Vec<String>) {
        self.channels.clear();
        for name in names.iter() {
            self.channels.insert(name.clone());
        }
    }

    fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: TaskOptions,
    ) -> PyResult<SpawnResult> {
        self.channel_spawn_task(&self.config.default_channel, task_name, params, options)
    }

    fn channel_spawn_task(
        &self,
        channel: &str,
        task_name: &str,
        params: &[u8],
        options: TaskOptions,
    ) -> PyResult<SpawnResult> {
        let result = self.runtime.block_on(self.storage.spawn_task(
            channel,
            task_name,
            params,
            Some(options.into()),
        ));
        result
            .map(|v| v.into())
            .map_err(|v| PyValueError::new_err(format!("Could not spawn task: {v:?}")))
    }

    fn emit_event(&self, event_name: &str, payload: &[u8]) -> PyResult<()> {
        let res = self
            .runtime
            .block_on(self.storage.emit_event(event_name, payload));

        res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    fn update_schema(&self) -> PyResult<()> {
        let res = self.runtime.block_on(self.storage.update_schema());

        res.map_err(|v| PyValueError::new_err(format!("Could not update_schema: {v:?}")))
    }

    fn create_worker(&self, worker_id: String, channels: Vec<String>) -> WorkerInner {
        WorkerInner {
            config: self.config.clone(),
            storage: self.storage.clone(),
            runtime: self.runtime.clone(),
            idle_count: 0,
            worker_id,
            channels,
        }
    }

    fn create_context(&self, claimed_task: ClaimedTask) -> ContextInner {
        ContextInner {
            storage: self.storage.clone(),
            runtime: self.runtime.clone(),
            claimed_task,
        }
    }
}

/// Expose the minimal worker API to be used by the python worker.
#[pyclass(from_py_object)]
#[derive(Clone)]
struct WorkerInner {
    config: Config,
    storage: Arc<Storage>,
    channels: Vec<String>,
    worker_id: String,
    runtime: Arc<tokio::runtime::Runtime>,
    idle_count: i32,
}

#[pymethods]
impl WorkerInner {
    #[getter(usecase)]
    pub fn usecase(&self) -> String {
        self.config.usecase.clone()
    }

    #[getter(app_module)]
    pub fn app_module(&self) -> String {
        self.config.app_module.clone()
    }

    #[getter(worker_sleep_ms)]
    pub fn worker_sleep_ms(&self) -> i32 {
        self.config.worker_sleep_ms
    }

    #[getter(worker_upkeep_interval_secs)]
    pub fn worker_upkeep_interval_secs(&self) -> i32 {
        self.config.worker_upkeep_interval_secs
    }

    #[getter(worker_concurrency)]
    pub fn worker_concurrency(&self) -> i32 {
        self.config.worker_concurrency
    }

    #[getter(worker_max_tasks_per_child)]
    pub fn worker_max_tasks_per_child(&self) -> i32 {
        self.config.worker_max_tasks_per_child
    }

    /// Claim a collection tasks for timeout seconds.
    fn claim_tasks(&mut self) -> PyResult<Vec<ClaimedTask>> {
        let channels: Vec<&str> = self.channels.iter().map(|c| c.as_ref()).collect();
        let timeout = Duration::from_secs(self.config.worker_claim_timeout_secs as u64);
        let claim_res = self.runtime.block_on(self.storage.claim_task(
            channels,
            &self.worker_id,
            timeout,
            self.config.worker_concurrency,
        ));

        claim_res
            .map(|v| {
                let mapped: Vec<ClaimedTask> = v.into_iter().map(|task| task.into()).collect();
                if mapped.is_empty() {
                    self.idle_count += 1;
                } else {
                    self.idle_count = 0;
                }
                mapped
            })
            .map_err(|e| PyValueError::new_err(format!("Could not claim tasks: {e:?}")))
    }

    /// Run all the upkeep operations on the database.
    fn run_upkeep(&self) -> PyResult<()> {
        self.runtime
            .block_on(self.storage.run_upkeep())
            .map_err(|e| PyValueError::new_err(format!("Upkeep failed: {e:?}")))
    }

    // Should upkeep be run right now by a Worker?
    // Set `config.worker_upkeep_inline` to false if you are running a dedicated
    // upkeep worker.
    fn should_run_upkeep(&self, timestamp: i64) -> bool {
        if !self.config.worker_upkeep_inline {
            return false;
        }
        let now = Utc::now().timestamp();
        let delta = now - timestamp;
        if delta < self.config.worker_upkeep_interval_secs as i64 {
            return false;
        }
        true
    }

    pub fn should_shutdown(&self) -> bool {
        if !self.config.worker_shutdown_on_idle {
            return false;
        }
        if self.idle_count < self.config.worker_shutdown_idle_max {
            return false;
        }
        true
    }

    /// Mark a run as failed.
    fn fail_run(
        &self,
        run_id: String,
        reason: Option<&[u8]>,
        retry_at: Option<Duration>,
    ) -> PyResult<()> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        self.runtime
            .block_on(
                self.storage
                    .fail_run(run_id, reason.unwrap_or(b""), retry_at),
            )
            .map_err(|e| PyValueError::new_err(format!("Could not fail_run: {e:?}")))
    }

    /// Mark a run as complete.
    fn complete_run(&self, run_id: String, run_result: Vec<u8>) -> PyResult<()> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        self.runtime
            .block_on(self.storage.complete_run(run_id, &run_result))
            .map_err(|e| PyValueError::new_err(format!("Could not complete_run: {e:?}")))
    }

    /// Re-schedule a task to run in the future.
    fn schedule_run(&self, run_id: String, wait_for: Duration) -> PyResult<()> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        self.runtime
            .block_on(self.storage.schedule_run(run_id, wait_for))
            .map_err(|e| PyValueError::new_err(format!("Could not schedule_run: {e:?}")))
    }
}

/// See taskturbine.pyi for docstrings
#[pyclass(skip_from_py_object)]
struct ContextInner {
    storage: Arc<Storage>,
    runtime: Arc<tokio::runtime::Runtime>,
    claimed_task: ClaimedTask,
}
#[pymethods]
impl ContextInner {
    #[getter(usecase)]
    fn usecase(&self) -> String {
        self.storage.get_config().usecase.to_owned()
    }

    #[getter(await_event_default_timeout_secs)]
    fn await_event_default_timeout_secs(&self) -> i32 {
        self.storage.get_config().await_event_default_timeout_secs
    }

    #[getter(claimed_task)]
    fn get_claimed_task(&self) -> ClaimedTask {
        self.claimed_task.clone()
    }

    fn emit_event(&self, event_name: String, payload: &[u8]) -> PyResult<()> {
        let res = self
            .runtime
            .block_on(self.storage.emit_event(&event_name, payload));

        res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    fn get_checkpoint(&self, checkpoint_name: String) -> PyResult<Checkpoint> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let res = self
            .runtime
            .block_on(self.storage.get_checkpoint(task_id, &checkpoint_name));
        if let Ok(Some(checkpoint)) = res {
            Ok(checkpoint.into())
        } else {
            Err(PyValueError::new_err(
                "Checkpoint not found, or read failed",
            ))
        }
    }

    fn set_checkpoint(
        &self,
        checkpoint_name: &str,
        state: &[u8],
        extend_claim: Option<Duration>,
    ) -> PyResult<()> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let Ok(run_id) = TryInto::<RunId>::try_into(&self.claimed_task.run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };

        let res = self.runtime.block_on(self.storage.set_checkpoint(
            task_id,
            run_id,
            checkpoint_name,
            state,
            extend_claim,
        ));

        res.map_err(|v| PyValueError::new_err(format!("Could not store checkpoint {v:?}")))
    }

    fn get_event_payload(&self, event_name: String, timeout: Duration) -> PyResult<AwaitResult> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let Ok(run_id) = TryInto::<RunId>::try_into(&self.claimed_task.run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };

        let step_name = format!("$awaitEvent:{event_name}");
        let payload_res = self.runtime.block_on(self.storage.await_event(
            task_id,
            run_id,
            &step_name,
            event_name.as_ref(),
            Some(timeout),
        ));
        match payload_res {
            Ok(result) => Ok(result.into()),
            Err(err) => Err(PyValueError::new_err(format!(
                "Could not await_event: {err:?}"
            ))),
        }
    }
}

/// See taskturbine.pyi for docstrings
#[pyclass(from_py_object)]
#[derive(Debug, PartialEq, Clone)]
struct TaskOptions {
    pub idempotency_key: Option<String>,
    pub headers: HashMap<String, String>,
    pub max_attempts: i32,
    pub retry_seconds: i32,
    pub retry_factor: f64,
    pub retry_max_seconds: i32,
    pub cancellation_max_age: i32,
}

/// Convert from python to taskturbine_core
impl From<TaskOptions> for taskturbine_core::storage::TaskOptions {
    fn from(value: TaskOptions) -> taskturbine_core::storage::TaskOptions {
        taskturbine_core::storage::TaskOptions {
            idempotency_key: value.idempotency_key,
            headers: value.headers,
            max_attempts: value.max_attempts,
            retry_seconds: value.retry_seconds,
            retry_factor: value.retry_factor,
            retry_max_seconds: value.retry_max_seconds,
            cancellation_max_age: value.cancellation_max_age,
        }
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
        cancellation_max_age,
        idempotency_key = None,
    ))]
    fn __new__(
        max_attempts: i32,
        retry_seconds: i32,
        retry_factor: f64,
        retry_max_seconds: i32,
        cancellation_max_age: i32,
        idempotency_key: Option<String>,
    ) -> Self {
        Self {
            idempotency_key,
            headers: HashMap::new(),
            max_attempts,
            retry_seconds,
            retry_factor,
            retry_max_seconds,
            cancellation_max_age,
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        *,
        headers,
        max_attempts,
        retry_seconds,
        retry_factor,
        retry_max_seconds,
        cancellation_max_age,
        idempotency_key
    ))]
    fn copy_with(
        &self,
        headers: Option<HashMap<String, String>>,
        max_attempts: Option<i32>,
        retry_seconds: Option<i32>,
        retry_factor: Option<f64>,
        retry_max_seconds: Option<i32>,
        cancellation_max_age: Option<i32>,
        idempotency_key: Option<String>,
    ) -> Self {
        let mut copied = self.clone();
        if idempotency_key.is_some() {
            copied.idempotency_key = idempotency_key;
        }
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

// Define all of the exports that are part of the native package.
// The mod name, pymodule name both need to align with the ext name
// in Cargo.toml
#[pymodule(name = "taskturbine")]
mod taskturbine {
    #[pymodule_export]
    use super::AppInner;
    #[pymodule_export]
    use super::ClaimedTask;
    #[pymodule_export]
    use super::Config;
    #[pymodule_export]
    use super::ContextInner;
    #[pymodule_export]
    use super::SpawnResult;
    #[pymodule_export]
    use super::TaskOptions;
    #[pymodule_export]
    use super::WorkerInner;
    #[pymodule_export]
    use super::asynclib::AsyncAppInner;
    #[pymodule_export]
    use super::asynclib::AsyncContextInner;
    #[pymodule_export]
    use super::asynclib::AsyncWorkerInner;
}
