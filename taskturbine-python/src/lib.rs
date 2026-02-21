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
};

mod blockingstorage;
mod config;
mod models;

use config::Config;
use models::{AwaitResult, Checkpoint, ClaimedTask, SpawnResult};

/// See taskturbine.pyi for docstrings
#[pyclass]
struct AppInner {
    #[pyo3(get)]
    config: Config,

    /// The set of channels that have been defined.
    #[pyo3(get)]
    channels: HashSet<String>,

    /// A blocking wrapper on taskturbine_core::storage::Storage
    storage: Arc<blockingstorage::BlockingStorage>,
}

#[pymethods]
impl AppInner {
    #[new]
    fn py_new(config: Config) -> Self {
        let mut channels = HashSet::new();
        channels.insert(config.default_channel.clone());
        let storage = blockingstorage::BlockingStorage::new(config.clone().into());

        AppInner {
            config,
            channels,
            storage: Arc::new(storage),
        }
    }

    fn add_channel(&mut self, value: String) {
        self.channels.insert(value);
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
        let result = self
            .storage
            .spawn_task(channel, task_name, params, Some(options.into()));

        result
            .map(|v| v.into())
            .map_err(|v| PyValueError::new_err(format!("Could not spawn task: {v:?}")))
    }

    fn emit_event(&self, event_name: &str, payload: &[u8]) -> PyResult<()> {
        let res = self.storage.emit_event(event_name, payload);

        res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    fn create_worker(&self, worker_id: String, channels: Vec<String>) -> WorkerInner {
        WorkerInner {
            storage: self.storage.clone(),
            claim_count: self.config.worker_concurrency,
            claim_timeout_secs: self.config.worker_claim_timeout_secs,
            worker_id,
            channels,
        }
    }

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
    storage: Arc<blockingstorage::BlockingStorage>,
    channels: Vec<String>,
    worker_id: String,
    claim_count: i32,
    claim_timeout_secs: i32,
}

#[pymethods]
impl WorkerInner {
    #[getter(cleanup_interval_secs)]
    pub fn worker_sleep_secs(&self) -> i32 {
        self.storage.get_config().worker_sleep_secs
    }

    #[getter(cleanup_interval_secs)]
    pub fn worker_cleanup_interval_secs(&self) -> i32 {
        self.storage.get_config().worker_cleanup_interval_secs
    }

    /// Claim a collection tasks for timeout seconds.
    fn claim_tasks(&self) -> PyResult<Vec<ClaimedTask>> {
        let channels: Vec<&str> = self.channels.iter().map(|c| c.as_ref()).collect();
        let timeout = Duration::from_secs(self.claim_timeout_secs as u64);
        let claim_res =
            self.storage
                .claim_task(channels, &self.worker_id, timeout, self.claim_count);

        claim_res
            .map(|v| {
                let mapped: Vec<ClaimedTask> = v.into_iter().map(|task| task.into()).collect();
                mapped
            })
            .map_err(|e| PyValueError::new_err(format!("Could not claim tasks: {e:?}")))
    }

    /// Run all the cleanup operations on the database.
    fn run_cleanup(&self) -> PyResult<()> {
        let older_than =
            Duration::from_secs(self.storage.get_config().worker_cleanup_cutoff_secs as u64);

        self.storage
            .run_cleanup(older_than)
            .map_err(|e| PyValueError::new_err(format!("Could not run_cleanup: {e:?}")))
    }

    // Should cleanup be run right now by a Worker?
    // Set `config.worker_cleanup_inline` to false if you are running a dedicated
    // cleanup worker.
    fn should_run_cleanup(&self, timestamp: i64) -> bool {
        let config = self.storage.get_config();
        if !config.worker_cleanup_inline {
            return false;
        }
        let now = Utc::now().timestamp();
        let delta = now - timestamp;
        if delta < config.worker_cleanup_interval_secs as i64 {
            return false;
        }
        return true;
    }

    /// Mark a run as failed.
    fn fail_run(&self, run_id: String, retry_at: Duration) -> PyResult<()> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        self.storage
            .fail_run(run_id, b"", Some(retry_at))
            .map_err(|e| PyValueError::new_err(format!("Could not fail_run: {e:?}")))
    }

    /// Mark a run as complete.
    fn complete_run(&self, run_id: String, run_result: Vec<u8>) -> PyResult<()> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        self.storage
            .complete_run(run_id, &run_result)
            .map_err(|e| PyValueError::new_err(format!("Could not complete_run: {e:?}")))
    }

    /// Re-schedule a task to run in the future.
    fn schedule_run(&self, run_id: String, wait_for: Duration) -> PyResult<()> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        self.storage
            .schedule_run(run_id, wait_for)
            .map_err(|e| PyValueError::new_err(format!("Could not schedule_run: {e:?}")))
    }
}

/// See taskturbine.pyi for docstrings
#[pyclass]
struct ContextInner {
    storage: Arc<blockingstorage::BlockingStorage>,
    claimed_task: ClaimedTask,
}
#[pymethods]
impl ContextInner {
    #[getter(await_event_default_timeout_secs)]
    fn await_event_default_timeout_secs(&self) -> i32 {
        self.storage.get_config().await_event_default_timeout_secs
    }

    #[getter(claimed_task)]
    fn get_claimed_task(&self) -> ClaimedTask {
        self.claimed_task.clone()
    }

    fn emit_event(&self, event_name: String, payload: &[u8]) -> PyResult<()> {
        let res = self.storage.emit_event(&event_name, payload);

        res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    fn get_checkpoint(&self, checkpoint_name: String) -> PyResult<Checkpoint> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let res = self.storage.get_checkpoint(task_id, &checkpoint_name);
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

        let res =
            self.storage
                .set_checkpoint(task_id, run_id, checkpoint_name, state, extend_claim);

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
        let payload_res =
            self.storage
                .await_event(task_id, run_id, &step_name, event_name.as_ref(), timeout);
        match payload_res {
            Ok(result) => Ok(result.into()),
            Err(err) => Err(PyValueError::new_err(format!(
                "Could not await_event: {err:?}"
            ))),
        }
    }
}

/// See taskturbine.pyi for docstrings
#[pyclass]
#[derive(Debug, PartialEq, Clone)]
struct TaskOptions {
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
}
