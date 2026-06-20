//! Async python bindings.
//!
//! These structs mirror the interface defined by AppInner, WorkerInner, ContextInner
//! but use async signatures that integrate with python's asyncio runtime.

use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::Utc;
use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::{
    models::{RunId, TaskId},
    storage::{Storage},
};

use crate::{
    TaskOptions,
    config::Config,
    models::{AwaitResult, Checkpoint, ClaimedTask, SpawnResult, UpkeepMetric},
};

#[pyclass(skip_from_py_object)]
pub struct AsyncAppInner {
    #[pyo3(get)]
    config: Config,

    /// The set of channels that have been defined.
    #[pyo3(get)]
    channels: HashSet<String>,

    /// A blocking wrapper on taskturbine_core::storage::Storage
    storage: Arc<Storage>,
}

#[pymethods]
impl AsyncAppInner {
    #[new]
    fn py_new(config: Config) -> Self {
        let mut channels = HashSet::new();
        channels.insert(config.default_channel.clone());

        // Make a throwaway runtime to build Storage in.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let storage = runtime.block_on(async { Storage::new(config.clone().into()) });

        AsyncAppInner {
            config,
            channels,
            storage: Arc::new(storage),
        }
    }

    fn add_channel(&mut self, value: String) {
        self.channels.insert(value);
    }

    fn set_channels(&mut self, names: Vec<String>) {
        self.channels.clear();
        for name in names.iter() {
            self.channels.insert(name.clone());
        }
    }

    fn spawn_task<'p>(
        &self,
        py: Python<'p>,
        task_name: String,
        params: Vec<u8>,
        options: TaskOptions,
    ) -> PyResult<Bound<'p, PyAny>> {
        self.channel_spawn_task(
            py,
            self.config.default_channel.clone(),
            task_name,
            params,
            options,
        )
    }

    fn channel_spawn_task<'p>(
        &self,
        py: Python<'p>,
        channel: String,
        task_name: String,
        params: Vec<u8>,
        options: TaskOptions,
    ) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py::<_, SpawnResult>(py, async move {
            let spawn_result = storage
                .spawn_task(
                    &channel,
                    &task_name,
                    params.as_slice(),
                    Some(options.into()),
                )
                .await;
            spawn_result
                .map(Into::<SpawnResult>::into)
                .map_err(|e| PyValueError::new_err(format!("Could not spawn task {e:?}")))
        })
    }

    fn emit_event<'p>(
        &self,
        py: Python<'p>,
        event_name: String,
        payload: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage.emit_event(&event_name, payload.as_slice()).await;
            result.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
        })
    }

    fn update_schema<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let result = storage.update_schema().await;
            result.map_err(|v| PyValueError::new_err(format!("Could not update_schema: {v:?}")))
        })
    }

    fn create_worker(&self, worker_id: String, channels: Vec<String>) -> AsyncWorkerInner {
        AsyncWorkerInner {
            config: self.config.clone(),
            storage: self.storage.clone(),
            worker_id,
            channels,
        }
    }

    fn create_context(&self, claimed_task: ClaimedTask) -> AsyncContextInner {
        AsyncContextInner {
            storage: self.storage.clone(),
            claimed_task,
        }
    }
}

/// See taskturbine.pyi for docstrings
#[pyclass(skip_from_py_object)]
pub struct AsyncContextInner {
    storage: Arc<Storage>,
    claimed_task: ClaimedTask,
}
#[pymethods]
impl AsyncContextInner {
    #[getter(await_event_default_timeout_secs)]
    fn await_event_default_timeout_secs(&self) -> i32 {
        self.storage.get_config().await_event_default_timeout_secs
    }

    #[getter(claimed_task)]
    fn get_claimed_task(&self) -> ClaimedTask {
        self.claimed_task.clone()
    }

    fn emit_event<'p>(
        &self,
        py: Python<'p>,
        event_name: String,
        payload: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event_name = event_name.clone();
            let res = storage.emit_event(&event_name, payload.as_slice()).await;
            res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
        })
    }

    fn get_checkpoint<'p>(
        &self,
        py: Python<'p>,
        checkpoint_name: String,
    ) -> PyResult<Bound<'p, PyAny>> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = storage.get_checkpoint(task_id, &checkpoint_name).await;
            if let Ok(Some(checkpoint)) = res {
                Ok(Into::<Checkpoint>::into(checkpoint))
            } else {
                Err(PyValueError::new_err(
                    "Checkpoint not found, or read failed",
                ))
            }
        })
    }

    fn set_checkpoint<'p>(
        &self,
        py: Python<'p>,
        checkpoint_name: String,
        state: Vec<u8>,
        extend_claim: Option<Duration>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let Ok(run_id) = TryInto::<RunId>::try_into(&self.claimed_task.run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = storage
                .set_checkpoint(
                    task_id,
                    run_id,
                    checkpoint_name.as_ref(),
                    state.as_slice(),
                    extend_claim,
                )
                .await;

            res.map_err(|v| PyValueError::new_err(format!("Could not store checkpoint {v:?}")))
        })
    }

    fn get_event_payload<'p>(
        &self,
        py: Python<'p>,
        event_name: String,
        timeout: Duration,
    ) -> PyResult<Bound<'p, PyAny>> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let Ok(run_id) = TryInto::<RunId>::try_into(&self.claimed_task.run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let step_name = format!("$awaitEvent:{event_name}");
            let payload_res = storage
                .await_event(
                    task_id,
                    run_id,
                    &step_name,
                    event_name.as_ref(),
                    Some(timeout),
                )
                .await;
            match payload_res {
                Ok(result) => Ok(Into::<AwaitResult>::into(result)),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Could not await_event: {err:?}"
                ))),
            }
        })
    }
}

/// Expose the minimal worker API to be used by the python worker.
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct AsyncWorkerInner {
    config: Config,
    storage: Arc<Storage>,
    channels: Vec<String>,
    worker_id: String,
}

#[pymethods]
impl AsyncWorkerInner {
    #[getter(app_module)]
    pub fn app_module(&self) -> String {
        self.config.app_module.clone()
    }

    #[getter(usecase)]
    pub fn usecase(&self) -> String {
        self.config.usecase.clone()
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
    fn claim_tasks<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let channels: Vec<String> = self.channels.to_vec();
        let timeout = Duration::from_secs(self.config.worker_claim_timeout_secs as u64);
        let storage = self.storage.clone();
        let worker_id = self.worker_id.clone();
        let limit = self.config.worker_concurrency;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let channels = channels.iter().map(|c| c.as_ref()).collect();
            let claim_res = storage
                .claim_task(channels, &worker_id, timeout, limit)
                .await;

            claim_res
                .map(|v| {
                    let mapped: Vec<ClaimedTask> = v.into_iter().map(|task| task.into()).collect();
                    mapped
                })
                .map_err(|e| PyValueError::new_err(format!("Could not claim tasks: {e:?}")))
        })
    }

    /// Run all the upkeep operations on the database.
    fn run_upkeep<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .run_upkeep()
                .await
                .map_err(|e| PyValueError::new_err(format!("Upkeep failed: {e:?}")))
        })
    }

    /// Collect metrics on the state of the usecase data.
    /// Generally called during upkeep
    fn upkeep_metrics<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let metrics = storage.upkeep_metrics().await;
            Ok(metrics.into_iter().map(|metric| metric.into()).collect::<Vec<UpkeepMetric>>())
        })
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

    /// Mark a run as failed.
    fn fail_run<'p>(
        &self,
        py: Python<'p>,
        run_id: String,
        reason: Option<Vec<u8>>,
        retry_at: Option<Duration>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();
        let reason = reason.unwrap_or(vec![]);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .fail_run(run_id, &reason, retry_at)
                .await
                .map_err(|e| PyValueError::new_err(format!("Could not fail_run: {e:?}")))
        })
    }

    /// Mark a run as complete.
    fn complete_run<'p>(
        &self,
        py: Python<'p>,
        run_id: String,
        run_result: Vec<u8>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .complete_run(run_id, &run_result)
                .await
                .map_err(|e| PyValueError::new_err(format!("Could not complete_run: {e:?}")))
        })
    }

    /// Re-schedule a task to run in the future.
    fn schedule_run<'p>(
        &self,
        py: Python<'p>,
        run_id: String,
        wait_for: Duration,
    ) -> PyResult<Bound<'p, PyAny>> {
        let Ok(run_id) = TryInto::<RunId>::try_into(run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            storage
                .schedule_run(run_id, wait_for)
                .await
                .map_err(|e| PyValueError::new_err(format!("Could not schedule_run: {e:?}")))
        })
    }
}
