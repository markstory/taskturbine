//! Async python bindings.
//!
//! These structs mirror the interface defined by AppInner, WorkerInner, ContextInner
//! but use async signatures that integrate with python's asyncio runtime.

use std::{collections::HashSet, sync::Arc, time::Duration};

use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::{models::{RunId, TaskId}, storage::Storage};

use crate::{TaskOptions, config::Config, models::{ClaimedTask, SpawnResult}};

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

        // Make a throwaway runtime to get started.
        // TODO figure out if there is a more efficient solution to this.
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

    /*
    fn create_worker(&self, worker_id: String, channels: Vec<String>) -> WorkerInner {
        WorkerInner {
            config: self.config.clone(),
            storage: self.storage.clone(),
            runtime: self.runtime.clone(),
            worker_id,
            channels,
        }
    }
    */

    fn create_context(&self, claimed_task: ClaimedTask) -> AsyncContextInner {
        AsyncContextInner {
            storage: self.storage.clone(),
            claimed_task,
        }
    }
}


/// See taskturbine.pyi for docstrings
#[pyclass(skip_from_py_object)]
struct AsyncContextInner {
    storage: Arc<Storage>,
    claimed_task: ClaimedTask,
}
#[pymethods]
impl AsyncContextInner {
    #[getter(await_event_default_timeout_secs)]
    fn await_event_default_timeout_secs(&self) -> i32 {
        self.storage.get_config().await_event_default_timeout_secs
    }

    // there is a 'fun' bug hiding here.
    #[getter(claimed_task)]
    fn get_claimed_task(&self) -> ClaimedTask {
        self.claimed_task.clone()
    }

    fn emit_event<'p>(&self, py: Python<'p>, event_name: String, payload: Vec<u8>) -> PyResult<Bound<'p, PyAny>> {
        let storage = self.storage.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let event_name = event_name.clone();
            let res = storage.emit_event(&event_name, payload.as_slice()).await;
            res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
        })
    }

    fn get_checkpoint<'p>(&self, py: Python<'p>, checkpoint_name: String) -> PyResult<Bound<'p, PyAny>> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let res = storage.get_checkpoint(task_id, &checkpoint_name).await;
            if let Ok(Some(checkpoint)) = res {
                Ok(checkpoint.into())
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
            let res = storage.set_checkpoint(
                task_id,
                run_id,
                checkpoint_name.as_ref(),
                state.as_slice(),
                extend_claim,
            ).await;

            res.map_err(|v| PyValueError::new_err(format!("Could not store checkpoint {v:?}")))
        })
    }

    fn get_event_payload<'p>(&self, py: Python<'p>, event_name: String, timeout: Duration) -> PyResult<Bound<'p, PyAny>> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&self.claimed_task.task_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let Ok(run_id) = TryInto::<RunId>::try_into(&self.claimed_task.run_id) else {
            return Err(PyValueError::new_err("Invalid uuid".to_string()));
        };
        let storage = self.storage.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let step_name = format!("$awaitEvent:{event_name}");
            let payload_res = storage.await_event(
                task_id,
                run_id,
                &step_name,
                event_name.as_ref(),
                Some(timeout),
            ).await;
            match payload_res {
                // TODO need another Into implementation
                Ok(result) => Ok(result.into()),
                Err(err) => Err(PyValueError::new_err(format!(
                    "Could not await_event: {err:?}"
                ))),
            }
        })
    }
}

