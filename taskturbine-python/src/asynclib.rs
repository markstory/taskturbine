//! Async python bindings.
//!
//! These structs mirror the interface defined by AppInner, WorkerInner, ContextInner
//! but use async signatures that integrate with python's asyncio runtime.

use std::{collections::HashSet, sync::Arc};

use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::storage::Storage;

use crate::{TaskOptions, config::Config, models::SpawnResult};

#[pyclass(skip_from_py_object)]
pub struct AsyncAppInner {
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
impl AsyncAppInner {
    #[new]
    fn py_new(config: Config) -> Self {
        let mut channels = HashSet::new();
        channels.insert(config.default_channel.clone());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let storage = runtime.block_on(async { Storage::new(config.clone().into()) });

        AsyncAppInner {
            config,
            channels,
            storage: Arc::new(storage),
            runtime: Arc::new(runtime),
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

    fn emit_event<'p>(&self, py: Python<'p>, event_name: String, payload: Vec<u8>) -> PyResult<Bound<'p, PyAny>> {
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

    fn create_context(&self, claimed_task: ClaimedTask) -> ContextInner {
        ContextInner {
            storage: self.storage.clone(),
            runtime: self.runtime.clone(),
            claimed_task,
        }
    }
    */
}
