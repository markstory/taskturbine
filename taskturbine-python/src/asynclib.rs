//! Async python bindings.
//!
//! These structs mirror the interface defined by AppInner, WorkerInner, ContextInner
//! but use async signatures that integrate with python's asyncio runtime.

use std::{collections::HashSet, sync::Arc};

use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::storage::Storage;

use crate::{config::Config, models::SpawnResult, TaskOptions};

#[pyclass]
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

    async fn spawn_task(
        &self,
        task_name: String,
        params: Vec<u8>,
        options: TaskOptions,
    ) -> PyResult<SpawnResult> {
        self.channel_spawn_task(self.config.default_channel.clone(), task_name, params, options).await
    }

    async fn channel_spawn_task(
        &self,
        channel: String,
        task_name: String,
        params: Vec<u8>,
        options: TaskOptions,
    ) -> PyResult<SpawnResult> {
        let storage = self.storage.clone();
        let result = self.runtime.spawn(async move {
            storage.spawn_task(
                &channel,
                &task_name,
                params.as_slice(),
                Some(options.into()),
            ).await
        }).await;

        let Ok(spawn_task_res) = result else {
            let e = result.err().unwrap();
            return Err(PyValueError::new_err(format!("Could not spawn task {e:?}")));
        };

        spawn_task_res
            .map(|v| v.into())
            .map_err(|e| PyValueError::new_err(format!("Could not spawn task {e:?}")))
    }

    async fn emit_event(&self, event_name: String, payload: Vec<u8>) -> PyResult<()> {
        let storage = self.storage.clone();
        let result = self.runtime.spawn(async move {
            storage.emit_event(&event_name, payload.as_slice()).await
        }).await;
        let Ok(emit_res) = result else {
            let e = result.err().unwrap();
            return Err(PyValueError::new_err(format!("Could not store event {e:?}")));
        };

        emit_res.map_err(|v| PyValueError::new_err(format!("Could not store event: {v:?}")))
    }

    async fn update_schema(&self) -> PyResult<()> {
        let storage = self.storage.clone();
        let result = self.runtime.spawn(async move {
            storage.update_schema().await
        }).await;
        let Ok(update_res) = result else {
            let e = result.err().unwrap();
            return Err(PyValueError::new_err(format!("Could not store event {e:?}")));
        };

        update_res.map_err(|v| PyValueError::new_err(format!("Could not update_schema: {v:?}")))
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
