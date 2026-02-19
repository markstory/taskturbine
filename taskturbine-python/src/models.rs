use std::time::Duration;

use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::models::{RunId, TaskId};

/// See taskturbine.pyi for docstrings
#[derive(Clone, Debug, PartialEq)]
#[pyclass]
pub struct ClaimedTask {
    #[pyo3(get)]
    pub task_id: String,

    #[pyo3(get)]
    pub run_id: String,

    #[pyo3(get)]
    pub channel: String,

    #[pyo3(get)]
    pub task_name: String,

    #[pyo3(get)]
    pub params: Vec<u8>,

    #[pyo3(get)]
    pub retry_seconds: i32,

    #[pyo3(get)]
    pub retry_factor: f32,

    #[pyo3(get)]
    pub retry_max_seconds: i32,

    #[pyo3(get)]
    pub attempt: i32,

    #[pyo3(get)]
    pub max_attempts: i32,
}

#[pymethods]
impl ClaimedTask {
    pub fn next_retry_in(&self) -> Duration {
        // Increment attempt to avoid multiply by 0
        let total_delay = self.retry_seconds as f32 * self.retry_factor.powi(self.attempt + 1);
        let capped = total_delay.min(self.retry_max_seconds as f32);

        Duration::from_secs(capped as u64)
    }
}

/// Convert from the python module to the core struct.
impl TryFrom<ClaimedTask> for taskturbine_core::models::ClaimedTask {
    type Error = String;

    fn try_from(value: ClaimedTask) -> Result<Self, Self::Error> {
        let Ok(task_id) = TryInto::<TaskId>::try_into(&value.task_id) else {
            return Err("Invalid task_id".to_string());
        };
        let Ok(run_id) = TryInto::<RunId>::try_into(&value.task_id) else {
            return Err("Invalid run_id".to_string());
        };

        Ok(taskturbine_core::models::ClaimedTask {
            task_id,
            run_id,
            channel: value.channel,
            task_name: value.task_name,
            params: value.params,
            retry_seconds: value.retry_seconds,
            retry_factor: value.retry_factor,
            retry_max_seconds: value.retry_max_seconds,
            attempt: value.attempt,
            max_attempts: value.max_attempts,
        })
    }
}

/// Convert from taskturbine_core model to the pyo3 one
impl From<taskturbine_core::models::ClaimedTask> for ClaimedTask {
    fn from(value: taskturbine_core::models::ClaimedTask) -> Self {
        ClaimedTask {
            task_id: value.task_id.0.to_string(),
            run_id: value.run_id.0.to_string(),
            channel: value.channel,
            task_name: value.task_name,
            params: value.params,
            retry_seconds: value.retry_seconds,
            retry_factor: value.retry_factor,
            retry_max_seconds: value.retry_max_seconds,
            attempt: value.attempt,
            max_attempts: value.max_attempts,
        }
    }
}

/// See taskturbine.pyi for docsstrings
#[pyclass]
#[derive(Debug, PartialEq, Clone)]
pub struct AwaitResult {
    #[pyo3(get)]
    pub payload: Vec<u8>,

    #[pyo3(get)]
    pub should_suspend: bool,
}

/// Convert from storage API to python binding
impl From<taskturbine_core::storage::AwaitResult> for AwaitResult {
    fn from(value: taskturbine_core::storage::AwaitResult) -> AwaitResult {
        let payload = value.payload;
        let should_suspend = value.should_suspend;

        AwaitResult {
            payload,
            should_suspend,
        }
    }
}

/// See taskturbine.pyi for docstrings
#[pyclass]
#[derive(Debug, PartialEq, Clone)]
pub struct SpawnResult {
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

/// See taskturbine.pyi for docstrings
#[pyclass]
pub struct Checkpoint {
    #[pyo3(get)]
    pub task_id: String,

    #[pyo3(get)]
    pub step_name: String,

    #[pyo3(get)]
    pub state: Vec<u8>,

    #[pyo3(get)]
    pub owner_run_id: String,

    #[pyo3(get)]
    pub updated_at: i64,
}

/// Convert from core API to python binding
impl From<taskturbine_core::models::Checkpoint> for Checkpoint {
    fn from(value: taskturbine_core::models::Checkpoint) -> Checkpoint {
        let task_id = value.task_id.0.into();
        let owner_run_id = value.owner_run_id.0.into();
        Checkpoint {
            task_id,
            owner_run_id,
            step_name: value.step_name.to_string(),
            state: value.state.clone(),
            updated_at: value.updated_at.timestamp(),
        }
    }
}
