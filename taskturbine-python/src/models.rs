use pyo3::{exceptions::PyValueError, prelude::*};
use taskturbine_core::models::{RunId, TaskId};
use uuid::Uuid;

/// Entity structure for a task that has been claimed
/// by a worker for execution. This is a snapshot of the state
/// from when the claim was made.
#[derive(Clone, Debug, PartialEq)]
#[pyclass]
pub struct ClaimedTask {
    /// The task id of the spawned task.
    pub task_id: String,
    /// The run id of the spawned run.
    pub run_id: String,
    /// The channel name the task was spawned in.
    pub channel: String,
    /// The name of the task that was claimed.
    pub task_name: String,
    /// The parameters of the task in bytes.
    pub params: Vec<u8>,
    /// The number of seconds betwen retries.
    pub retry_seconds: i32,
    /// The factor to multiple retries by attempt count.
    pub retry_factor: f32,
    /// The maximum number of seconds to wait between retries.
    pub retry_max_seconds: i32,
    /// The current attempt count.
    pub attempt: i32,
    /// The maximum number of attempts allowed.
    pub max_attempts: i32,
}

/// Convert from the python module to the core struct.
/// TODO convert to try_from
impl From<ClaimedTask> for taskturbine_core::models::ClaimedTask {
    fn from(value: ClaimedTask) -> Self {
        // TODO: This is a bit YOLO
        let task_id = Uuid::parse_str(&value.task_id).unwrap();
        let run_id = Uuid::parse_str(&value.run_id).unwrap();

        taskturbine_core::models::ClaimedTask {
            task_id: TaskId(task_id),
            run_id: RunId(run_id),
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
pub struct Task {
    /// The python module name of the task. This module is expected to be within
    /// `[Config.app_module]`. This module will be imported when running the task.
    #[pyo3(get, set)]
    pub module_name: String,

    /// The unique name of the task. Tasks having unique names helps ease refactoring
    /// operations as module names are not persisted in task records.
    #[pyo3(get, set)]
    pub task_name: String,
}

/// The metadata for a task.
///
/// This is shared data to/from python.
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

/// The metadata for the result of await_event
///
/// This is shared data to/from python.
#[pyclass]
#[derive(Debug, PartialEq, Clone)]
pub struct AwaitResult {
    /// The event payload that was awaited upon.
    /// Application logic is responsible for decoding bytes.
    pub payload: Vec<u8>,

    /// Whether or not the runtime should suspend as we're still waiting for the event.
    pub should_suspend: bool,
}

/// Convert from storage API to python binding
impl From<taskturbine_core::storage::AwaitResult> for AwaitResult {
    fn from(value: taskturbine_core::storage::AwaitResult) -> AwaitResult {
        let payload = value.payload;
        let should_suspend = value.should_suspend;

        AwaitResult { payload, should_suspend }
    }
}

/// The result of spawning a task.
///
/// This is shared data to/from python.
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
