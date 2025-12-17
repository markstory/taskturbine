/// Common datastructures and models for taskturbine.
use chrono::{DateTime, Utc};
use std::{
    fmt::{Display, Formatter},
    time::Duration,
};
use uuid::Uuid;

/// The states that a task/run can be in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum TaskState {
    /// The task is ready for execution, and waiting for a worker to claim it.
    Pending,
    /// The task has been claimed by a worker.
    Running,
    /// The task isn't waiting for a future time to elapse, or event to happen.
    Sleeping,
    /// The task has been executed successfully.
    Completed,
    /// The task was not executed successfully.
    Failed,
    /// The task was not cancelled due to max age.
    Cancelled,
}

/// Marker type for Task identifiers. Bare UUIDs are easy to confuse.
#[derive(sqlx::Decode, sqlx::Encode, Clone, Copy, Debug, PartialEq)]
pub struct TaskId(pub Uuid);

impl sqlx::Type<sqlx::Postgres> for TaskId {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Marker type for Run identifiers. Bare UUIDs are easy to confuse.
#[derive(sqlx::Decode, sqlx::Encode, Clone, Copy, Debug, PartialEq)]
pub struct RunId(pub Uuid);

impl sqlx::Type<sqlx::Postgres> for RunId {
    fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

impl Display for RunId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Entity structure for a task
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct Task {
    /// The task id of the spawned task.
    pub task_id: TaskId,
    /// The channel the task belongs to.
    pub channel: String,
    /// The name of the task that was claimed.
    pub task_name: String,
    /// The parameters of the task in bytes.
    pub params: Vec<u8>,
    /// The headers of the task in bytes. Will generally contain JSON encoded metadata.
    pub headers: Vec<u8>,
    /// The number of seconds betwen retries.
    pub retry_seconds: i32,
    /// The factor to multiple retries by attempt count.
    pub retry_factor: f32,
    /// The maximum number of seconds to wait between retries.
    pub retry_max_seconds: i32,
    /// The current attempt count.
    pub attempts: i32,
    /// The maximum number of attempts allowed.
    pub max_attempts: i32,
    /// The timestamp the task was completed at if applicable.
    pub completed_at: Option<DateTime<Utc>>,
    /// The maximum age in seconds before the task should be cancelled.
    pub cancellation_max_age: i32,
    /// The timestamp the task was created at.
    pub created_at: DateTime<Utc>,
    /// The current state of the task.
    pub state: TaskState,
    /// The run id of the last attempt if applicable.
    pub last_attempt_run: Option<RunId>,
}

impl Task {
    /// Calculate the next retry based on retry attributes.
    pub fn next_retry_at(&self) -> DateTime<Utc> {
        let now = Utc::now();
        let total_delay = self.retry_seconds as f32 * self.retry_factor.powi(self.attempts);
        let capped = total_delay.min(self.retry_max_seconds as f32);
        now + Duration::from_secs(capped as u64)
    }
}

/// Entity structure for a task that has been claimed
/// by a worker for execution. This is a snapshot of the state
/// from when the claim was made.
#[derive(sqlx::FromRow, Clone, Debug, PartialEq)]
pub struct ClaimedTask {
    /// The task id of the spawned task.
    pub task_id: TaskId,
    /// The run id of the spawned run.
    pub run_id: RunId,
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

impl ClaimedTask {
    /// Calculate the next retry based on retry attributes.
    pub fn next_retry_at(&self) -> DateTime<Utc> {
        let now = Utc::now();
        // Increment to avoid
        let total_delay = self.retry_seconds as f32 * self.retry_factor.powi(self.attempt + 1);
        let capped = total_delay.min(self.retry_max_seconds as f32);
        now + Duration::from_secs(capped as u64)
    }
}

/// Result of spawning a task.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpawnResult {
    /// The task id of the spawned task.
    pub task_id: TaskId,
    /// The run id of the initial run spawned for the task.
    /// The run will begin as pending.
    pub run_id: RunId,
}

/// Entity structure for a task checkpoint
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct Checkpoint {
    /// The task id of the spawned task.
    pub task_id: TaskId,
    /// The step name of the checkpoint. Step names are made
    /// unique per task to handle duplicate step names.
    pub step_name: String,
    /// The payload/state of the checkpoint in bytes.
    /// Applications are responsible for serializing/deserializing
    pub state: Vec<u8>,
    /// The run that created this checkpoint.
    pub owner_run_id: RunId,
    /// The timestamp the checkpoint was created or updated.
    pub updated_at: DateTime<Utc>,
}
