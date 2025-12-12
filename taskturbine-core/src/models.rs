use chrono::{DateTime, Utc};
use std::{fmt::{Display, Formatter}, time::Duration};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum TaskState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
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
    pub task_id: TaskId,
    pub namespace: String,
    pub task_name: String,
    pub params: Vec<u8>,
    pub headers: Vec<u8>,
    pub retry_seconds: i32,
    pub retry_factor: f32,
    pub retry_max_seconds: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub completed_at: Option<DateTime<Utc>>,
    pub cancellation_max_age: i32,
    pub created_at: DateTime<Utc>,
    pub state: TaskState,
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

#[derive(sqlx::FromRow, Clone, Debug, PartialEq)]
pub struct ClaimedTask {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub task_name: String,
    pub params: Vec<u8>,
    pub retry_seconds: i32,
    pub retry_factor: f32,
    pub retry_max_seconds: i32,
    pub attempt: i32,
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

/// Entity structure for a task checkpoint
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct Checkpoint {
    pub task_id: TaskId,
    pub step_name: String,
    pub state: Vec<u8>,
    pub owner_run_id: RunId,
    pub updated_at: DateTime<Utc>,
}
