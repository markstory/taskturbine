/// Common datastructures and models for taskturbine.
use chrono::{DateTime, Utc};
use std::{
    collections::{HashMap, HashSet}, fmt::{Display, Formatter}, str::FromStr, sync::Mutex, time::Duration
};
use uuid::Uuid;

/// The states that a task/run can be in.
#[derive(Clone, Copy, Debug, PartialEq, sqlx::Type)]
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

/// Used by CLI for parsing from string.
/// Db conversions are handled with `sqlx` attribute macro
impl FromStr for TaskState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let enum_val = match s.to_lowercase().as_ref() {
            "pending" => TaskState::Pending,
            "running" => TaskState::Running,
            "sleeping" => TaskState::Sleeping,
            "completed" => TaskState::Completed,
            "failed" => TaskState::Failed,
            "cancelled" => TaskState::Cancelled,
            &_ => return Err(format!("Invalid value `{s}` for TaskState")),
        };
        Ok(enum_val)
    }
}

impl Display for TaskState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_val = match self {
            TaskState::Pending => "pending",
            TaskState::Running => "running",
            TaskState::Sleeping => "sleeping",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Cancelled => "cancelled",
        };
        f.write_str(str_val)
    }
}

/// Marker type for Task identifiers. Bare UUIDs are easy to confuse.
#[derive(sqlx::Decode, sqlx::Encode, Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
impl TryFrom<String> for TaskId {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let Ok(uuid) = Uuid::parse_str(&value) else {
            return Err(());
        };
        Ok(Self(uuid))
    }
}
impl TryFrom<&String> for TaskId {
    type Error = ();

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        let Ok(uuid) = Uuid::parse_str(value) else {
            return Err(());
        };
        Ok(Self(uuid))
    }
}
impl From<Uuid> for TaskId {
    fn from(value: Uuid) -> Self {
        Self(value)
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
impl TryFrom<String> for RunId {
    type Error = ();

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let Ok(uuid) = Uuid::parse_str(&value) else {
            return Err(());
        };
        Ok(Self(uuid))
    }
}
impl TryFrom<&String> for RunId {
    type Error = ();

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        let Ok(uuid) = Uuid::parse_str(value) else {
            return Err(());
        };
        Ok(Self(uuid))
    }
}
impl From<Uuid> for RunId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

/// Entity structure for a task
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct Task {
    /// The task id of the spawned task.
    pub task_id: TaskId,
    /// The application/usecase the task belongs to.
    pub usecase: String,
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
    /// Calculate the delay until the next attempt should be made
    /// based on retry attributes.
    pub fn next_retry_in(&self) -> Duration {
        let total_delay = self.retry_seconds as f32 * self.retry_factor.powi(self.attempts);
        let capped = total_delay.min(self.retry_max_seconds as f32);
        Duration::from_secs(capped as u64)
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

impl ClaimedTask {
    /// Calculate the delay until the next attempt should be made
    /// based on retry attributes.
    pub fn next_retry_in(&self) -> Duration {
        // Increment to avoid multiply by 0
        let total_delay = self.retry_seconds as f32 * self.retry_factor.powi(self.attempt + 1);
        let capped = total_delay.min(self.retry_max_seconds as f32);

        Duration::from_secs(capped as u64)
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
pub struct Run {
    pub task_id: TaskId,
    pub run_id: RunId,
    /// The attempt index this Run is.
    pub attempt: i32,
    /// The current state of the task.
    pub state: TaskState,
    /// The timestamp that the current claim expires at.
    pub claimed_by: Option<String>,
    /// The timestamp that the current claim expires at.
    /// Once a claim expires, the cleanup operations will
    /// make the task available again.
    pub claim_expires_at: Option<DateTime<Utc>>,
    /// The timestamp the run can be claimed next.
    pub available_at: Option<DateTime<Utc>>,
    /// The timestamp the run started execution if available.
    pub started_at: Option<DateTime<Utc>>,
    /// The timestamp the run completed if defined.
    pub completed_at: Option<DateTime<Utc>>,
    /// The result payload of the task in bytes. Generally utf8 encoded.
    pub result: Option<Vec<u8>>,
    /// Reason bytes for why run failed. Generally utf8 encoded.
    pub failure_reason: Option<Vec<u8>>,
    /// Timestamp the run was created
    pub created_at: DateTime<Utc>,
}

/// Entity structure for a task checkpoint
#[derive(sqlx::FromRow, Clone, Debug, PartialEq)]
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

/// Provides in memory storage of steps -> checkpoint names
/// It is possible for userland code to repeat step names
/// (like in a loop). We need to handle tracking separate
/// completion states for each iteration.
///
/// This structure is meant to be an ephemeral cache that is
/// intended to be written to *after* writing to `Storage`.
#[derive(Default)]
pub struct Checkpoints {
    counters: Mutex<HashMap<String, u32>>,
    loaded: HashSet<TaskId>,
    checkpoint_data: HashMap<(TaskId, String), Checkpoint>,
}

impl Checkpoints {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(HashMap::new()),
            loaded: HashSet::new(),
            checkpoint_data: HashMap::new(),
        }
    }

    /// Generate a checkpoint name that includes the name and counter value.
    fn format_name(&self, name: &str, count: &u32) -> String {
        let suffix = if *count == 1 {
            "".to_string()
        } else {
            format!("#{count}")
        };

        format!("{name}{suffix}")
    }

    /// Get the current counter value for a checkpoint name.
    /// Will return None on checkpoints that aren't known yet.
    fn get_counter(&self, name: &str) -> Option<u32> {
        // TODO can this allocation be removed?
        let counter = self.counters.lock().expect("get lock failed");
        counter.get(name).cloned()
    }

    /// Get the latest checkpoint name for a step
    ///
    /// Each time a checkpoint name is created for a step, a counter
    /// is incremented. This method will read the state of only
    /// the latest generated checkpoint name. There is the possibility
    /// that the checkpoint has no state yet.
    pub fn get_latest_name(&self, step_name: &str) -> Option<String> {
        let counter = self.get_counter(step_name)?;
        Some(self.format_name(step_name, &counter))
    }

    /// Generate a unique checkpoint name from a step name.
    /// Handles the scenario where userland code has multiple
    /// steps with the same name.
    pub fn generate_name(&self, name: &str) -> String {
        let mut counters = self.counters.lock().expect("generate_name lock failed");
        if !counters.contains_key(name) {
            counters.insert(name.to_string(), 0);
        }
        if let Some(value) = counters.get_mut(name) {
            *value += 1;
        }
        let count = if let Some(value) = counters.get(name) {
            *value
        } else {
            0
        };
        self.format_name(name, &count)
    }

    /// Check if a task has had its checkpoints loaded yet.
    pub fn is_loaded(&self, task_id: &TaskId) -> bool {
        self.loaded.contains(task_id)
    }

    /// Store a collection of Checkpoints for a task.
    pub fn store(&mut self, task_id: TaskId, checkpoints: Vec<Checkpoint>) {
        for checkpoint in checkpoints.into_iter() {
            self.checkpoint_data
                .insert((task_id, checkpoint.step_name.to_owned()), checkpoint);
        }
    }

    /// Get a single checkpoint from the loaded checkpoint data.
    /// TODO cleanup one param as a ref, and one as owned is madness.
    pub fn get(&self, task_id: TaskId, checkpoint_name: &str) -> Option<Checkpoint> {
        // TODO rework the key of this map so that lookups can be done without allocations.
        let key = (task_id, checkpoint_name.to_owned());
        self.checkpoint_data.get(&key).cloned()
    }

    /// Add a checkpoint to the cache.
    ///
    /// It is assumed that the checkpoint has already been stored.
    pub fn add(&mut self, task_id: TaskId, checkpoint: Checkpoint) {
        let key = (task_id, checkpoint.step_name.to_owned());
        self.checkpoint_data.insert(key, checkpoint);
    }
}

/// An Event payload
///
/// Events are captured with `emit_event` and tasks can register
/// to wait for events with `await_event`. Events enable you
/// to synchronize task execution with the completion of work
/// in other systems. For example, a webhook need to be received.
#[derive(Debug)]
pub struct Event {
    pub event_name: String,
    pub payload: Vec<u8>,
}

/// Entity structure for a scheduler entry state
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct SchedulerState {
    pub schedule_id: String,
    pub last_run: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use crate::models::{RunId, TaskId};

    #[test]
    fn task_id_from_string() {
        let res: Result<TaskId, ()> = "bad-value".to_string().try_into();
        assert!(res.is_err());

        let uuid = Uuid::now_v7();
        let uuid_string = uuid.to_string();
        let res: Result<TaskId, ()> = (&uuid_string).try_into();
        assert!(res.is_ok());

        let uuid_string = uuid.to_string();
        let res: Result<TaskId, ()> = uuid_string.try_into();
        assert!(res.is_ok());
        let task_id = res.unwrap();
        assert_eq!(
            task_id.0.to_string(),
            uuid.to_string(),
            "string values should be the same"
        );
    }

    #[test]
    fn run_id_from_string() {
        let res: Result<RunId, ()> = "bad-value".to_string().try_into();
        assert!(res.is_err());

        let uuid = Uuid::now_v7();
        let uuid_string = uuid.to_string();
        let res: Result<RunId, ()> = (&uuid_string).try_into();
        assert!(res.is_ok());

        let res: Result<RunId, ()> = uuid_string.try_into();
        assert!(res.is_ok());
        let run_id = res.unwrap();
        assert_eq!(
            run_id.0.to_string(),
            uuid.to_string(),
            "string values should be the same"
        );
    }
}
