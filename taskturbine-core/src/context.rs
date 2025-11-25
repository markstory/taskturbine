use std::{collections::HashMap, str::Bytes, time::Duration};

use chrono::Utc;

use crate::{api::Storage, models::ClaimedTask};

/// Used as signaling 'errors' to the worker runtime
/// from userland operations. For example, when a task
/// needs to be suspended because it is waiting on an event.
enum FlowControl {
    InvalidValue(String),
    Failure(String),
    Suspend(Duration),
}

pub struct Event<'a> {
    pub event_name: String,
    pub payload: Bytes<'a>,
}

struct Checkpoints {
    counters: HashMap<String, u32>,
}
impl Checkpoints {
    pub fn new() -> Self {
        Self { counters: HashMap::new() }
    }

    /// Incrment the counter for a given name and get the new value.
    pub fn incr(&self, name: &str) -> u32 {
        let updated = match self.counters.get_mut(name) {
            Some(value) => *value = *value + 1 as u32,
            None => 1,
        }

        updated
    }
}

/// Execution context for a task.
/// Passed to task functions by the Worker runtime.
///
/// Context contains the Step interface, as well as the await_event()
/// interface method.
pub struct TaskContext {
    task: ClaimedTask,
    storage: Storage,
    checkpoints: Checkpoints,
}

impl TaskContext {
    pub fn build(task: ClaimedTask, storage: Storage) -> Self {
        let checkpoints = Checkpoints::new();

        Self { task, storage, checkpoints}
    }

    fn checkpoint_name(&self, name: &str) -> String {
        let count = self.checkpoints.incr(name);
        let suffix = if count == 1 {
            "".to_string()
        } else {
            format!("#{count}")
        };

        return format!({name}{suffix});
    }

    /// Define a new step with a name
    /// When steps complete, they create checkpoints of the
    /// step results which enables re-runs of the task to durably
    /// resume from their last checkpoint.
    pub async fn step<T>(&self, name: &str, step_fn: impl FnOnce(T) -> ()) -> String {
        // TODO Need to capture more about the types here.
        "result".to_string()
    }

    /// Await for an event to be captured by emit_event.
    /// When the event has not happened, the Result will be an Err
    /// that indicates that the worker should sleep. You almost
    /// always want to call this with `?`
    pub async fn await_event(&self, event_name: &str, timeout: Option<Duration>) -> Result<Event, FlowControl> {
        // TODO Use config?
        let wait_for = timeout.unwrap_or_else(|| Duration::from_secs(60));
        let wake_at = Utc::now() + wait_for;

        let step_name = format!("$awaitEvent:{event_name}");

        let res = self.storage.await_event(
            self.task.task_id,
            self.task.run_id, 
            step_name,
            event_name,
            wake_at).await;
        if let Ok(wait) = res {
            if wait.should_suspend {
                return Err(FlowControl::Suspend(wait_for))
            }
        }

        let err = res.err().unwrap();
        return Err(FlowControl::Failure(format!("Could not store an event wait: {err:?}")))
    }

    /// Suspend the current task until the provided duration has elapsed.
    /// You almost always want to call this with `?`
    pub async fn sleep_for(&self, duration: Duration) -> Result<(), FlowControl> {
        Ok(())
    }
}
