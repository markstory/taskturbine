use chrono::Utc;
use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{api::Storage, models::ClaimedTask};

/// Used as signaling 'errors' to the worker runtime
/// from userland operations. For example, when a task
/// needs to be suspended because it is waiting on an event.
#[derive(Debug)]
pub enum FlowControl {
    InvalidValue(String),
    Failure(String),
    Suspend(Duration),
}

pub struct Event {
    pub event_name: String,
    pub payload: Vec<u8>,
}

struct Checkpoints {
    counters: HashMap<String, u32>,
}
impl Checkpoints {
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Incrment the counter for a given name and get the new value.
    pub fn incr(&mut self, name: &str) -> u32 {
        if !self.counters.contains_key(name) {
            self.counters.insert(name.to_string(), 0);
        }
        if let Some(value) = self.counters.get_mut(name) {
            *value += 1;
        }
        if let Some(value) = self.counters.get(name) {
            return value.clone();
        } else {
            0
        }
    }
}

/// Execution context for a task.
/// Passed to task functions by the Worker runtime.
///
/// Context contains the Step interface, as well as the await_event()
/// interface method.
pub struct TaskContext {
    task: ClaimedTask,
    storage: Arc<Storage>,
    checkpoints: Checkpoints,
}

impl TaskContext {
    pub fn build(task: ClaimedTask, storage: Arc<Storage>) -> Self {
        let checkpoints = Checkpoints::new();

        Self {
            task,
            storage,
            checkpoints,
        }
    }

    /// Convert a step name into a unique checkpoint name.
    /// Handles the scenario where userland code has multiple
    /// steps with the same name.
    fn checkpoint_name(&mut self, name: &str) -> String {
        let count = self.checkpoints.incr(name);
        let suffix = if count == 1 {
            "".to_string()
        } else {
            format!("#{count}")
        };

        return format!("{name}{suffix}");
    }

    /// Define a new step with a name
    /// When steps complete, they create checkpoints of the
    /// step results which enables re-runs of the task to durably
    /// resume from their last checkpoint.
    pub async fn step<E>(
        &mut self,
        name: &str,
        step_fn: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, FlowControl> {
        // See if the step has a completed checkpoint
        let checkpoint_name = self.checkpoint_name(name);
        let res = self
            .storage
            .get_checkpoint(self.task.task_id, &checkpoint_name)
            .await;
        if let Err(err) = res {
            return Err(FlowControl::Failure(format!(
                "Failed to read checkpoint {err:?}"
            )));
        }
        let checkpoint_opt = res.unwrap();
        if let Some(checkpoint) = checkpoint_opt {
            return Ok(checkpoint.state);
        }
        let res = step_fn();
        if let Ok(state) = res {
            let res = self
                .storage
                .set_checkpoint(
                    self.task.task_id,
                    self.task.run_id,
                    &checkpoint_name,
                    state.as_slice(),
                    None,
                )
                .await;
            if let Err(err) = res {
                return Err(FlowControl::Failure(format!(
                    "Could not store checkpoint {err:?}"
                )));
            }

            return Ok(state)
        }
        return Err(FlowControl::Failure("Task execution failed".to_string()));
    }

    /// Record an event as having completed.
    /// Events allow you to synchronize tasks with external actions
    /// that can be recorded as events. Events can have a Payload of bytes.
    /// How those bytes are encoded is an application concern.
    pub async fn emit_event(&self, event_name: &str, payload: &[u8]) -> Result<(), FlowControl> {
        let res = self.storage.emit_event(event_name, payload).await;

        if let Err(err) = res {
            return Err(FlowControl::Failure(format!(
                "Could not store event {err:?}"
            )));
        }
        Ok(())
    }

    /// Await for an event to be captured by emit_event.
    /// When the event has not happened, the Result will be an Err
    /// that indicates that the worker should sleep. You almost
    /// always want to call this with `?`
    pub async fn await_event(
        &self,
        event_name: &str,
        timeout: Option<Duration>,
    ) -> Result<Event, FlowControl> {
        // TODO Use config?
        let wait_for = timeout.unwrap_or_else(|| Duration::from_secs(60));
        let step_name = format!("$awaitEvent:{event_name}");

        let res = self
            .storage
            .await_event(
                self.task.task_id,
                self.task.run_id,
                &step_name,
                event_name,
                Some(wait_for.as_secs()),
            )
            .await;

        if let Ok(wait) = &res {
            if wait.should_suspend {
                return Err(FlowControl::Suspend(wait_for));
            }
            let event = Event {
                event_name: event_name.to_string(),
                payload: wait.payload.to_vec(),
            };
            return Ok(event);
        }

        // TODO use thiserror to make error casting more succinct
        let err = res.err().unwrap();
        return Err(FlowControl::Failure(format!(
            "Could not store an event wait: {err:?}"
        )));
    }

    /// Suspend the current task until the provided duration has elapsed.
    /// You almost always want to call this with `?`
    pub async fn sleep_for(
        &mut self,
        step_name: &str,
        duration: Duration,
    ) -> Result<(), FlowControl> {
        // Look for an existing checkpoint, return if it exists.
        let checkpoint_name = self.checkpoint_name(step_name);
        let res = self
            .storage
            .get_checkpoint(self.task.task_id, &checkpoint_name)
            .await;
        if let Err(err) = res {
            return Err(FlowControl::Failure(format!(
                "failed to get checkpoint, {err:?}"
            )));
        }
        let checkpoint_opt = res.unwrap();
        if let Some(_value) = checkpoint_opt {
            // Found a checkpoint continue.
            return Ok(());
        }

        // Create a checkpoint, and schedule a run in the future.
        let payload = step_name;
        let res = self
            .storage
            .set_checkpoint(
                self.task.task_id,
                self.task.run_id,
                step_name,
                payload.as_bytes(),
                None,
            )
            .await;
        if let Err(err) = res {
            return Err(FlowControl::Failure(format!(
                "Could not store checkpoint. {err:?}"
            )));
        }

        Err(FlowControl::Suspend(duration))
    }
}
