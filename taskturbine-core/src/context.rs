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

/// Provides in memory storage of steps -> checkpoint names
/// It is possible for userland code to repeat step names
/// (like in a loop). We need to handle tracking separate
/// completion states for each iteration.
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
            *value
        } else {
            0
        }
    }
}

/// The result of steps. The basic API is just bytes.
///
/// TODO figure out how to integrate serde for this.
/// Perhaps that is best left to userland code?
pub type StepData = Vec<u8>;

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
    /// Create a TaskContext from a ClaimedTask and Storage API.
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

        format!("{name}{suffix}")
    }

    /// Define a new async step with a name
    /// When steps complete, they create checkpoints of the
    /// step results which enables re-runs of the task to durably
    /// resume from their last checkpoint.
    pub async fn async_step<F, E, Fut>(
        &mut self,
        name: &str,
        step_fn: F,
    ) -> Result<StepData, FlowControl>
    where
        Fut: Future<Output = Result<StepData, E>>,
        F: FnOnce() -> Fut,
    {
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
        let res = step_fn().await;
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

            return Ok(state as StepData);
        }
        Err(FlowControl::Failure("Task execution failed".to_string()))
    }

    /// Define a new synchronous step with a name
    /// When steps complete, they create checkpoints of the
    /// step results which enables re-runs of the task to durably
    /// resume from their last checkpoint.
    pub async fn step<F, E>(&mut self, name: &str, step_fn: F) -> Result<StepData, FlowControl>
    where
        F: FnOnce() -> Result<StepData, E>,
    {
        let async_step_fn = async || step_fn();

        self.async_step(name, async_step_fn).await
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
        Err(FlowControl::Failure(format!(
            "Could not store an event wait: {err:?}"
        )))
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

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::{api::TaskTurbineError, config::Config};

    enum TestError {
        GenericError,
    }

    async fn create_storage() -> Storage {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        let config = Config {
            usecase: "test".into(),
            database_url: db_url,
            database_log_queries: false,
            worker_concurrency: 3,
            worker_sleep_secs: 2,
            worker_cleanup_cutoff_secs: 500,
            worker_cleanup_probability: 0.1,
            worker_cleanup_limit: 1000,
        };
        let storage = Storage::new(config);

        // Ensure migrations have been applied
        storage.update_schema().await.unwrap();

        storage
    }

    async fn claim_task(storage: &Storage, task_name: &str) -> ClaimedTask {
        let _ = storage.spawn_task("ns", task_name, b"", None).await;

        let claim_until = Utc::now() + Duration::from_secs(60);
        let claimed = storage
            .claim_task("worker-1", claim_until, 1)
            .await
            .unwrap();
        let claim = &claimed[0];

        claim.clone()
    }

    #[tokio::test]
    async fn step_reads_existing_checkpoint() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let res = storage
            .set_checkpoint(claim.task_id, claim.run_id, "first-step", b"hi", None)
            .await;
        assert!(res.is_ok(), "checkpoint should save");

        let mut context = TaskContext::build(claim.clone(), storage);
        let res = context
            .step::<_, TaskTurbineError>("first-step", || Ok(b"should not run".to_vec()))
            .await
            .unwrap();

        assert_eq!(res, b"hi".to_vec(), "Should get checkpoint state");
    }

    #[tokio::test]
    async fn step_stores_checkpoint_on_success() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), storage.clone());

        let res = context
            .step::<_, TaskTurbineError>("first-step", || Ok(b"checkpoint value".to_vec()))
            .await
            .unwrap();
        assert_eq!(res, b"checkpoint value".to_vec());

        let stored = storage.get_checkpoint(claim.task_id, "first-step").await;
        let Ok(Some(value)) = stored else {
            panic!("Should read stored checkpoint");
        };
        assert_eq!(value.state, b"checkpoint value".to_vec());
    }

    #[tokio::test]
    async fn step_no_store_checkpoint_on_failure() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), storage.clone());

        let res = context
            .step("first-step", || Err(TestError::GenericError))
            .await;

        let Err(err) = res else {
            panic!("await_event should return error here");
        };
        assert!(
            matches!(err, FlowControl::Failure(_)),
            "Should get a flow control error"
        );

        let stored = storage.get_checkpoint(claim.task_id, "first-step").await;
        assert!(
            stored.unwrap().is_none(),
            "Should not have stored checkpoint"
        );
    }

    #[tokio::test]
    async fn emit_event_saves_event() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), storage.clone());

        let uuid = Uuid::now_v7();
        let event_id = format!("event-{uuid}");
        let res = context.emit_event(&event_id, b"payload data").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn await_event_saves_wait() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), storage.clone());

        let wait_key = format!("await-{}", Uuid::now_v7());
        let res = context
            .await_event(&wait_key, Some(Duration::from_secs(300)))
            .await;

        let Err(err) = res else {
            panic!("await_event should return error here");
        };
        assert!(matches!(err, FlowControl::Suspend(v) if v == Duration::from_secs(300)));
    }

    #[tokio::test]
    async fn await_event_returns_waited_event() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), storage.clone());

        let wait_key = format!("await-{}", Uuid::now_v7());
        let res = context
            .emit_event(&wait_key, b"{wait_key} event payload")
            .await;
        assert!(res.is_ok());
        let res = context
            .await_event(&wait_key, Some(Duration::from_secs(300)))
            .await;

        assert!(res.is_ok());
        let event = res.unwrap();
        assert_eq!(wait_key, event.event_name);
    }

    #[tokio::test]
    async fn sleep_for_suspends() {
        let storage = Arc::new(create_storage().await);
        let claim = claim_task(&storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), storage.clone());

        let res = context
            .sleep_for("sleeptime", Duration::from_secs(60))
            .await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, FlowControl::Suspend(v) if v == Duration::from_secs(60)));

        let res = storage.get_checkpoint(claim.task_id, "sleeptime").await;
        assert!(res.is_ok());
        let Ok(Some(checkpoint)) = res else {
            panic!("Could not find checkpoint");
        };
        assert_eq!(b"sleeptime".to_vec(), checkpoint.state);
    }
}
