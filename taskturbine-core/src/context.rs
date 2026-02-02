use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    app::{Channel, TaskturbineApp},
    models::{ClaimedTask, Event, RunId, SpawnResult, TaskId},
    storage::{TaskOptions, TaskTurbineError},
};

/// Used as signaling 'errors' to the worker runtime
/// from userland operations. For example, when a task
/// needs to be suspended because it is waiting on an event.
#[derive(Debug)]
pub enum FlowControl {
    /// The task has encountered an invalid value and cannot continue.
    InvalidValue(String),
    /// The task has encountered a retriable error.
    Failure(String),
    /// The task should be suspended for the given duration.
    Suspend(Duration),
    /// The task is waiting for an event, and is currently suspended.
    Suspended,
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

    /// Get the current counter value for a checkpoint name.
    /// Will return None on checkpoints that aren't known yet.
    pub fn get_counter<'a>(&'a self, name: &str) -> Option<&'a u32> {
        self.counters.get(name)
    }
}

/// The result of steps. The primitive API is just bytes.
pub type StepData = Vec<u8>;

/// Execution context for a task.
/// Passed to task functions by the Worker runtime.
///
/// Context instances let you define task steps, interact with events,
/// and spawn new tasks.
pub struct TaskContext {
    task: ClaimedTask,
    app: Arc<TaskturbineApp>,
    checkpoints: Checkpoints,
}

impl TaskContext {
    /// Create a TaskContext from a ClaimedTask and Application.
    pub fn build(task: ClaimedTask, app: Arc<TaskturbineApp>) -> Self {
        Self {
            task,
            app,
            checkpoints: Checkpoints::new(),
        }
    }

    /// Convert a step name into a unique checkpoint name.
    /// Handles the scenario where userland code has multiple
    /// steps with the same name.
    fn checkpoint_name(&mut self, name: &str) -> String {
        let count = self.checkpoints.incr(name);

        self.format_checkpoint_name(name, &count)
    }

    /// Generate a checkpoint name that includes the name and counter value.
    fn format_checkpoint_name(&self, name: &str, count: &u32) -> String {
        let suffix = if *count == 1 {
            "".to_string()
        } else {
            format!("#{count}")
        };

        format!("{name}{suffix}")
    }

    /// Get the task_id that is currently being run.
    pub fn task_id(&self) -> TaskId {
        self.task.task_id
    }

    /// Get the run_id that is currently being run.
    pub fn run_id(&self) -> RunId {
        self.task.run_id
    }

    /// Get a reference to the parameters of the task as
    /// [`Vec<u8>`]. Converting bytes into a structure is an application
    /// concern.
    pub fn param_bytes(&self) -> &Vec<u8> {
        &self.task.params
    }

    /// Get the result of a previously completed step name.
    /// If the step has not been completed, the return is None.
    /// If there are multiple steps with the same name, the *latest* iteration will be used.
    pub async fn step_result(&self, step_name: &str) -> Result<Option<StepData>, TaskTurbineError> {
        let Some(counter) = self.checkpoints.get_counter(step_name) else {
            return Ok(None);
        };
        let checkpoint_name = self.format_checkpoint_name(step_name, counter);
        let result_data = self
            .app
            .storage
            .get_checkpoint(self.task.task_id, &checkpoint_name)
            .await;

        let Ok(Some(checkpoint)) = result_data else {
            return Ok(None);
        };

        Ok(Some(checkpoint.state))
    }

    /// Define a new async step with a name.
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
        F: FnOnce(TaskContext) -> Fut,
    {
        // See if the step has a completed checkpoint
        let checkpoint_name = self.checkpoint_name(name);
        let res = self
            .app
            .storage
            .get_checkpoint(self.task.task_id, &checkpoint_name)
            .await;
        let Ok(checkpoint_opt) = res else {
            let err = res.err().unwrap();
            return Err(FlowControl::Failure(format!(
                "Failed to read checkpoint {err:?}"
            )));
        };
        if let Some(checkpoint) = checkpoint_opt {
            return Ok(checkpoint.state);
        }

        // Create a disposable context to avoid &mut reference and lifetime hell.
        let context = Self::build(self.task.clone(), self.app.clone());
        let res = step_fn(context).await;

        match res {
            Ok(state) => {
                let res = self
                    .app
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

                Ok(state as StepData)
            }
            // TODO should propagate errors here.
            Err(_) => Err(FlowControl::Failure("Task execution failed".to_string())),
        }
    }

    /// Define a new synchronous step with a name.
    /// When steps complete, they create checkpoints of the
    /// step results which enables re-runs of the task to durably
    /// resume from their last checkpoint.
    pub async fn step<F, E>(&mut self, name: &str, step_fn: F) -> Result<StepData, FlowControl>
    where
        F: FnOnce(TaskContext) -> Result<StepData, E>,
    {
        let async_step_fn = async |ctx: TaskContext| step_fn(ctx);

        self.async_step(name, async_step_fn).await
    }

    /// Record an event as having completed.
    /// Events allow you to synchronize tasks with external actions
    /// that can be recorded as events. Events can have a Payload of bytes.
    /// How those bytes are encoded is an application concern.
    pub async fn emit_event(&self, event_name: &str, payload: &[u8]) -> Result<(), FlowControl> {
        let res = self.app.storage.emit_event(event_name, payload).await;

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
        let wait_for = timeout.unwrap_or_else(|| {
            Duration::from_secs(self.app.config.await_event_default_timeout_secs as u64)
        });
        let step_name = format!("$awaitEvent:{event_name}");

        let res = self
            .app
            .storage
            .await_event(
                self.task.task_id,
                self.task.run_id,
                &step_name,
                event_name,
                Some(wait_for.as_secs()),
            )
            .await;

        match res {
            Ok(wait) => {
                if wait.should_suspend {
                    return Err(FlowControl::Suspended);
                }
                let event = Event {
                    event_name: event_name.to_string(),
                    payload: wait.payload.to_vec(),
                };
                Ok(event)
            }
            Err(err) => Err(FlowControl::Failure(format!(
                "Could not store an event wait: {err:?}"
            ))),
        }
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
            .app
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
            .app
            .storage
            .set_checkpoint(
                self.task.task_id,
                self.task.run_id,
                &checkpoint_name,
                payload.as_bytes(),
                None,
            )
            .await;

        match res {
            Ok(_) => Err(FlowControl::Suspend(duration)),
            Err(err) => Err(FlowControl::Failure(format!(
                "Could not store checkpoint. {err:?}"
            ))),
        }
    }

    /// Spawn a task on the default channel and initialize the first run.
    ///
    /// An error is returned if the task name is not registered.
    pub async fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, TaskTurbineError> {
        self.app.spawn_task(task_name, params, options).await
    }

    /// Get a [`Channel`] wrapper to spawn tasks
    /// on non-default channels.
    ///
    /// An error is returned if the task name is not registered.
    pub fn channel<'a>(&'a self, name: &'a str) -> Channel<'a> {
        self.app.channel(name)
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::{
        app::TaskturbineApp,
        config::Config,
        storage::{Storage, TaskTurbineError},
    };
    use sqlx::Row;

    enum TestError {
        GenericError,
    }

    async fn create_app() -> TaskturbineApp {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        let config = Config {
            usecase: format!("context-test-{}", Uuid::now_v7()),
            database_url: db_url,
            ..Config::default()
        };
        let app = TaskturbineApp::new(config);
        app.storage.update_schema().await.unwrap();

        app
    }

    async fn claim_task(storage: &Storage, task_name: &str) -> ClaimedTask {
        let _ = storage
            .spawn_task("ns", task_name, b"", None)
            .await
            .unwrap();

        let claim_until = Duration::from_secs(60);
        let claimed = storage
            .claim_task(vec![], "worker-1", claim_until, 1)
            .await
            .unwrap();
        let claim = &claimed[0];

        claim.clone()
    }

    async fn hello_world(mut _ctx: TaskContext) -> Result<(), FlowControl> {
        println!("hello world");
        Ok(())
    }

    #[tokio::test]
    async fn step_reads_existing_checkpoint() {
        let app = create_app().await;
        let claim = claim_task(&app.storage, "hello-world").await;
        let res = app
            .storage
            .set_checkpoint(claim.task_id, claim.run_id, "first-step", b"hi", None)
            .await;
        assert!(res.is_ok(), "checkpoint should save");

        let arc_app = Arc::new(app);
        let mut context = TaskContext::build(claim.clone(), arc_app);
        let res = context
            .step::<_, TaskTurbineError>("first-step", |_ctx| Ok(b"should not run".to_vec()))
            .await
            .unwrap();

        assert_eq!(res, b"hi".to_vec(), "Should get checkpoint state");
    }

    #[tokio::test]
    async fn step_stores_checkpoint_on_success() {
        let app = create_app().await;
        let arc_app = Arc::new(app);
        let claim = claim_task(&arc_app.storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), arc_app.clone());

        let res = context
            .step::<_, TaskTurbineError>("first-step", |_ctx| Ok(b"checkpoint value".to_vec()))
            .await
            .unwrap();
        assert_eq!(res, b"checkpoint value".to_vec());

        let stored = arc_app
            .storage
            .get_checkpoint(claim.task_id, "first-step")
            .await;
        let Ok(Some(value)) = stored else {
            panic!("Should read stored checkpoint");
        };
        assert_eq!(value.state, b"checkpoint value".to_vec());
    }

    #[tokio::test]
    async fn step_no_store_checkpoint_on_failure() {
        let app = create_app().await;
        let arc_app = Arc::new(app);
        let claim = claim_task(&arc_app.storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), arc_app.clone());

        let res = context
            .step("first-step", |_ctx| Err(TestError::GenericError))
            .await;

        let Err(err) = res else {
            panic!("await_event should return error here");
        };
        assert!(
            matches!(err, FlowControl::Failure(_)),
            "Should get a flow control error"
        );

        let stored = arc_app
            .storage
            .get_checkpoint(claim.task_id, "first-step")
            .await;
        assert!(
            stored.unwrap().is_none(),
            "Should not have stored checkpoint"
        );
    }

    #[tokio::test]
    async fn emit_event_saves_event() {
        let app = Arc::new(create_app().await);
        let claim = claim_task(&app.storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), app);

        let uuid = Uuid::now_v7();
        let event_id = format!("event-{uuid}");
        let res = context.emit_event(&event_id, b"payload data").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn await_event_saves_wait() {
        let app = Arc::new(create_app().await);
        let claim = claim_task(&app.storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), app.clone());

        let wait_key = format!("await-{}", Uuid::now_v7());
        let res = context
            .await_event(&wait_key, Some(Duration::from_secs(300)))
            .await;

        let Err(err) = res else {
            panic!("await_event should return error here");
        };
        assert!(matches!(err, FlowControl::Suspended));
        let run = app.storage.get_run(claim.run_id).await.unwrap();
        assert_eq!(run.get::<String, _>("state"), "sleeping");
        assert!(run.get::<Option<String>, _>("claimed_by").is_none());
    }

    #[tokio::test]
    async fn await_event_returns_waited_event() {
        let app = Arc::new(create_app().await);
        let claim = claim_task(&app.storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), app.clone());

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
        let app = Arc::new(create_app().await);
        let claim = claim_task(&app.storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), app.clone());

        let res = context
            .sleep_for("sleeptime", Duration::from_secs(60))
            .await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, FlowControl::Suspend(v) if v == Duration::from_secs(60)));

        let res = app.storage.get_checkpoint(claim.task_id, "sleeptime").await;
        assert!(res.is_ok());
        let Ok(Some(checkpoint)) = res else {
            panic!("Could not find checkpoint");
        };
        assert_eq!(b"sleeptime".to_vec(), checkpoint.state);
    }

    #[tokio::test]
    async fn spawn_task_error_on_missing_task() {
        let app = Arc::new(create_app().await);
        let claim = claim_task(&app.storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), app.clone());

        let res = context
            .spawn_task("favorite-food", b"payload data", None)
            .await;
        assert!(res.is_err(), "Should not be able to spawn undefined task");
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::ValidationError(_)
        ))
    }

    #[tokio::test]
    async fn spawn_task_success() {
        let app = create_app().await.register_task("hello-world", hello_world);
        let arc_app = Arc::new(app);

        let claim = claim_task(&arc_app.storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), arc_app.clone());

        let res = context
            .spawn_task("hello-world", b"payload data", None)
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn task_id_and_run_id() {
        let app = create_app().await.register_task("hello-world", hello_world);
        let arc_app = Arc::new(app);

        let claim = claim_task(&arc_app.storage, "hello-world").await;
        let context = TaskContext::build(claim.clone(), arc_app.clone());

        assert_eq!(claim.task_id, context.task_id());
        assert_eq!(claim.run_id, context.run_id());
    }

    #[tokio::test]
    async fn param_bytes() {
        let app = create_app().await.register_task("hello-world", hello_world);
        let arc_app = Arc::new(app);

        let _ = arc_app
            .spawn_task("hello-world", b"{\"name\":\"test\"}", None)
            .await
            .unwrap();

        let claim_until = Duration::from_secs(60);
        let claimed = arc_app
            .storage
            .claim_task(vec![], "worker-1", claim_until, 1)
            .await
            .unwrap();

        let Some(claim) = claimed.first() else {
            panic!("Did not claim a task");
        };
        let context = TaskContext::build(claim.clone(), arc_app.clone());

        let bytes = context.param_bytes();
        assert_eq!(bytes.as_slice(), b"{\"name\":\"test\"}");
    }

    #[tokio::test]
    async fn step_result() {
        let app = create_app().await;
        let arc_app = Arc::new(app);
        let claim = claim_task(&arc_app.storage, "hello-world").await;
        let mut context = TaskContext::build(claim.clone(), arc_app.clone());

        let res = context
            .step::<_, TaskTurbineError>("first-step", |_ctx| Ok(b"checkpoint value".to_vec()))
            .await
            .unwrap();
        assert_eq!(res, b"checkpoint value".to_vec());

        let stored = context.step_result("first-step").await;
        assert!(stored.is_ok());
        let value = stored.unwrap();
        assert!(value.is_some());

        let stored = context.step_result("undefined").await;
        assert!(stored.is_ok());
        let value = stored.unwrap();
        assert!(value.is_none());
    }
}
