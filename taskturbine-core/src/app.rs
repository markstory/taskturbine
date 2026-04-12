use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use async_channel::{Receiver, Sender, TrySendError};
use tokio::{signal::unix::SignalKind, task::JoinSet, time};

use crate::{
    config::Config,
    context::{FlowControl, TaskContext},
    models::{ClaimedTask, SpawnResult},
    storage::{Storage, StorageError, TaskOptions},
};

/// TaskRegistry contains a map of task names -> task handlers
type TaskRegistry = HashMap<String, Box<dyn TaskHandler<TaskContext> + Send + Sync>>;

/// A basic interface for results from tasks & steps.
/// Applications are responsible for decoding bytes.
pub type ResultData = Vec<u8>;

/// The result type of task functions.
pub type TaskResult = Result<Option<ResultData>, FlowControl>;

/// The entrypoint and container for a Task application
///
/// Application instances are created from [`Config`]. Tasks and channels
/// are defined on the Application, and then you can create a [`Worker`]
/// that can execute tasks and perform cleanup operations.
///
/// Tasks can be scheduled using [`TaskturbineApp::spawn_task()`] or [`TaskturbineApp::channel()`]
pub struct TaskturbineApp {
    pub(crate) config: Config,
    pub(crate) storage: Storage,
    tasks: TaskRegistry,
    channels: HashSet<String>,
}

impl TaskturbineApp {
    /// Create an app instance from a config object.
    pub fn new(config: Config) -> Self {
        let storage = Storage::new(config.clone());

        let mut channels = HashSet::new();
        channels.insert(config.default_channel.clone());

        Self {
            config,
            storage,
            channels,
            tasks: HashMap::new(),
        }
    }

    /// Define a channel that tasks can be consumed on.
    ///
    /// Channels allow you to have dedicated workers for specific
    /// workloads in your application. For example, you may want to
    /// have separate worker deployments for high-volume tasks, or
    /// latency sensitive workloads.
    ///
    /// You can spawn tasks onto specific channels using [`TaskturbineApp::channel()`]
    pub fn add_channel(mut self, channel: &str) -> Self {
        self.channels.insert(channel.into());

        self
    }

    /// Check if a channel is defined.
    pub fn has_channel(&self, channel: &str) -> bool {
        self.channels.contains(channel)
    }

    /// Get a Channel that can be used to spawn tasks.
    ///
    /// ```rust
    /// use taskturbine_core::app::TaskturbineApp;
    /// use taskturbine_core::config::Config;
    ///
    /// (async || {
    ///     let config = Config::default();
    ///     let mut app = TaskturbineApp::new(config);
    ///     app = app.add_channel("reports");
    ///
    ///     let event_payload = vec![];
    ///     app.channel("reports").spawn_task("process-feedback", &event_payload, None).await;
    /// })();
    /// ```
    ///
    /// Will panic if an undeclared channel is used.
    pub fn channel<'a>(&'a self, name: &'a str) -> Channel<'a> {
        if !self.has_channel(name) {
            panic!("Unknown channel {name}");
        }
        Channel::new(name, self)
    }

    /// Register a task with a given name.
    ///
    /// Once a task is registered, it can be spawned into any channel
    /// that is defined in the App. See [`TaskturbineApp::channel()`]
    ///
    /// Duplicate task names will panic at runtime.
    ///
    /// ```rust
    /// use taskturbine_core::app::TaskturbineApp;
    /// use taskturbine_core::config::Config;
    ///
    /// (async || {
    ///     let config = Config::default();
    ///     let mut app = TaskturbineApp::new(config);
    ///
    ///     app.register_task("process-feedback", |ctx| async {
    ///         // Do some IO
    ///         Ok(None)
    ///     });
    /// })();
    /// ```
    pub fn register_task<T>(mut self, task_name: &str, task_fn: T) -> Self
    where
        T: TaskHandler<TaskContext> + Sync + Send + 'static,
    {
        let wrapper = move |ctx| task_fn.call(ctx);
        if self.tasks.contains_key(task_name) {
            panic!("Task named {task_name} is already registered");
        }
        self.tasks.insert(task_name.to_string(), Box::new(wrapper));

        self
    }

    /// Check if a task with a given name has been registered.
    pub fn has_task(&self, task_name: &str) -> bool {
        self.tasks.contains_key(task_name)
    }

    /// Create a worker by consuming the app.
    ///
    /// A worker will only claim tasks in `channels` if channels is not-empty.
    /// If `channels` is empty, tasks in all channels will be processed.
    ///
    /// ```rust
    /// use taskturbine_core::app::TaskturbineApp;
    /// use taskturbine_core::config::Config;
    ///
    /// (async || {
    ///     let config = Config::default();
    ///     let mut app = TaskturbineApp::new(config.clone());
    ///
    ///     // Create a worker that consumes from all channels
    ///     // in the application.
    ///     let worker = app.create_worker("worker-1", vec![]);
    ///
    ///     // Create a worker that only consumes `reports` tasks.
    ///     let mut app = TaskturbineApp::new(config);
    ///     let worker = app.create_worker("worker-1", vec!["reports".into()]);
    /// })();
    /// ```
    pub fn create_worker(self, worker_id: &str, channels: Vec<String>) -> Worker {
        let arc_self = Arc::new(self);
        Worker::new(arc_self, worker_id.to_string(), channels)
    }

    /// Spawn a task on the default channel and initialize the first run.
    ///
    /// An error is returned if the task name is not registered.
    ///
    /// ```rust
    /// use taskturbine_core::app::TaskturbineApp;
    /// use taskturbine_core::config::Config;
    ///
    /// (async || {
    ///     let config = Config::default();
    ///     let mut app = TaskturbineApp::new(config.clone());
    ///
    ///     app.spawn_task("process-feedback", b"{\"user_id\":123}", None).await;
    /// })();
    /// ```
    pub async fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, StorageError> {
        if !self.tasks.contains_key(task_name) {
            return Err(StorageError::ValidationError(format!(
                "No task named {task_name} is registered."
            )));
        }
        self.storage
            .spawn_task(&self.config.default_channel, task_name, params, options)
            .await
    }

    /// Record an event as having completed.
    /// Events allow you to synchronize tasks with external actions
    /// that can be recorded as events. Events can have a Payload of bytes.
    /// How those bytes are encoded is an application concern.
    ///
    /// ```rust
    /// use taskturbine_core::app::TaskturbineApp;
    /// use taskturbine_core::config::Config;
    ///
    /// (async || {
    ///     let config = Config::default();
    ///     let mut app = TaskturbineApp::new(config);
    ///
    ///     let payload = b"{\"a\":12}";
    ///     app.emit_event("email-verify-foo@example.com", payload).await;
    /// })();
    /// ```
    pub async fn emit_event(&self, event_name: &str, payload: &[u8]) -> Result<(), FlowControl> {
        let res = self.storage.emit_event(event_name, payload).await;

        if let Err(err) = res {
            return Err(FlowControl::Failure(format!(
                "Could not store event {err:?}"
            )));
        }
        Ok(())
    }
}

/// Trait for async Task functions that return a result.
///
/// This trait isn't directly implemented by application tasks. Instead this
/// trait is implictly implemented by wrapping functions registered with [`TaskturbineApp::register_task()`].
///
/// The current result is not generic, and requires a FlowControl error to be used.
pub trait TaskHandler<Ctx> {
    fn call(&self, ctx: Ctx) -> Pin<Box<dyn Future<Output = TaskResult> + Send>>;
}

/// Implement the TaskHandler trait for Fn(TaskContext) -> Ret
/// Trait bounds narrow down to async functions that return a narrow result
/// type.
impl<F: Sync + 'static, Ret> TaskHandler<TaskContext> for F
where
    F: Fn(TaskContext) -> Ret + Sync + 'static,
    Ret: Future<Output = TaskResult> + Send + 'static,
{
    fn call(&self, ctx: TaskContext) -> Pin<Box<dyn Future<Output = TaskResult> + Send>> {
        Box::pin(self(ctx))
    }
}

/// Channel wrapper
/// Gives a more ergonomic path to working with channels
/// as required.
pub struct Channel<'a> {
    name: &'a str,
    app: &'a TaskturbineApp,
}

impl<'a> Channel<'a> {
    pub fn new(name: &'a str, app: &'a TaskturbineApp) -> Self {
        Self { name, app }
    }

    /// Spawn a task into this channel.
    ///
    /// See [`TaskturbineApp::spawn_task()`].
    pub async fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, StorageError> {
        if !self.app.has_task(task_name) {
            return Err(StorageError::ValidationError(format!(
                "No task named {task_name} is registered."
            )));
        }
        self.app
            .storage
            .spawn_task(self.name, task_name, params, options)
            .await
    }
}

/// Errors from worker operations.
#[derive(Debug)]
pub enum WorkerError {
    Message(String),
}

/// Convert from storage errors to worker errors.
impl From<StorageError> for WorkerError {
    fn from(err: StorageError) -> Self {
        WorkerError::Message(format!("{err:?}"))
    }
}

/// Worker instances claim tasks, execute them and update
/// storage with task results.
///
/// A Worker can be run with [`run_worker`]. You can also
/// use [`run_cleanup_worker`] to run a cleanup worker.
pub struct Worker {
    /// The application instance this worker is for.
    app: Arc<TaskturbineApp>,

    /// The channels this worker is consuming from.
    channels: Vec<String>,

    /// The ID of this worker. It is helpful to give each worker a different ID
    /// so you can track down why tasks are abandoned.
    pub worker_id: String,

    /// The number of tasks this worker should claim on each iteration
    /// of the run loop.
    pub claim_count: i32,
}

impl Worker {
    /// Create a new worker.
    pub fn new(app: Arc<TaskturbineApp>, worker_id: String, channels: Vec<String>) -> Self {
        let claim_count = app.config.worker_concurrency;
        Worker {
            app,
            worker_id,
            claim_count,
            channels,
        }
    }

    /// Get a reference to the Config used by this worker.
    pub fn config(&self) -> &Config {
        &self.app.config
    }

    /// Claim a batch of tasks from storage. The size of the batch
    /// is determined by [`Worker::claim_count`]
    pub async fn claim_tasks(&self, timeout: Duration) -> Result<Vec<ClaimedTask>, WorkerError> {
        let channels = self.channels.iter().map(|i| i.as_ref()).collect();
        let res = self
            .app
            .storage
            .claim_task(channels, &self.worker_id, timeout, self.claim_count)
            .await;
        if let Err(err) = res {
            Err(err.into())
        } else {
            Ok(res.unwrap())
        }
    }

    /// Runs a upkeep step on storage.
    ///
    /// Takes a datetime of what is considered stale and can be purged.
    /// See [run_upkeep_worker](fn.run_upkeep_worker.html) for running upkeep operations.
    pub async fn run_upkeep(&self) -> Result<(), WorkerError> {
        self.app
            .storage
            .run_upkeep()
            .await
            .map_err(|e| WorkerError::Message(format!("{e:?}")))
    }

    /// Execute a task function and record the execution status.
    async fn execute_task(&self, task: ClaimedTask) {
        let task_id = &task.task_id;
        log::debug!("Attempting to execute {task_id}");

        let context = TaskContext::build(task.clone(), self.app.clone());
        let taskname = &task.task_name;
        let Some(task_fn) = self.app.tasks.get(taskname) else {
            log::warn!("No task named {taskname} is registered.");
            // Fail the run.
            // We could be in a cross deploy situation, and following
            // the retry schedule of the task allows for recovery on the next
            // attempt.
            let res = self.app.storage.fail_run(task.run_id, b"", None).await;
            if let Err(schedule_err) = res {
                log::error!("Unable to fail run {schedule_err:?}");
            }
            return;
        };

        let storage = &self.app.storage;
        match task_fn.call(context).await {
            Err(FlowControl::InvalidValue(msg)) => {
                log::warn!("Invalid value {msg}");
                self.fail_run(task).await;
            }
            Err(FlowControl::Failure(msg)) => {
                log::debug!("Task run failure: {msg}");
                self.fail_run(task).await;
            }
            Err(FlowControl::Suspended) => {
                log::debug!("Task run suspended: run_id={}", task.run_id);
            }
            Err(FlowControl::Suspend(wait_for)) => {
                let res = storage.schedule_run(task.run_id, wait_for).await;
                if let Err(schedule_err) = res {
                    log::error!("Failed to schedule run on suspend {schedule_err:?}");
                }
            }
            Ok(maybe_result) => {
                log::debug!("Completed task {taskname}");
                let result_data = maybe_result.unwrap_or_else(Vec::new);
                let res = storage
                    .complete_run(task.run_id, result_data.as_slice())
                    .await;
                if let Err(msg) = res {
                    log::error!("Failed to complete run {msg:?}");
                }
            }
        }
    }

    /// Helper method to fail a run and log an error
    /// if recording the failure also fails.
    async fn fail_run(&self, task: ClaimedTask) {
        let retry_at = task.next_retry_in();
        let res = self
            .app
            .storage
            .fail_run(task.run_id, b"", Some(retry_at))
            .await;
        if let Err(schedule_err) = res {
            log::error!("Failed to fail run {schedule_err:?}");
        }
    }
}

/// Run a worker in a while loop.
/// Consumes the worker and runs indefinitely until the process is killed.
///
/// ```rust
/// use taskturbine_core::app::{TaskturbineApp, run_worker};
/// use taskturbine_core::config::Config;
///
/// (async || {
///     let config = Config::default();
///     let mut app = TaskturbineApp::new(config);
///     app = app.add_channel("reports");
///
///     run_worker(app.create_worker("worker-1", vec!["reports".into()])).await
/// })();
/// ```
///
/// Use [Config](../config/struct.Config.html) to configure worker behavior.
pub async fn run_worker(worker: Worker) {
    let arc_worker = Arc::new(worker);
    let config = arc_worker.config();
    let (send, recv) =
        async_channel::bounded::<ClaimedTask>((config.worker_concurrency * 2) as usize);

    log::debug!("Spawning {} executors", config.worker_concurrency);
    let mut task_set = JoinSet::new();
    for _ in 0..config.worker_concurrency {
        task_set.spawn(process_task(arc_worker.clone(), recv.clone()));
    }
    tokio::spawn(claim_tasks(arc_worker.clone(), send.clone()));

    // TODO This should run the state-machine cleanup, not retention
    if config.worker_upkeep_inline {
        tokio::spawn(run_upkeep(arc_worker.clone()));
    }

    elegant_departure::tokio::depart()
        .on_termination()
        .on_signal(SignalKind::quit())
        .await
}

/// Run a upkeep worker in a while loop.
///
/// In multi-worker deployments, it can be more efficient to run the upkeep
/// operations as a dedicated worker/process instead of having each worker
/// periodically running upkeep operations.
///
/// Consumes the worker and runs indefinitely until the process is killed.
///
/// ```rust
/// use taskturbine_core::app::{TaskturbineApp, run_upkeep_worker};
/// use taskturbine_core::config::Config;
///
/// (async || {
///     let config = Config::default();
///     let app = TaskturbineApp::new(config);
///
///     run_upkeep_worker(app.create_worker("worker-1", vec!["feedback-ingest".into()])).await
/// })();
/// ```
///
/// Use [Config](../config/struct.Config.html) to configure worker behavior.
pub async fn run_upkeep_worker(worker: Worker) {
    let arc_worker = Arc::new(worker);

    tokio::spawn(run_upkeep(arc_worker.clone()));

    elegant_departure::tokio::depart()
        .on_termination()
        .on_signal(SignalKind::quit())
        .await
}

/// Run upkeep operations periodically.
/// Every `config.worker_upkeep_interval` a upkeep operation will run
/// which deletes completed tasks and expired events from the database.
async fn run_upkeep(worker: Arc<Worker>) {
    log::debug!("Spawing upkeep");
    let config = worker.config();
    let mut timer = time::interval(Duration::from_secs(
        config.worker_upkeep_interval_secs as u64,
    ));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let guard = elegant_departure::get_shutdown_guard();

    loop {
        tokio::select! {
            _ = timer.tick() => {
                log::debug!("Running upkeep operations.");
                match worker.run_upkeep().await {
                    Ok(_) => {
                        log::info!("upkeep operations complete");
                    },
                    Err(err) => {
                        log::error!("{err:?}");
                    }
                }
            }
            _ = guard.wait() => {
                log::debug!("Shutting down upkeep");
                break;
            }
        }
    }
}

/// Claim tasks and append them to the `work_send` channel.
/// If no tasks could be claimed, the claimer will sleep for `config.worker_sleep_secs`.
async fn claim_tasks(worker: Arc<Worker>, work_send: Sender<ClaimedTask>) {
    log::debug!("Spawning claim_tasks");
    let config = worker.config();
    let guard = elegant_departure::get_shutdown_guard();

    loop {
        let timeout = Duration::from_secs(config.worker_claim_timeout_secs as u64);
        tokio::select! {
            Ok(mut claimed) = worker.claim_tasks(timeout) => {
                log::debug!("Claimed {} tasks", claimed.len());
                if claimed.is_empty() {
                    let sleep_secs = config.worker_sleep_secs;
                    log::debug!("No tasks claimed, worker sleeping for {sleep_secs} seconds");
                    time::sleep(time::Duration::from_secs(sleep_secs as u64)).await;
                }
                while !claimed.is_empty() {
                    let task_opt = claimed.last();
                    if task_opt.is_none() {
                        // All claimed tasks have been processsed.
                        break;
                    }
                    let task = task_opt.unwrap();

                    match work_send.try_send(task.clone()) {
                        Ok(_) => {
                            // Task was sent, it can be popped now.
                            claimed.pop();
                        },
                        Err(TrySendError::Full(_)) => {
                            // Backpressure as all executors are busy.
                            // If we blocking send the worker won't shutdown.
                            log::debug!("work_send was full; sleeping and re-attempting.");
                            time::sleep(time::Duration::from_secs(config.worker_sleep_secs as u64)).await;
                        },
                        Err(TrySendError::Closed(_)) => {
                            log::warn!("Channel is closed, shutting down claim_tasks");
                            return;
                        }
                    }
                }
            }
            _ = guard.wait() => {
                log::debug!("Shutting down claim_tasks");
                break;
            }
        }
    }
}

/// Read from the inbound worker channel and execute tasks.
async fn process_task(worker: Arc<Worker>, work_channel: Receiver<ClaimedTask>) {
    let guard = elegant_departure::get_shutdown_guard();
    loop {
        tokio::select! {
            Ok(task) = work_channel.recv() => {
                worker.execute_task(task).await;
            }
            _ = guard.wait() => {
                log::debug!("Shutting down process_task");
                work_channel.close();
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use sqlx::Row;
    use uuid::Uuid;

    use crate::{
        config::Config,
        context::{FlowControl, TaskContext},
        models::TaskState,
        storage::{StorageError, TaskOptions},
    };

    use super::TaskturbineApp;

    async fn create_app() -> TaskturbineApp {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        let config = Config {
            usecase: "test".to_string(),
            database_url: db_url,
            default_channel: "channel-one".into(),
            ..Config::default()
        };
        let app = TaskturbineApp::new(config);
        app.storage.update_schema().await.unwrap();

        app
    }

    async fn create_app_with_task(channel: &str) -> TaskturbineApp {
        create_app()
            .await
            .add_channel(channel)
            .register_task("first-task", |_ctx| async { Ok(None) })
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[tokio::test]
    #[should_panic]
    async fn register_task_panic() {
        create_app()
            .await
            .register_task("duplicate-task", |_ctx| async { Ok(None) })
            .register_task("duplicate-task", |_ctx| async { Ok(None) });
    }

    #[tokio::test]
    async fn add_channel_has_channel() {
        let app = create_app().await.add_channel("reports");

        assert!(app.has_channel("reports"), "Should have defined channel");
        assert!(
            app.has_channel("channel-one"),
            "Should have default channel"
        );
        assert!(
            !app.has_channel("undefined"),
            "Should not have unregistered channel"
        );
    }

    #[tokio::test]
    async fn add_channel_and_spawn() {
        let app = create_app()
            .await
            .add_channel("reports")
            .register_task("hello-world", |_ctx| async { Ok(None) });

        let res = app
            .channel("reports")
            .spawn_task("hello-world", b"", None)
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    #[should_panic]
    async fn channel_panic_on_undefined() {
        create_app().await.channel("duplicate-task");
    }

    #[tokio::test]
    async fn spawn_task_known() {
        let app = create_app()
            .await
            .register_task("first-task", |_ctx| async { Ok(None) });

        let res = app.spawn_task("first-task", b"", None).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn spawn_task_not_known() {
        let app = create_app().await;

        let res = app.spawn_task("first-task", b"", None).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, StorageError::ValidationError(_)));
    }

    #[tokio::test]
    async fn emit_event_saves_event() {
        let app = create_app().await;

        let uuid = Uuid::now_v7();
        let event_id = format!("event-{uuid}");
        let res = app.emit_event(&event_id, b"payload data").await;
        assert!(res.is_ok(), "failed to emit event");
    }

    #[tokio::test]
    async fn worker_config() {
        let channel = "worker_config";
        let app = create_app_with_task(channel).await;

        let worker = app.create_worker("worker-1", vec![]);
        let config = worker.config();
        assert_eq!(config.default_channel, "channel-one");
    }

    #[tokio::test]
    async fn worker_claim_tasks() {
        let channel = "worker_claim_tasks";
        let app = create_app_with_task(channel).await;

        let res = app
            .channel(channel)
            .spawn_task("first-task", b"", None)
            .await;
        assert!(res.is_ok(), "failed to spawn task");
        let spawned = res.unwrap();

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        let claimed = res.unwrap();
        assert_eq!(claimed.len(), 1);

        let claim = &claimed[0];
        assert_eq!(claim.task_id, spawned.task_id);
    }

    #[tokio::test]
    async fn worker_execute_task() {
        let channel = "worker_execute_task";
        let app = create_app_with_task(channel).await;

        let res = app
            .channel(channel)
            .spawn_task("first-task", b"", None)
            .await;
        assert!(res.is_ok(), "failed to spawn task");

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        for task in res.unwrap().into_iter() {
            let run_id = task.run_id;
            worker.execute_task(task).await;

            let task_data = worker.app.storage.get_run(run_id).await.unwrap();
            assert_eq!(TaskState::Completed, task_data.get::<TaskState, _>("state"));
        }
    }

    #[tokio::test]
    async fn worker_execute_task_result() {
        let channel = "worker_execute_task_results";
        let mut app = create_app_with_task(channel).await;
        app = app.register_task("result-task", async |_ctx: TaskContext| {
            let data = (b"{\"some\":\"json\"}").to_vec();
            Ok(Some(data))
        });

        let res = app
            .channel(channel)
            .spawn_task("result-task", b"", None)
            .await;
        assert!(res.is_ok(), "failed to spawn task");

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        for task in res.unwrap().into_iter() {
            let run_id = task.run_id;
            worker.execute_task(task).await;

            let task_data = worker.app.storage.get_run(run_id).await.unwrap();
            dbg!(&task_data);
            assert_eq!(TaskState::Completed, task_data.get::<TaskState, _>("state"));
            assert_eq!(
                b"{\"some\":\"json\"}".to_vec(),
                task_data.get::<Vec<u8>, _>("result")
            );
        }
    }

    #[tokio::test]
    async fn worker_execute_task_failure() {
        let channel = "worker_execute_task_failure";
        let mut app = create_app_with_task(channel).await;
        app = app.register_task("fail-task", async |_ctx: TaskContext| {
            Err(FlowControl::Failure("failure".to_string()))
        });

        let res = app
            .channel(channel)
            .spawn_task("fail-task", b"", None)
            .await;
        assert!(res.is_ok(), "failed to spawn task");

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        for task in res.unwrap().into_iter() {
            let run_id = task.run_id;
            worker.execute_task(task).await;

            let task_data = worker.app.storage.get_run(run_id).await.unwrap();
            assert_eq!(TaskState::Failed, task_data.get::<TaskState, _>("state"));
        }
    }

    #[tokio::test]
    async fn worker_execute_task_invalid_value() {
        let channel = "worker_execute_task_invalid_value";
        let mut app = create_app_with_task(channel).await;
        app = app.register_task("second-task", async |_ctx: TaskContext| {
            Err(FlowControl::InvalidValue(
                "something invalid was passed".to_string(),
            ))
        });

        let res = app
            .channel(channel)
            .spawn_task("second-task", b"", None)
            .await;
        assert!(res.is_ok(), "failed to spawn task");

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        for task in res.unwrap().into_iter() {
            let run_id = task.run_id;
            worker.execute_task(task).await;

            let task_data = worker.app.storage.get_run(run_id).await.unwrap();
            assert_eq!(TaskState::Failed, task_data.get::<TaskState, _>("state"));
        }
    }

    #[tokio::test]
    async fn worker_execute_task_suspend_with_duration() {
        // Include a timestamp to isolate test runs as we leave state behind
        let channel = format!("worker_execute_task_suspended_{}", now());
        let mut app = create_app_with_task(&channel).await;
        app = app.register_task("sleep-task", async |mut ctx: TaskContext| {
            ctx.sleep_for("sleep-time", Duration::from_secs(30)).await?;
            Ok(None)
        });

        let options = TaskOptions {
            max_attempts: 1,
            ..TaskOptions::default()
        };
        let res = app
            .channel(&channel)
            .spawn_task("sleep-task", b"", Some(options))
            .await;
        assert!(res.is_ok(), "failed to spawn task");

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        for task in res.unwrap().into_iter() {
            let run_id = task.run_id;
            worker.execute_task(task).await;

            let task_data = worker.app.storage.get_run(run_id).await.unwrap();
            assert_eq!(task_data.get::<TaskState, _>("state"), TaskState::Sleeping);
            assert_eq!(
                task_data.get::<Option<String>, _>("claimed_by"),
                None,
                "claim should be released on suspension"
            );
            assert_eq!(
                task_data.get::<Option<String>, _>("claim_expires_at"),
                None,
                "claim expiry should be cleared on suspension"
            );
        }
    }

    #[tokio::test]
    async fn worker_execute_task_suspend() {
        // Include a timestamp to isolate test runs as we leave state behind
        let channel = format!("worker_execute_task_suspend_{}", now());
        let mut app = create_app_with_task(&channel).await;
        app = app.register_task("sleep-task", async |ctx: TaskContext| {
            let _event = ctx.await_event("sleep-time", None).await?;
            Ok(None)
        });

        let res = app
            .channel(&channel)
            .spawn_task("sleep-task", b"", None)
            .await;
        assert!(res.is_ok(), "failed to spawn task");

        let worker = app.create_worker("worker-1", vec![channel.to_string()]);
        let timeout = Duration::from_secs(300);
        let res = worker.claim_tasks(timeout).await;
        assert!(res.is_ok(), "failed to claim tasks");

        for task in res.unwrap().into_iter() {
            let run_id = task.run_id;
            worker.execute_task(task).await;

            let task_data = worker.app.storage.get_run(run_id).await.unwrap();
            assert_eq!(TaskState::Sleeping, task_data.get::<TaskState, _>("state"));
        }
    }

    #[tokio::test]
    async fn worker_execute_task_undefined_task() {
        let channel = "worker_execute_task_undefined_task";
        let app = create_app_with_task(channel).await;

        let res = app
            .channel(channel)
            .spawn_task("undefined", b"", None)
            .await;
        assert!(res.is_err(), "undefined task should fail");
        let err = res.err().unwrap();
        assert!(matches!(err, StorageError::ValidationError(..)));
    }

    #[tokio::test]
    async fn worker_run_upkeep() {
        // TODO expand this test to check that it releases claims
        // and cancels old tasks
        let channel = "worker_run_cleanup";
        let app = create_app_with_task(channel).await;

        let worker = app.create_worker("worker-1", vec![]);
        let res = worker.run_upkeep().await;
        assert!(res.is_ok());
    }
}
