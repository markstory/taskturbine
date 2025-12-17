use std::{collections::{HashMap, HashSet}, pin::Pin, sync::Arc, time::Duration};

use async_channel::{Receiver, Sender, TrySendError};
use chrono::{DateTime, Utc};
use tokio::{signal::unix::SignalKind, task::JoinSet, time};

use crate::{
    config::Config,
    context::{FlowControl, TaskContext},
    models::{ClaimedTask, SpawnResult},
    storage::{Storage, TaskOptions, TaskTurbineError},
};

/// TaskRegistry contains a map of task names -> task handlers
type TaskRegistry = HashMap<String, Box<dyn TaskHandler<TaskContext> + Send + Sync>>;

/// The container for a collection of Tasks
pub struct TaskturbineApp {
    config: Config,
    storage: Arc<Storage>,
    tasks: TaskRegistry,
    channels: HashSet<String>,
}

impl TaskturbineApp {
    /// Create an app instance from a config object.
    pub fn new(config: Config) -> Self {
        let storage = Arc::new(Storage::new(config.clone()));

        let mut channels = HashSet::new();
        channels.insert(config.default_channel.clone());

        Self {
            config,
            storage,
            channels,
            tasks: HashMap::new(),
        }
    }

    /// Update the storage instance used.
    pub fn with_storage(&mut self, storage: Storage) -> &mut Self {
        self.storage = Arc::new(storage);

        self
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
    /// Will panic if an undeclared channel is used.
    pub fn channel<'a>(&'a self, name: &'a str) -> Channel<'a> {
        if !self.has_channel(name) {
            panic!("Unknown channel {}", name);
        }
        Channel::new(name, self)
    }

    /// Register a task with a given name.
    ///
    /// Once a task is registered, it can be spawned into any channel
    /// that is defined in the App. See [`TaskturbineApp::channel()`]
    ///
    /// Duplicate task names will panic at runtime.
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
    pub fn create_worker(self, worker_id: &str, channels: Vec<String>) -> Worker {
        Worker::new(self, worker_id.to_string(), channels)
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
        if !self.tasks.contains_key(task_name) {
            return Err(TaskTurbineError::ValidationError(format!(
                "No task named {task_name} is registered."
            )));
        }
        self.storage
            .spawn_task(&self.config.default_channel, task_name, params, options)
            .await
    }
}

/// Trait for async Task functions that return a result.
///
/// This trait isn't directly implemented by application tasks. Instead this
/// trait is implictly implemented by wrapping functions registered with [`TaskturbineApp::register_task()`].
///
/// The current result is not generic, and requires a FlowControl error to be used.
pub trait TaskHandler<Ctx> {
    fn call(&self, ctx: Ctx) -> Pin<Box<dyn Future<Output = Result<(), FlowControl>> + Send>>;
}

/// Implement the TaskHandler trait for Fn(TaskContext) -> Ret
/// Trait bounds narrow down to async functions that return a narrow result
/// type.
/// TODO: Consider replacing `()` with `Bytes` so that tasks can return values.
impl<F: Sync + 'static, Ret> TaskHandler<TaskContext> for F
where
    F: Fn(TaskContext) -> Ret + Sync + 'static,
    Ret: Future<Output = Result<(), FlowControl>> + Send + 'static,
{
    fn call(
        &self,
        ctx: TaskContext,
    ) -> Pin<Box<dyn Future<Output = Result<(), FlowControl>> + Send>> {
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
        Self {
            name, app,
        }
    }

    /// Spawn a task into this channel.
    ///
    /// See [`TaskturbineApp::spawn_task()`].
    pub async fn spawn_task(
        &self,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, TaskTurbineError> {
        if !self.app.has_task(task_name) {
            return Err(TaskTurbineError::ValidationError(format!(
                "No task named {task_name} is registered."
            )));
        }
        self.app.storage.spawn_task(self.name, task_name, params, options).await
    }
}


/// Errors from worker operations.
#[derive(Debug)]
pub enum WorkerError {
    Message(String),
}

/// Convert from storage errors to worker errors.
impl From<TaskTurbineError> for WorkerError {
    fn from(err: TaskTurbineError) -> Self {
        WorkerError::Message(format!("{err:?}"))
    }
}

/// Worker instances claim tasks, execute them and update
/// storage with task results.
pub struct Worker {
    app: TaskturbineApp,
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
    pub fn new(app: TaskturbineApp, worker_id: String, channels: Vec<String>) -> Self {
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
    pub async fn claim_tasks(
        &self,
        timeout: DateTime<Utc>,
    ) -> Result<Vec<ClaimedTask>, WorkerError> {
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

    /// Runs a cleanup step on storage.
    ///
    /// Takes a datetime of what is considered stale and can be purged.
    pub async fn run_cleanup(&self, older_than: DateTime<Utc>) -> Result<(), WorkerError> {
        let cleanup_limit = self.config().worker_cleanup_limit;
        let res = self
            .app
            .storage
            .cleanup_events(older_than, cleanup_limit)
            .await;
        if let Err(err) = res {
            return Err(err.into());
        }
        let res = self
            .app
            .storage
            .cleanup_tasks(older_than, cleanup_limit)
            .await;
        if let Err(err) = res {
            return Err(err.into());
        }
        Ok(())
    }

    /// Execute a task function and record the execution status.
    async fn execute_task(&self, task: ClaimedTask) {
        let task_id = &task.task_id;
        log::debug!("Attempting to execute {task_id}");

        let context = TaskContext::build(task.clone(), self.app.storage.clone());
        let taskname = &task.task_name;
        let Some(task_fn) = self.app.tasks.get(taskname) else {
            log::warn!("No task named {taskname} is registered.");
            return;
        };

        let storage = self.app.storage.clone();
        match task_fn.call(context).await {
            Err(FlowControl::InvalidValue(msg)) => log::warn!("Invalid value {msg}"),
            Err(FlowControl::Failure(msg)) => {
                log::debug!("Task run failure: {msg}");

                let retry_at = task.next_retry_at();
                let res = storage.fail_run(task.run_id, b"", Some(retry_at)).await;
                if let Err(schedule_err) = res {
                    log::error!("Failed to fail run {schedule_err:?}");
                }
            }
            Err(FlowControl::Suspend(wait_for)) => {
                let wake_at = Utc::now() + wait_for;

                let res = storage.schedule_run(task.run_id, wake_at).await;
                if let Err(schedule_err) = res {
                    log::error!("Failed to schedule run {schedule_err:?}");
                }
            }
            Ok(_) => {
                log::debug!("Completed task {taskname}");
                let res = storage.complete_run(task.run_id, b"").await;
                if let Err(msg) = res {
                    log::error!("Failed to complete run {msg:?}");
                }
            }
        }
    }
}

/// Run a a worker in a while loop.
/// Consumes the worker and runs indefinitely until the process is killed.
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

    tokio::spawn(run_cleanup(arc_worker.clone()));
    tokio::spawn(claim_tasks(arc_worker.clone(), send.clone()));

    elegant_departure::tokio::depart()
        .on_termination()
        .on_signal(SignalKind::quit())
        .await
}

/// Run cleanup operations periodically.
/// Every `config.worker_cleanup_interval` a cleanup operation will run
/// which deletes completed tasks and expired events from the database.
async fn run_cleanup(worker: Arc<Worker>) {
    log::debug!("Spawing cleanup");
    let config = worker.config();
    let mut timer = time::interval(Duration::from_secs(
        config.worker_cleanup_interval_secs as u64,
    ));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let guard = elegant_departure::get_shutdown_guard();

    loop {
        tokio::select! {
            _ = timer.tick() => {
                log::debug!("Running cleanup operations.");
                let cleanup_time = Utc::now() - Duration::from_secs(config.worker_cleanup_cutoff_secs as u64);
                match worker.run_cleanup(cleanup_time).await {
                    Ok(_) => (),
                    Err(err) => {
                        log::error!("{err:?}");
                    }
                }
            }
            _ = guard.wait() => {
                log::debug!("Shutting down cleanup");
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
        let timeout = Utc::now() + Duration::from_secs(60);
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
                        // We got to the end, break this while loop
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
    use crate::{storage::TaskTurbineError, config::Config};

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

    #[tokio::test]
    #[should_panic]
    async fn register_task_panic() {
        create_app()
            .await
            .register_task("duplicate-task", |_ctx| async { Ok(()) })
            .register_task("duplicate-task", |_ctx| async { Ok(()) });
    }

    #[tokio::test]
    async fn add_channel_has_channel() {
        let app = create_app()
            .await
            .add_channel("reports");

        assert!(app.has_channel("reports"), "Should have defined channel");
        assert!(app.has_channel("channel-one"), "Should have default channel");
        assert!(!app.has_channel("undefined"), "Should not have unregistered channel");
    }

    #[tokio::test]
    async fn add_channel_and_spawn() {
        let app = create_app()
            .await
            .add_channel("reports")
            .register_task("hello-world", |_ctx| async { Ok(()) });

        let res = app.channel("reports").spawn_task("hello-world", b"", None).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    #[should_panic]
    async fn channel_panic_on_undefined() {
        create_app()
            .await
            .channel("duplicate-task");
    }

    #[tokio::test]
    async fn spawn_task_known() {
        let app = create_app()
            .await
            .register_task("first-task", |_ctx| async { Ok(()) });

        let res = app.spawn_task("first-task", b"", None).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn spawn_task_not_known() {
        let app = create_app().await;

        let res = app.spawn_task("first-task", b"", None).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, TaskTurbineError::ValidationError(_)));
    }
}
