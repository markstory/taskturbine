use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use async_channel::{Receiver, Sender, TrySendError};
use chrono::{DateTime, Utc};
use tokio::{signal::unix::SignalKind, task::JoinSet, time};

use crate::{
    api::{Storage, TaskOptions, TaskTurbineError},
    config::Config,
    context::{FlowControl, TaskContext},
    models::{ClaimedTask, SpawnResult},
};

/// TaskRegistry contains a map of task names -> task handlers
pub type TaskRegistry = HashMap<String, Box<dyn TaskHandler<TaskContext> + Send + Sync>>;

/// The container for a collection of Tasks
pub struct TaskturbineApp {
    config: Config,
    storage: Arc<Storage>,
    tasks: TaskRegistry,
}

impl TaskturbineApp {
    /// Create an app instance from a config object.
    pub fn new(config: Config) -> Self {
        let storage = Arc::new(Storage::new(config.clone()));
        Self {
            config,
            storage,
            tasks: HashMap::new(),
        }
    }

    /// Update the storage instance used.
    pub fn with_storage(&mut self, storage: Storage) -> &mut Self {
        self.storage = Arc::new(storage);

        self
    }

    /// Define a channel that tasks can be consumed on.
    pub fn channel(&mut self, channel: &str) {
        // TODO
    }

    /// Register a task with a given name.
    ///
    /// Once a task is registered, it can be spawned into a named channel
    /// via [`TaskturbineApp::channel()`]
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

    /// Create a worker by consuming the app.
    pub fn create_worker(self, worker_id: &str) -> Worker {
        Worker::new(self, worker_id.to_string())
    }

    /// Spawn a new task and initialize the first run.
    ///
    /// An error is returned if the task name is not registered.
    pub async fn spawn_task(
        &self,
        channel: &str,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, TaskTurbineError> {
        // TODO update this to use the default channel.
        if !self.tasks.contains_key(task_name) {
            return Err(TaskTurbineError::ValidationError(format!(
                "No task named {task_name} is registered."
            )));
        }
        self.storage
            .spawn_task(channel, task_name, params, options)
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
    worker_id: String,
    /// The number of tasks this worker should claim on each iteration
    /// of the run loop.
    claim_count: i32,
}

impl Worker {
    /// Create a new worker.
    pub fn new(app: TaskturbineApp, worker_id: String) -> Self {
        let claim_count = app.config.worker_concurrency;
        Worker {
            app,
            worker_id,
            claim_count,
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
        let res = self
            .app
            .storage
            .claim_task(&self.worker_id, timeout, self.claim_count)
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
    use crate::{api::TaskTurbineError, config::Config};

    use super::TaskturbineApp;

    fn create_app() -> TaskturbineApp {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        let config = Config {
            usecase: "test".to_string(),
            database_url: db_url,
            database_log_queries: false,
            worker_concurrency: 3,
            worker_sleep_secs: 2,
            worker_cleanup_cutoff_secs: 500,
            worker_cleanup_interval_secs: 30,
            worker_cleanup_limit: 1000,
        };
        TaskturbineApp::new(config)
    }

    #[tokio::test]
    #[should_panic]
    async fn register_task_panic() {
        let app = create_app();
        app.register_task("duplicate-task", |_ctx| async { Ok(()) })
            .register_task("duplicate-task", |_ctx| async { Ok(()) });
    }

    #[tokio::test]
    async fn create_channel() {
        let mut app = create_app();
        let ns = app.channel("important");
    }

    #[tokio::test]
    async fn spawn_task_known() {
        let app = create_app().register_task("first-task", |_ctx| async { Ok(()) });

        let res = app.spawn_task("default", "first-task", b"", None).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn spawn_task_not_known() {
        let app = create_app();

        let res = app.spawn_task("default", "first-task", b"", None).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, TaskTurbineError::ValidationError(_)));
    }
}
