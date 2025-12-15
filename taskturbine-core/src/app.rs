use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use async_channel::Receiver;
use chrono::{DateTime, Utc};
use futures::future::join_all;
use tokio::{signal::unix::SignalKind, task::JoinSet, time};

use crate::{
    api::{Storage, TaskTurbineError},
    config::Config,
    context::{FlowControl, TaskContext},
    models::ClaimedTask,
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

    /// Register a task with a given name.
    ///
    /// Duplicate names will panic at runtime.
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
        Worker {
            app,
            worker_id,
            claim_count: 1,
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
    let (send, recv) = async_channel::bounded::<ClaimedTask>((config.worker_concurrency * 2) as usize);

    log::debug!("Spawning {} executors", config.worker_concurrency);
    let mut task_set = JoinSet::new();
    for _ in 0..config.worker_concurrency {
        task_set.spawn(process_task(arc_worker.clone(), recv.clone()));
    }

    tokio::spawn({
        log::debug!("Spawing cleanup");
        let cleanup_worker = arc_worker.clone();
        let cleanup_config = config.clone();
        let mut timer = time::interval(
            Duration::from_secs(cleanup_config.worker_cleanup_interval_secs as u64)
        );
        timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let guard = elegant_departure::get_shutdown_guard();

        async move {
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        log::debug!("Running cleanup operations.");
                        let cleanup_time = Utc::now() - Duration::from_secs(cleanup_config.worker_cleanup_cutoff_secs as u64);
                        match cleanup_worker.run_cleanup(cleanup_time).await {
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
    });

    tokio::spawn({
        log::debug!("Spawning task claimer");
        let fetch_worker = arc_worker.clone();
        let fetch_config = config.clone();
        let guard = elegant_departure::get_shutdown_guard();

        async move {
            loop {
                let timeout = Utc::now() + Duration::from_secs(60);

                tokio::select! {
                    Ok(claimed) = fetch_worker.claim_tasks(timeout) => {
                        log::debug!("Fetched {} tasks", claimed.len());
                        for task in claimed.iter() {
                            let _ = send.send(task.clone()).await;
                        }
                        if claimed.is_empty() {
                            let sleep_secs = fetch_config.worker_sleep_secs;
                            time::sleep(time::Duration::from_secs(sleep_secs as u64)).await;
                            log::debug!("No tasks completed, worker sleeping for {sleep_secs} seconds");
                        }
                    }
                    _ = guard.wait() => {
                        log::debug!("Shutting down claim_tasks");
                        break;
                    }
                }
            }
        }
    });

    elegant_departure::tokio::depart()
        .on_termination()
        .on_signal(SignalKind::quit())
        .await
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
                break;
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use crate::config::Config;

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
}
