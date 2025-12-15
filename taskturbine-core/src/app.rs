use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures::future::join_all;
use rand::Rng;
use tokio::time;

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
        return if let Err(err) = res {
            return Err(err.into());
        } else {
            Ok(res.unwrap())
        };
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
        log::debug!("Attemting to execute {task_id}");

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
    let mut rng = rand::rng();
    let arc_worker = Arc::new(worker);
    let config = arc_worker.config();

    while let Ok(completed) = run_batch(arc_worker.clone()).await {
        if completed == 0 {
            let sleep_secs = config.worker_sleep_secs;
            time::sleep(time::Duration::from_secs(sleep_secs as u64)).await;
            log::debug!("No tasks completed, worker sleeping for {sleep_secs} seconds");
        }

        // Run cleanup periodically. Use rng to sample
        if config.worker_cleanup_probability > rng.random() {
            let cleanup_time =
                Utc::now() - Duration::from_secs(config.worker_cleanup_cutoff_secs as u64);
            match arc_worker.run_cleanup(cleanup_time).await {
                Ok(_) => (),
                Err(err) => {
                    log::error!("{err:?}");
                }
            }
        }
    }
}

/// Run a batch of tasks from a worker concurrently.
/// This will claim a batch of tasks from the worker,
/// spawn [`Worker::execute_task`] in parallel and wait for the results.
async fn run_batch(worker: Arc<Worker>) -> Result<i32, WorkerError> {
    let timeout = Utc::now() + Duration::from_secs(60);
    let claimed = worker.claim_tasks(timeout).await?;

    let executions: Vec<_> = claimed
        .into_iter()
        .map(|task| worker.execute_task(task))
        .collect();

    let completed = executions.len();
    let _ = join_all(executions).await;

    Ok(completed as i32)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::config::Config;

    use super::{TaskturbineApp, run_batch};

    fn create_app() -> TaskturbineApp {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        let config = Config {
            usecase: "test".to_string(),
            database_url: db_url,
            database_log_queries: false,
            worker_sleep_secs: 2,
            worker_cleanup_cutoff_secs: 500,
            worker_cleanup_probability: 0.1,
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
    async fn worker_run_once_task_success() {
        let app = create_app();
        let storage = &app.storage;
        let _ = storage
            .spawn_task("test", "hello-world", b"", None)
            .await
            .unwrap();

        let worker = Arc::new(app.create_worker("some-worker-id"));
        let res = run_batch(worker).await;
        assert!(res.is_ok());
    }
}
