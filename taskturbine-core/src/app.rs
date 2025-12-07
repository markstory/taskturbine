use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use chrono::Utc;

use crate::{api::Storage, config::Config, context::{FlowControl, TaskContext}, models::ClaimedTask};

/// TaskRouter contains a map of task names -> task handlers
pub type TaskRouter = HashMap<String, Box<dyn TaskHandler<TaskContext> + Send + Sync>>;


/// The container for a collection of Tasks
pub struct TaskturbineApp {
    storage: Arc<Storage>,
    tasks: TaskRouter,
}

impl TaskturbineApp {
    /// Create an app instance from a config object.
    pub fn new(config: Config) -> Self {
        let storage = Arc::new(Storage::new(config));
        Self {
            storage,
            tasks: HashMap::new()
        }
    }

    /// Update the storage instance used.
    pub fn with_storage(&mut self, storage: Storage) -> &mut Self {
        self.storage = Arc::new(storage);

        self
    }

    /// Register a task with a given name.
    pub fn register_task<T>(mut self, task_name: &str, task_fn: T) -> Self
    where
        T: TaskHandler<TaskContext> + Sync + Send + 'static
    {
        let wrapper = move |ctx| task_fn.call(ctx);
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
/// The current result is not generic, and requires a FlowControl error
/// to be used.
pub trait TaskHandler<Ctx> {
    fn call(&self, ctx: Ctx) -> Pin<Box<dyn Future<Output = Result<(), FlowControl>> + Send>>;
}

// Implement the TaskHandler trait for Fn(TaskContext) -> Ret
// Trait bounds narrow down to async functions that return a narrow result
// type.
impl<F: Sync + 'static, Ret> TaskHandler<TaskContext> for F
where
    F: Fn(TaskContext) -> Ret + Sync + 'static,
    Ret: Future<Output = Result<(), FlowControl>> + Send + 'static,
{
    fn call(&self, ctx: TaskContext) -> Pin<Box<dyn Future<Output = Result<(), FlowControl>> + Send>> {
        Box::pin(self(ctx))
    }
}

#[derive(Debug)]
pub enum WorkerError {
    Message(String)
}

/// Worker instances claim tasks, execute them and update
/// storage with task results.
pub struct Worker {
    app: TaskturbineApp,
    worker_id: String,
    claim_count: i32,
}

impl Worker {
    /// Create a new worker.
    pub fn new(app: TaskturbineApp, worker_id: String) -> Self {
        Worker {
            app, worker_id, claim_count: 1
        }
    }

    /// Runs a single loop of the worker
    ///
    /// High level flow is:
    /// - Claim some tasks
    /// - Execute those tasks.
    ///
    /// Errors from tasks are trapped and reported as task failures.
    pub async fn run_once(&self) -> Result<i32, WorkerError> {
        let timeout = Utc::now() + Duration::from_secs(60);
        let res = self.app.storage.claim_task(&self.worker_id, timeout, self.claim_count).await;
        let claimed = if let Err(err) = res {
            return Err(WorkerError::Message(format!("{err:?}")));
        } else {
            res.unwrap()
        };
        for task in claimed.iter() {
            self.execute_task(task).await;
        }
        Ok(claimed.len() as i32)
    }

    /// Execute a task function and record the execution status.
    async fn execute_task(&self, task: &ClaimedTask) {
        let task_id = &task.task_id;
        println!("Attemting to execute {task_id}");

        let context = TaskContext::build(task.clone(), self.app.storage.clone());
        let taskname = &task.task_name;
        let Some(task_fn) = self.app.tasks.get(taskname) else {
            println!("No task named {taskname} is registered.");
            return;
        };

        let storage = self.app.storage.clone();
        match task_fn.call(context).await {
            Err(FlowControl::InvalidValue(msg)) => println!("Invalid value {msg}"),
            Err(FlowControl::Failure(msg)) => {
                println!("Task run failure: {msg}");
                let retry_at = task.next_retry_at();
                let res = storage.fail_run(task.run_id, b"", Some(retry_at)).await;
                if let Err(schedule_err) = res {
                    println!("Failed to fail run {schedule_err:?}");
                }
            }
            Err(FlowControl::Suspend(wait_for)) => {
                let wake_at = Utc::now() + wait_for;

                let res = storage.schedule_run(task.run_id, wake_at).await;
                if let Err(schedule_err) = res {
                    println!("Failed to schedule run {schedule_err:?}");
                }
            }
            Ok(_) => {
                println!("Completed task {taskname}");
                let res = storage.complete_run(task.run_id, b"").await;
                if let Err(msg) = res {
                    println!("Failed to complete run {msg:?}");
                }
            }
        }
    }
}

pub async fn run_worker(worker: Worker) {
    while let Ok(_) = worker.run_once().await {
        // TODO upkeep/garbage collection?
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
        };
        TaskturbineApp::new(config)
    }

    #[tokio::test]
    async fn worker_run_once_task_success() {
        let app = create_app();
        let storage = &app.storage;
        let _ = storage.spawn_task("test", "hello-world", b"", None).await.unwrap();

        let worker = app.create_worker("some-worker-id");
        let res = worker.run_once().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn worker_run_once_task_failure() {
    }
}
