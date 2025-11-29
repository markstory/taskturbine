use std::{collections::HashMap, pin::Pin};

use crate::context::{FlowControl, TaskContext};

/// TaskRouter contains a map of task names -> task handlers
pub type TaskRouter = HashMap<String, Box<dyn TaskHandler<TaskContext> + Send + Sync>>;


/// The container for a collection of Tasks
pub struct TaskturbineApp {
    tasks: TaskRouter,
}

impl TaskturbineApp {
    pub fn new() -> Self {
        Self { 
            tasks: HashMap::new()
        }
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
}


/// Trait for async functions that return a result.
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

