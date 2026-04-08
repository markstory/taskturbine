use clap::Args;

use crate::{CliError, admin_storage::{AdminStorage, TaskListOptions}};
use taskturbine_core::{
    models::TaskState,
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct TaskListArgs {
    /// TODO make this a glob pattern
    #[arg(
        long,
        help = "A substring to match task names against"
    )]
    pub taskname: Option<String>,

    #[arg(
        long,
        help = "The task state value to filter by"
    )]
    pub state: Option<TaskState>,

    #[arg(
        long,
        help = "The task channel"
    )]
    pub channel: Option<String>,

    #[arg(
        long,
        help = "The task usecase"
    )]
    pub usecase: Option<String>,
}

/// Implement into/from to convert into the storage interface struct
impl From<TaskListArgs> for TaskListOptions {
    fn from(value: TaskListArgs) -> Self {
        TaskListOptions {
            taskname: value.taskname,
            state: value.state,
            channel: value.channel,
            usecase: value.usecase,
        }
    }
}

pub async fn execute(storage: Storage, args: TaskListArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let options: TaskListOptions = args.into();

    let tasks = admin_storage
        .task_list(options)
        .await
        .map_err(<StorageError as Into<CliError>>::into)?;

    for task in tasks.iter() {
        println!("Task Id: {}", task.task_id);
        println!("  usecase:    {}", task.usecase);
        println!("  channel:    {}", task.channel);
        println!("  task_name:  {}", task.task_name);
        println!("  state:      {}", task.state);
        println!("  headers:    {}", str::from_utf8(&task.headers).unwrap_or("<non-utf8 data>"));
        println!("  parameters: {}", str::from_utf8(&task.params).unwrap_or("<non-utf8 data>"));
        println!(" Retry:");
        println!("  seconds:      {}", &task.retry_seconds);
        println!("  factor:       {}", &task.retry_factor);
        println!("  max_seconds:  {}", &task.retry_max_seconds);
        println!("  attempts:     {}", &task.attempts);
        println!("  max_attempts: {}", &task.max_attempts);
        println!(" cancellation_max_age:  {}", &task.cancellation_max_age);
        println!();
    }
    if tasks.is_empty() {
        println!("No tasks match those filtering options");
    }
    Ok(())
}
