use clap::Args;

use crate::{CliError, admin_storage::AdminStorage};
use taskturbine_core::{
    models::TaskState,
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct TaskListArgs {
    /// The task state to filter by
    pub state: Option<TaskState>,
}

pub async fn execute(storage: Storage, _args: TaskListArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let tasks = admin_storage
        .task_list()
        .await
        .map_err(<StorageError as Into<CliError>>::into)?;

    for task in tasks.iter() {
        println!("Task Id: {}", task.task_id);
        println!("  usecase:    {}", task.usecase);
        println!("  channel:    {}", task.channel);
        println!("  task_name:  {}", task.task_name);
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
    Ok(())
}
