use clap::Args;

use crate::{
    CliError,
    admin_storage::{AdminStorage, TaskGetOptions},
};
use taskturbine_core::{
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct TaskGetArgs {
    /// The id of the task to get
    pub task_id: String,
}

/// Implement into/from to convert into the storage interface struct
impl From<TaskGetArgs> for TaskGetOptions {
    fn from(value: TaskGetArgs) -> Self {
        TaskGetOptions {
            task_id: value.task_id,
        }
    }
}

pub async fn execute(storage: Storage, args: TaskGetArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let options: TaskGetOptions = args.into();

    let details = admin_storage
        .task_get(options)
        .await
        .map_err(<StorageError as Into<CliError>>::into)?;

    let task = details.task;
    println!();
    println!("Task Id: {}", task.task_id);
    println!("  channel:    {}", task.channel);
    println!("  task_name:  {}", task.task_name);
    println!("  state:      {}", task.state);
    println!(
        "  headers:    {}",
        str::from_utf8(&task.headers).unwrap_or("<non-utf8 data>")
    );
    println!(
        "  parameters: {}",
        str::from_utf8(&task.params).unwrap_or("<non-utf8 data>")
    );
    println!(" Retry:");
    println!("  seconds:      {}", &task.retry_seconds);
    println!("  factor:       {}", &task.retry_factor);
    println!("  max_seconds:  {}", &task.retry_max_seconds);
    println!("  attempts:     {}", &task.attempts);
    println!("  max_attempts: {}", &task.max_attempts);
    println!(" cancellation_max_age:  {}", &task.cancellation_max_age);
    println!();

    println!("Runs:");
    println!();
    for run in details.runs.iter() {
        println!("Run Id: {}", run.run_id);
        println!(" attempt: {}", run.attempt);
        println!(" state: {}", run.state);
        println!(" claimed_by: {}", run.claimed_by);
        println!(" created at: {}", run.created_at);
    }
    println!();

    println!("Checkpoints:");
    println!();
    for checkpoint in details.checkpoints.iter() {
        println!("Checkpoint: {}", checkpoint.step_name);
        println!(" owner run: {}", checkpoint.owner_run_id);
        println!(" updated at: {}", checkpoint.updated_at);
    }

    Ok(())
}
