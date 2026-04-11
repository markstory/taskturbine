use clap::Args;

use crate::{
    admin_storage::{AdminStorage, TaskListOptions}, formatters, CliError
};
use taskturbine_core::{
    models::TaskState,
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct TaskListArgs {
    /// TODO make this a glob pattern
    #[arg(long, help = "A substring to match task names against")]
    pub taskname: Option<String>,

    #[arg(long, help = "The task state value to filter by")]
    pub state: Option<TaskState>,

    #[arg(long, help = "The task channel")]
    pub channel: Option<String>,
}

/// Implement into/from to convert into the storage interface struct
impl From<TaskListArgs> for TaskListOptions {
    fn from(value: TaskListArgs) -> Self {
        TaskListOptions {
            taskname: value.taskname,
            state: value.state,
            channel: value.channel,
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
        formatters::dump_task(task);
        println!();
    }
    if tasks.is_empty() {
        println!("No tasks match those filtering options");
    }
    Ok(())
}
