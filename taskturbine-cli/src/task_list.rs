use clap::Args;

use crate::{
    CliError,
    admin_storage::{AdminStorage, TaskListOptions},
    formatters,
};
use taskturbine_core::{
    models::TaskState,
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct TaskListArgs {
    #[arg(long, help = "A regexp pattern to match task names against")]
    pub taskname: Option<String>,

    #[arg(long, help = "The task state value to filter by")]
    pub state: Option<TaskState>,

    #[arg(long, help = "The task channel")]
    pub channel: Option<String>,

    #[arg(long, default_value_t = 50, help = "The number of records to read")]
    pub limit: i32,
}

/// Implement into/from to convert into the storage interface struct
impl From<TaskListArgs> for TaskListOptions {
    fn from(value: TaskListArgs) -> Self {
        TaskListOptions {
            taskname: value.taskname,
            state: value.state,
            channel: value.channel,
            limit: value.limit,
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
        log::info!("No tasks match those filtering options");
    }
    Ok(())
}
