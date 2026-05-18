use clap::Args;

use crate::{
    CliError,
    admin_storage::{AdminStorage, TaskGetOptions},
    formatters,
};
use taskturbine_core::{
    models::TaskId,
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct TaskGetArgs {
    /// The id of the task to get
    pub task_id: String,

    #[arg(
        long,
        default_value_t = false,
        help = "Enable to show result and state attributes as utf8 strings"
    )]
    pub show_results: bool,
}

/// Implement into/from to convert into the storage interface struct
impl TryFrom<TaskGetArgs> for TaskGetOptions {
    type Error = String;
    fn try_from(value: TaskGetArgs) -> Result<Self, Self::Error> {
        let task_id: TaskId = value
            .task_id
            .try_into()
            .map_err(|_| "Invalid task_id".to_string())?;

        Ok(TaskGetOptions { task_id })
    }
}

pub async fn execute(storage: Storage, args: TaskGetArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let show_results = args.show_results;
    let options: TaskGetOptions = args.try_into().map_err(CliError)?;

    let details = admin_storage
        .task_get(options.clone())
        .await
        .map_err(<StorageError as Into<CliError>>::into)?;

    println!();
    formatters::dump_task(&details.task);
    println!();

    println!("== Runs ==");
    println!();
    for run in details.runs.iter() {
        formatters::dump_run(run, show_results, false);
    }
    println!();

    println!("== Checkpoints ==");
    println!();
    for checkpoint in details.checkpoints.iter() {
        formatters::dump_checkpoint(checkpoint, show_results);
    }

    Ok(())
}
