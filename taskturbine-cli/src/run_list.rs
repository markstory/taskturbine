use clap::Args;

use crate::{
    CliError,
    admin_storage::{AdminStorage, RunListOptions},
    formatters,
};
use taskturbine_core::{
    models::{TaskId, TaskState},
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct RunListArgs {
    #[arg(long, help = "The task to get runs for")]
    pub task_id: Option<String>,

    #[arg(long, help = "The task state value to filter by")]
    pub state: Option<TaskState>,

    #[arg(long, help = "The task channel")]
    pub channel: Option<String>,
}

/// Implement into/from to convert into the storage interface struct
impl TryFrom<RunListArgs> for RunListOptions {
    type Error = String;

    fn try_from(value: RunListArgs) -> Result<Self, String> {
        let task_id: Option<TaskId> = match value.task_id {
            Some(task_id) => task_id
                .try_into()
                .map(Some)
                .map_err(|_| "Invalid task_id".to_string())?,
            None => None,
        };
        Ok(RunListOptions { task_id })
    }
}

pub async fn execute(storage: Storage, args: RunListArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let options: RunListOptions = args.try_into().map_err(CliError::Message)?;

    let runs = admin_storage
        .run_list(options)
        .await
        .map_err(<StorageError as Into<CliError>>::into)?;

    for run in runs.iter() {
        formatters::dump_run(run, false, true);
        println!();
    }
    if runs.is_empty() {
        log::info!("No runs match those filtering options");
    }
    Ok(())
}
