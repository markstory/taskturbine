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
    // TODO continue here. Format the list nicely. See if there are
    // text UI libraries I could be using to make this look good with less effort.
    println!("{tasks:?}");
    Ok(())
}
