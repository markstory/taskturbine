use clap::Args;

use crate::{
    admin_storage::{AdminStorage, TaskGetOptions}, formatters, CliError
};
use taskturbine_core::storage::{Storage, StorageError};

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
impl From<TaskGetArgs> for TaskGetOptions {
    fn from(value: TaskGetArgs) -> Self {
        TaskGetOptions {
            task_id: value.task_id,
            show_results: value.show_results,
        }
    }
}

pub async fn execute(storage: Storage, args: TaskGetArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let options: TaskGetOptions = args.into();

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
        formatters::dump_run(run, options.show_results);
    }
    println!();

    println!("== Checkpoints ==");
    println!();
    for checkpoint in details.checkpoints.iter() {
        println!("Checkpoint: {}", checkpoint.step_name);
        println!(" owner run: {}", checkpoint.owner_run_id);
        println!(" updated at: {}", checkpoint.updated_at);
        if options.show_results {
            println!(
                " result: {}",
                str::from_utf8(checkpoint.state.as_slice()).unwrap_or(formatters::INVALID_DATA)
            );
        }
    }

    Ok(())
}
