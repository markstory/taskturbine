use clap::Args;

use crate::{
    admin_storage::{AdminStorage, RunGetOptions}, formatters, CliError
};
use taskturbine_core::{
    models::RunId,
    storage::{Storage, StorageError},
};

#[derive(Args, Debug)]
pub struct RunGetArgs {
    /// The run id
    pub run_id: String,
}

/// Implement into/from to convert into the storage interface struct
impl TryFrom<RunGetArgs> for RunGetOptions {
    type Error = String;

    fn try_from(value: RunGetArgs) -> Result<Self, String> {
        let run_id: RunId = value.run_id
            .try_into()
            .map_err(|_| "Invalid run_id".to_string())?;
        Ok(RunGetOptions { run_id })
    }
}

pub async fn execute(storage: Storage, args: RunGetArgs) -> Result<(), CliError> {
    let admin_storage = AdminStorage::new(storage.get_config());
    let options: RunGetOptions = args.try_into().map_err(CliError::Message)?;

    let run_details = admin_storage
        .run_get(options)
        .await
        .map_err(<StorageError as Into<CliError>>::into)?;

    println!();
    formatters::dump_run(&run_details.run, true, true);
    println!();

    println!("== Checkpoints ==");
    println!();
    for checkpoint in run_details.checkpoints.iter() {
        formatters::dump_checkpoint(checkpoint, true);
    }
    println!();

    Ok(())
}
