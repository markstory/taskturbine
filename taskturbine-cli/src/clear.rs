use clap::Args;

use crate::CliError;
use taskturbine_core::api::Storage;

#[derive(Args, Debug)]
pub struct ClearArgs {
    #[arg(long, help = "Confirm that you want to clear all data")]
    pub execute: bool,
}

pub async fn clear_storage(storage: Storage, args: ClearArgs) -> Result<(), CliError> {
    println!("Clearing all tasks from the database");
    if args.execute {
        let res = storage.clear_storage().await;

        return match res {
            Ok(_) => Ok(()),
            Err(err) => Err(CliError::Message(format!("Failed to clear tasks {err:?}"))),
        };
    } else {
        println!("SKIP: You did not provide --execute to confirm execution.");

        Ok(())
    }
}
