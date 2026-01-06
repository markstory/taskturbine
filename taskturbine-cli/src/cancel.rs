use clap::Args;
use uuid::Uuid;

use crate::CliError;
use taskturbine_core::{models::TaskId, storage::Storage};

#[derive(Args, Debug)]
pub struct CancelArgs {
    /// The task id to cancel
    pub task_id: Uuid,
}

pub async fn cancel(storage: Storage, args: CancelArgs) -> Result<(), CliError> {
    log::info!("Cancelling task {}", args.task_id);

    let task_id = TaskId(args.task_id);
    let res = storage.cancel_task(task_id).await;
    match res {
        Ok(_) => {
            log::info!("Task cancelled");
            Ok(())
        }
        Err(e) => {
            log::error!("Could not cancel: {:?}", e);
            Ok(())
        }
    }
}
