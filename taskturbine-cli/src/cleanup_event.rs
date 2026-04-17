use std::time::Duration;

use clap::Args;
use crate::CliError;
use chrono::Utc;
use taskturbine_core::storage::Storage;

#[derive(Args, Debug)]
pub struct CleanupArgs {
    #[arg(long, help = "The number of records to limit to.", default_value_t = 1000)]
    limit: i32,

    #[arg(
        long,
        help = "The number of seconds into history you wan to retain. Data older than this will be deleted.",
        default_value_t = 600
    )]
    cutoff_secs: i32,
}

pub async fn execute(storage: Storage, args: CleanupArgs) -> Result<(), CliError> {
    let cutoff = Duration::from_secs(args.cutoff_secs as u64);
    let older_than = Utc::now() - cutoff;
    let limit = args.limit;

    log::info!("Cleaning up event data up to {limit} records older than {older_than}");
    match storage.cleanup_events(older_than, limit).await {
        Ok(removed) => {
            log::info!("Cleanup complete. Removed {removed}");
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}
