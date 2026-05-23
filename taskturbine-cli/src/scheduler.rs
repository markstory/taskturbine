use clap::Args;
use uuid::Uuid;

use crate::CliError;
use taskturbine_core::{storage::Storage};

#[derive(Args, Debug)]
pub struct SchedulerArgs {
    #[arg(long, help = "Scheduler configuration file to use")]
    pub config: String,
}

pub async fn scheduler(storage: Storage, args: SchedulerArgs) -> Result<(), CliError> {
    log::info!("Starting taskturbine-scheduler");

    log::info!("Scheduler complete");
    Ok(())
}
