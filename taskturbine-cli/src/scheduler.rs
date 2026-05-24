use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use clap::Args;
use serde::Deserialize;
use tokio::signal::unix::SignalKind;
use tokio::time;

use crate::CliError;
use taskturbine_core::{storage::Storage};

#[derive(Args, Debug)]
pub struct SchedulerArgs {
    #[arg(long, help = "Scheduler configuration file to use")]
    pub config: String,
}

/// Simple typed config DTO layer.
/// TODO implement serializer
#[derive(Deserialize)]
struct SchedulerConfig {
    pub schedules: HashMap<String, ScheduleEntry>
}
#[derive(Deserialize)]
struct ScheduleEntry {
    pub taskname: String,
    pub channel: String,
    pub schedule: ScheduleKind,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScheduleKind {
    Cron(String),
    Timedelta(String),
}


pub async fn scheduler(storage: Storage, args: SchedulerArgs) -> Result<(), CliError> {
    log::info!("Starting taskturbine-scheduler");

    let config_contents = match tokio::fs::read(&args.config).await {
        Ok(contents) => String::from_utf8(contents),
        Err(_) => return Err(CliError(format!("Could not read config file {}", &args.config))),
    };
    let Ok(config_string) = config_contents else {
        return Err(CliError("Could not convert config file data into a string".into()));
    };
    // let config_data = config_string.parse::<toml::Table>().expect("Could not parse toml from config file");
    let config: SchedulerConfig = toml::from_str(&config_string).expect("Could not parse configuration file.");

    log::info!("Configuration parsed. {} tasks loaded", config.schedules.len());

    run_scheduler_worker(storage, config).await;

    log::info!("Scheduler complete");
    Ok(())
}

pub async fn run_scheduler_worker(storage: Storage, config: SchedulerConfig) {
    tokio::spawn(run_scheduler(storage, config));

    // Should this be handled in main.rs?
    elegant_departure::tokio::depart()
        .on_termination()
        .on_signal(SignalKind::quit())
        .await
}

pub async fn run_scheduler(storage: Storage, config: SchedulerConfig) {
    log::debug!("Starting scheduler");
    // TODO should this have config? scheduler_poll_interval
    let mut timer = time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let guard = elegant_departure::get_shutdown_guard();

    let scheduler = Scheduler::new(storage, config);

    loop {
        tokio::select! {
            _ = timer.tick() => {
                let sleep_time = scheduler.tick().await;
                log::debug!("Completed scheduler tick. Should sleep for {sleep_time}");
            }
            _ = guard.wait() => {
                log::debug!("Shutting down upkeep");
                break;
            }
        }
    }
}

struct Scheduler {
    storage: Storage,
    config: SchedulerConfig,
    last_run: HashMap<String, DateTime<Utc>>
}

impl Scheduler {
    pub fn new(storage: Storage, config: SchedulerConfig) -> Self {
        Self { storage, config, last_run: HashMap::new() }
    }

    /// Return the number of seconds to sleep for.
    pub async fn tick(&self) -> i32 {
        let now = Utc::now();
        1
    }
}
