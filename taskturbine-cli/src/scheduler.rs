use std::{
    collections::{BinaryHeap, HashMap},
    time::Duration,
};

use chrono::{DateTime, Utc};
use clap::Args;
use serde::Deserialize;
use tokio::signal::unix::SignalKind;
use tokio::time;

use crate::CliError;
use taskturbine_core::storage::Storage;

#[derive(Args, Debug)]
pub struct SchedulerArgs {
    #[arg(long, help = "Scheduler configuration file to use")]
    pub config: String,
}

/// Simple typed config DTO layer.
/// TODO implement serializer
#[derive(Deserialize)]
struct SchedulerConfig {
    pub schedules: HashMap<String, ScheduleEntry>,
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
    Timedelta(TimedeltaData),
}

#[derive(Deserialize)]
struct TimedeltaData {
    hours: Option<i32>,
    minutes: Option<i32>,
    seconds: Option<i32>,
}

pub async fn scheduler(storage: Storage, args: SchedulerArgs) -> Result<(), CliError> {
    log::info!("Starting taskturbine-scheduler");

    let config_contents = match tokio::fs::read(&args.config).await {
        Ok(contents) => String::from_utf8(contents),
        Err(_) => {
            return Err(CliError(format!(
                "Could not read config file {}",
                &args.config
            )));
        }
    };
    let Ok(config_string) = config_contents else {
        return Err(CliError(
            "Could not convert config file data into a string".into(),
        ));
    };
    // let config_data = config_string.parse::<toml::Table>().expect("Could not parse toml from config file");
    let config: SchedulerConfig =
        toml::from_str(&config_string).expect("Could not parse configuration file.");

    log::info!(
        "Configuration parsed. {} tasks loaded",
        config.schedules.len()
    );

    run_scheduler_worker(storage, config).await;

    log::info!("Scheduler complete");
    Ok(())
}

async fn run_scheduler_worker(storage: Storage, config: SchedulerConfig) {
    tokio::spawn(run_scheduler(storage, config));

    // Should this be handled in main.rs?
    elegant_departure::tokio::depart()
        .on_termination()
        .on_signal(SignalKind::quit())
        .await
}

async fn run_scheduler(storage: Storage, config: SchedulerConfig) {
    log::debug!("Starting scheduler");
    // TODO should this have config? scheduler_poll_interval
    let mut timer = time::interval(Duration::from_secs(1));
    timer.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let guard = elegant_departure::get_shutdown_guard();

    let now = Utc::now();
    let mut scheduler = Scheduler::new(storage, config, now);

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

trait Schedule {
    /// Is this schedule currently due? or past due based on the last_run.
    /// Schedules that are due, will have tasks spawned.
    fn is_due(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> bool;

    /// Get the number of seconds until the task is due again.
    fn remaining_seconds(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> i64;
}

struct TimedeltaSchedule {
    duration: Duration,
}
impl TimedeltaSchedule {
    fn new(schedule: &TimedeltaData) -> Self {
        let mut total_seconds = 0;
        if let Some(v) = schedule.seconds {
            total_seconds += v;
        }
        if let Some(v) = schedule.minutes {
            total_seconds += v * 60;
        }
        if let Some(v) = schedule.hours {
            total_seconds += v * 60 * 60;
        }
        let duration = Duration::from_secs(total_seconds as u64);
        Self {duration}
    }
}
impl Schedule for TimedeltaSchedule {
    /// Check if the delta between last_run and now is at least schedule seconds apart.
    fn is_due(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> bool {
        let remaining = self.remaining_seconds(now, last_run);
        remaining <= 0
    }

    /// Get the seconds remaining between last_run and now
    fn remaining_seconds(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> i64 {
        let gap = now - last_run;
        log::debug!("now {now}");
        log::debug!("last_run {last_run}");
        log::debug!("gap {gap}");
        (self.duration.as_secs() as i64) - gap.num_seconds()
    }
}

struct StorageEntry {
    key: String,
    taskname: String,
    channel: String,
    schedule: Box<dyn Schedule + Send>,
    pub last_run: DateTime<Utc>,
}
impl StorageEntry {
    fn new(key: &String, config_entry: &ScheduleEntry, last_run: DateTime<Utc>) -> Self {
        let schedule = match &config_entry.schedule {
            ScheduleKind::Cron(_value) => panic!("not done"),
            ScheduleKind::Timedelta(value) => TimedeltaSchedule::new(value),
        };
        Self {
            key: key.to_owned(),
            taskname: config_entry.taskname.clone(),
            channel: config_entry.channel.clone(),
            last_run,
            schedule: Box::new(schedule),
        }
    }

    fn is_due(&self, now: DateTime<Utc>) -> bool {
        self.schedule.is_due(now, self.last_run)
    }

    fn remaining_seconds(&self, now: DateTime<Utc>) -> i64 {
        self.schedule.remaining_seconds(now, self.last_run)
    }
}
impl Ord for StorageEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // TODO re-implement this with ordering by next runtime
        self.taskname.cmp(&other.taskname)
    }
}
impl PartialOrd for StorageEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for StorageEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.taskname == other.taskname && self.channel == other.channel
    }
}
impl Eq for StorageEntry {}

struct Scheduler {
    storage: Storage,
    entries: BinaryHeap<StorageEntry>,
}

impl Scheduler {
    // TODO this now parameter should become a map of storage entry : last_run state loaded from
    // the storage layer. First we'll need schema for that.
    pub fn new(storage: Storage, config: SchedulerConfig, now: DateTime<Utc>) -> Self {
        let mut entries = BinaryHeap::new();
        for (key, config_entry) in config.schedules.iter() {
            // TODO figure out if I need Reversed
            entries.push(StorageEntry::new(key, config_entry, now))
        }
        Self {
            storage,
            entries,
        }
    }

    /// Return the number of seconds to sleep for.
    pub async fn tick(&mut self) -> i64 {
        // look at the top of the heap
        let mut next_tick_at = 1;
        loop {
            let now = Utc::now();

            // This method takes a &mut, so it should be threadsafe
            let is_due = if let Some(entry) = self.entries.peek() {
                entry.is_due(now)
            } else {
                false
            };
            if !is_due {
                log::debug!("no tasks due now");
                break;
            }

            if let Some(mut entry) = self.entries.pop() {
                // Update last_run state.
                // TODO add options and params support
                let result = self.storage.spawn_task(&entry.channel, &entry.taskname, b"", None).await;
                match result {
                    Ok(spawn) => {
                        let task_id = spawn.task_id;
                        let run_id = spawn.run_id;
                        log::debug!("Spawned task_id={task_id} run_id={run_id}");

                        let now = Utc::now();
                        entry.last_run = now;
                    },
                    Err(err) => {
                        log::error!("Failed to spawn task. Error: {err:?}");
                    }
                }
                // Put the entry back into the heap where it can be sorted.
                self.entries.push(entry);
            } else {
                log::debug!("could not pop from self.entries");
                break;
            }
        }
        // Refresh time as spawning can be slow.
        let now = Utc::now();

        // We're done this tick update the sleep time until the next task is due.
        if let Some(entry) = self.entries.peek() {
            next_tick_at = entry.remaining_seconds(now);
        }
        next_tick_at
    }
}
