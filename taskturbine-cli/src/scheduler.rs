use std::{collections::HashMap, str::FromStr, time::Duration};

use chrono::{DateTime, Timelike, Utc};
use clap::Args;
use serde::Deserialize;
use tokio::signal::unix::SignalKind;

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
impl ScheduleEntry {
    /// Create a Schedule from the ScheduleKind data.
    fn make_schedule(&self) -> Result<Box<dyn Schedule + Send>, String> {
        match &self.schedule {
            ScheduleKind::Cron(value) => {
                let result = CronSchedule::new(value);
                match result {
                    Ok(schedule) => Ok(Box::new(schedule)),
                    Err(message) => {
                        let taskname = &self.taskname;
                        Err(format!(
                            "Invalid cron schedule found for {taskname}. Skipping this schedule. {value} is invalid: {message}"
                        ))
                    }
                }
            }
            ScheduleKind::Timedelta(value) => Ok(Box::new(TimedeltaSchedule::new(value))),
        }
    }
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

/// Command function for running a scheduler.
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
            "Could not read config file data into a string".into(),
        ));
    };
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
    let guard = elegant_departure::get_shutdown_guard();

    let now = Utc::now().with_nanosecond(0).unwrap();
    let mut scheduler = Scheduler::new(storage);
    for (key, config_entry) in config.schedules.iter() {
        let schedule = match config_entry.make_schedule() {
            Ok(schedule) => schedule,
            Err(message) => {
                log::error!("{}", message);
                continue;
            }
        };

        // TODO figure out if I need Reversed.
        let entry = StorageEntry::new(key, config_entry, now, schedule);
        scheduler.add(entry);
    }

    scheduler.sort_entries();

    loop {
        tokio::select! {
            sleep_time = scheduler.tick() => {
                let sleep_time = sleep_time.max(1);
                log::debug!("Completed scheduler tick. Will sleep for {sleep_time}");
                tokio::time::sleep(Duration::from_secs(sleep_time as u64)).await;
            },
            _ = guard.wait() => {
                log::info!("Shutting down scheduler");
                break;
            }
        }
    }
}

/// Abstract schedule interface
trait Schedule {
    /// Is this schedule currently due? or past due based on the last_run.
    /// Schedules that are due, will have tasks spawned.
    fn is_due(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> bool {
        let remaining = self.remaining_seconds(now, last_run);
        remaining <= 0
    }

    /// Get the number of seconds until the task is due again.
    fn remaining_seconds(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> i64;
}

/// Schedule that follows a timedelta of hours, minutes and seconds.
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
        Self { duration }
    }
}
impl Schedule for TimedeltaSchedule {
    /// Get the seconds remaining between last_run and now
    fn remaining_seconds(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> i64 {
        let gap = now - last_run;
        (self.duration.as_secs() as i64) - gap.num_seconds()
    }
}

/// Schedule that follows an expanded crontab schedule.
///
/// Schedules are defined as an expression of
///
/// sec   min   hour   day of month   month   day of week   year
struct CronSchedule {
    cron_schedule: cron::Schedule,
}
impl CronSchedule {
    fn new(schedule: &str) -> Result<Self, cron::error::Error> {
        let cron_schedule = cron::Schedule::from_str(schedule)?;
        Ok(Self { cron_schedule })
    }
}
impl Schedule for CronSchedule {
    /// Get the seconds remaining between last_run and now
    fn remaining_seconds(&self, now: DateTime<Utc>, last_run: DateTime<Utc>) -> i64 {
        let next = self.cron_schedule.after(&last_run).next();
        if let Some(next) = next {
            let gap = next - now;
            let seconds = gap.num_seconds();
            if seconds > 0 {
                return seconds;
            }
            // Less than 0
            return 0;
        }
        // There is no next schedule. Wait a second.
        1
    }
}

/// A single task schedule entry
///
/// Contains state for spawning the task, the schedule and last_run data.
struct StorageEntry {
    key: String,
    taskname: String,
    channel: String,
    schedule: Box<dyn Schedule + Send>,
    pub last_run: DateTime<Utc>,
}
impl StorageEntry {
    fn new(
        key: &str,
        config_entry: &ScheduleEntry,
        last_run: DateTime<Utc>,
        schedule: Box<dyn Schedule + Send>,
    ) -> Self {
        Self {
            key: key.to_owned(),
            taskname: config_entry.taskname.clone(),
            channel: config_entry.channel.clone(),
            last_run,
            schedule,
        }
    }

    fn is_due(&self, now: DateTime<Utc>) -> bool {
        self.schedule.is_due(now, self.last_run)
    }

    fn remaining_seconds(&self, now: DateTime<Utc>) -> i64 {
        self.schedule.remaining_seconds(now, self.last_run)
    }
}

/// The state machine for scheduled tasks
struct Scheduler {
    storage: Storage,
    /// Sorted vec of entries.
    entries: Vec<StorageEntry>,
}

impl Scheduler {
    // TODO read last_run information from storage.
    pub fn new(storage: Storage) -> Self {
        let entries = vec![];
        Self { storage, entries }
    }

    /// Add a ScheduleEntry to the scheduler.
    pub fn add(&mut self, entry: StorageEntry) {
        self.entries.push(entry)
    }

    /// Sort the entries vec based on time remaining for each schedule.
    fn sort_entries(&mut self) {
        let now = Utc::now().with_nanosecond(0).unwrap();
        self.entries.sort_by_key(|a| a.remaining_seconds(now));
    }

    /// Return the number of seconds to sleep for.
    pub async fn tick(&mut self) -> i64 {
        for entry in self.entries.iter_mut() {
            // Refresh now on each cycle in case spawning takes time.
            let now = Utc::now().with_nanosecond(0).unwrap();

            if !entry.is_due(now) {
                log::debug!("no more tasks due now");
                break;
            }

            let key = &entry.key;
            log::debug!("Schedule {key} is due");

            // TODO add options and params support
            let result = self
                .storage
                .spawn_task(&entry.channel, &entry.taskname, b"", None)
                .await;
            match result {
                Ok(spawn) => {
                    let task_id = spawn.task_id;
                    let run_id = spawn.run_id;
                    log::debug!("Spawned task_id={task_id} run_id={run_id}");

                    let now = Utc::now().with_nanosecond(0).unwrap();
                    entry.last_run = now;
                    log::debug!("Updating state of {key:?} to {now:?}");

                    // TODO Persist last_run state.
                }
                Err(err) => {
                    log::error!("Failed to spawn task. Error: {err:?}");
                }
            }
        }

        self.sort_entries();
        if let Some(entry) = self.entries.first() {
            let now = Utc::now().with_nanosecond(0).unwrap();
            return entry.remaining_seconds(now);
            // return (entry.remaining_seconds(now) - 1).max(0);
        }

        // If we didn't have a new entry sleep 1 second to conserve resources
        1
    }
}

#[cfg(test)]
mod tests {
    use chrono::Timelike;

    use super::*;

    #[test]
    fn timedelta_schedule_remaining_seconds() {
        let now = Utc::now();
        let last_run = now.with_minute(0).unwrap().with_second(0).unwrap();
        let due = now.with_minute(1).unwrap().with_second(30).unwrap();
        let not_due = now.with_minute(1).unwrap().with_second(20).unwrap();
        let very_early = now.with_minute(0).unwrap().with_second(20).unwrap();
        let the_past = last_run - Duration::from_secs(180);
        let the_future = last_run + Duration::from_secs(180);

        let schedule = TimedeltaSchedule::new(&TimedeltaData {
            hours: None,
            minutes: Some(1),
            seconds: Some(30),
        });
        assert_eq!(schedule.remaining_seconds(due, last_run), 0);
        assert_eq!(schedule.remaining_seconds(not_due, last_run), 10);
        assert_eq!(schedule.remaining_seconds(very_early, last_run), 70);
        assert_eq!(
            schedule.remaining_seconds(the_past, last_run),
            270,
            "handles full cycles"
        );
        assert_eq!(
            schedule.remaining_seconds(the_future, last_run),
            -90,
            "negative value when overdue"
        );
    }

    #[test]
    fn timedelta_schedule_remaining_seconds_minutes_and_hours() {
        let last_run = "2026-05-30 12:05:30Z".parse::<DateTime<Utc>>().unwrap();
        let after_last_run = last_run + Duration::from_secs(1);
        let before_next = "2026-05-30 13:08:30Z".parse::<DateTime<Utc>>().unwrap();

        let schedule = TimedeltaSchedule::new(&TimedeltaData {
            hours: Some(1),
            minutes: Some(5),
            seconds: Some(30),
        });
        assert_eq!(
            schedule.remaining_seconds(after_last_run, last_run),
            3600 + 300 + 29
        );
        assert_eq!(schedule.remaining_seconds(before_next, last_run), 150);
    }

    #[test]
    fn timedelta_schedule_is_due() {
        let now = Utc::now();
        let last_run = now.with_minute(0).unwrap().with_second(0).unwrap();
        let due = now.with_minute(1).unwrap().with_second(30).unwrap();
        let not_due = now.with_minute(1).unwrap().with_second(20).unwrap();
        let very_early = now.with_minute(0).unwrap().with_second(20).unwrap();
        let the_past = last_run - Duration::from_secs(180);
        let the_future = last_run + Duration::from_secs(180);

        let schedule = TimedeltaSchedule::new(&TimedeltaData {
            hours: None,
            minutes: Some(1),
            seconds: Some(30),
        });
        assert!(schedule.is_due(due, last_run));
        assert!(!schedule.is_due(not_due, last_run));
        assert!(!schedule.is_due(very_early, last_run));
        assert!(!schedule.is_due(the_past, last_run), "handles full cycles");
        assert!(
            schedule.is_due(the_future, last_run),
            "negative value when overdue"
        );
    }

    #[test]
    fn cron_schedule_remaining_seconds() {
        let now = Utc::now();
        let last_run = now.with_minute(0).unwrap().with_second(0).unwrap();
        let due = now.with_minute(1).unwrap().with_second(0).unwrap();
        let not_due = now.with_minute(0).unwrap().with_second(50).unwrap();
        let very_early = now.with_minute(0).unwrap().with_second(20).unwrap();
        let the_past = last_run - Duration::from_secs(180);
        let the_future = last_run + Duration::from_secs(180);

        let schedule = CronSchedule::new("0 */1 * * * *").unwrap();
        assert_eq!(schedule.remaining_seconds(due, last_run), 0);
        assert_eq!(schedule.remaining_seconds(not_due, last_run), 9);
        assert_eq!(schedule.remaining_seconds(very_early, last_run), 39);
        assert_eq!(
            schedule.remaining_seconds(the_past, last_run),
            239,
            "handles full cycles"
        );
        assert_eq!(
            schedule.remaining_seconds(the_future, last_run),
            0,
            "0 when overdue"
        );
    }

    #[test]
    fn cron_schedule_is_due() {
        let now = Utc::now();
        let last_run = now.with_minute(0).unwrap().with_second(0).unwrap();
        let due = now.with_minute(1).unwrap().with_second(0).unwrap();
        let not_due = now.with_minute(0).unwrap().with_second(50).unwrap();
        let very_early = now.with_minute(0).unwrap().with_second(50).unwrap();
        let the_past = last_run - Duration::from_secs(180);
        let the_future = last_run + Duration::from_secs(180);

        let schedule = CronSchedule::new("0 */1 * * * * *").unwrap();
        assert!(schedule.is_due(due, last_run));
        assert!(!schedule.is_due(not_due, last_run));
        assert!(!schedule.is_due(very_early, last_run));
        assert!(!schedule.is_due(the_past, last_run), "handles full cycles");
        assert!(
            schedule.is_due(the_future, last_run),
            "negative value when overdue"
        );
    }

    #[test]
    fn storage_entry_remaining_seconds_and_is_due() {
        let config = ScheduleEntry {
            taskname: "update-data".to_owned(),
            channel: "default".to_owned(),
            schedule: ScheduleKind::Cron("0 */5 * * * * *".to_owned()),
        };
        let now = "2026-05-30 12:00:00Z".parse::<DateTime<Utc>>().unwrap();

        let schedule = config.make_schedule().unwrap();
        let entry = StorageEntry::new("update-data", &config, now, schedule);
        assert_eq!(entry.taskname, "update-data");
        assert_eq!(entry.channel, "default");

        let next_time = "2026-05-30 12:05:00Z".parse::<DateTime<Utc>>().unwrap();
        assert!(entry.is_due(next_time));
        assert_eq!(entry.remaining_seconds(next_time), 0);

        let before_next = "2026-05-30 12:03:00Z".parse::<DateTime<Utc>>().unwrap();
        assert!(!entry.is_due(before_next));
        assert_eq!(entry.remaining_seconds(before_next), 120);
    }
}
