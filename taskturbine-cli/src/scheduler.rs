use core::fmt;
use std::{collections::HashMap, str::FromStr, time::Duration};

use chrono::{DateTime, Timelike, Utc};
use clap::Args;
use serde::Deserialize;
use tokio::signal::unix::SignalKind;

use crate::CliError;
use taskturbine_core::storage::{Storage, TaskOptions};

#[derive(Args, Debug)]
pub struct SchedulerArgs {
    #[arg(long, help = "Scheduler configuration file to use")]
    pub config: String,
}

/// Simple typed config DTO layer.
#[derive(Deserialize)]
struct SchedulerConfig {
    pub schedules: HashMap<String, ScheduleEntry>,
}
#[derive(Deserialize)]
struct ScheduleEntry {
    pub taskname: String,
    pub channel: String,
    pub schedule: ScheduleKind,
    pub params: Option<Vec<u8>>,
    pub options: Option<ScheduleOptions>,
}
impl ScheduleEntry {
    /// Create a Schedule from the ScheduleKind data.
    fn make_schedule(&self) -> Result<Box<dyn Schedule + Send + Sync>, String> {
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

/// Schedule configuration version of TaskOptions
#[derive(Deserialize)]
struct ScheduleOptions {
    /// Map of headers to include with the task activation
    pub headers: HashMap<String, String>,

    /// The maximum number of attempts to make on this task
    pub max_attempts: i32,

    /// The minimum number of seconds to wait between retries.
    pub retry_seconds: i32,

    /// The multipier to apply to retry delays between attempts.
    /// Use > 1.0 to create exponential backoff.
    pub retry_factor: f64,

    /// The maximum number of seconds to wait between retries.
    pub retry_max_seconds: i32,

    /// The maximum age of a task before it should not be run.
    /// Measured in seconds from when the task was created.
    pub cancellation_max_age: i32,
}
impl From<&ScheduleOptions> for TaskOptions {
    fn from(value: &ScheduleOptions) -> Self {
        let mut options = TaskOptions::default();
        options.headers = value.headers.clone();
        options.max_attempts = value.max_attempts;
        options.retry_seconds = value.retry_seconds;
        options.retry_factor = value.retry_factor;
        options.retry_max_seconds = value.retry_max_seconds;
        options.cancellation_max_age = value.cancellation_max_age;
        options
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

    let state = storage.get_scheduler_last_run().await.unwrap();
    log::debug!("Loaded scheduler state");

    let mut scheduler = Scheduler::new(storage);
    let now = Utc::now().with_nanosecond(0).unwrap();

    for (key, config_entry) in config.schedules.iter() {
        let schedule = match config_entry.make_schedule() {
            Ok(schedule) => schedule,
            Err(message) => {
                log::error!("{}", message);
                continue;
            }
        };
        let mut entry = StorageEntry::new(key, config_entry, now, schedule);
        if let Some(last_run) = state.get(&entry.storage_key()) {
            entry.last_run = *last_run;
        }
        scheduler.add(entry);
    }

    scheduler.sort_entries();

    loop {
        let now = Utc::now().with_nanosecond(0).unwrap();

        tokio::select! {
            sleep_time = scheduler.tick(now) => {
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
trait Schedule: fmt::Display {
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
impl fmt::Display for TimedeltaSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let seconds = self.duration.as_secs();
        f.write_str(format!("td:{seconds}").as_ref())
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
impl fmt::Display for CronSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let schedule = &self.cron_schedule;
        f.write_str(format!("c:{schedule}").as_ref())
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
    params: Option<Vec<u8>>,
    options: Option<TaskOptions>,
    schedule: Box<dyn Schedule + Send + Sync>,
    last_run: DateTime<Utc>,
}
impl StorageEntry {
    fn new(
        key: &str,
        config_entry: &ScheduleEntry,
        last_run: DateTime<Utc>,
        schedule: Box<dyn Schedule + Send + Sync>,
    ) -> Self {
        let options = config_entry.options.as_ref().map(|v| v.into());

        Self {
            key: key.to_owned(),
            taskname: config_entry.taskname.clone(),
            channel: config_entry.channel.clone(),
            params: config_entry.params.clone(),
            last_run,
            schedule,
            options,
        }
    }

    fn is_due(&self, now: DateTime<Utc>) -> bool {
        self.schedule.is_due(now, self.last_run)
    }

    fn remaining_seconds(&self, now: DateTime<Utc>) -> i64 {
        self.schedule.remaining_seconds(now, self.last_run)
    }

    /// Specific key to storage the run state of this entry by.
    /// Depends on the schedule key, taskname, and schedule.
    fn storage_key(&self) -> String {
        format!("{}:{}:{}", self.key, self.taskname, self.schedule)
    }
}

/// The state machine for scheduled tasks
struct Scheduler {
    storage: Storage,
    /// Sorted vec of entries.
    entries: Vec<StorageEntry>,
}

impl Scheduler {
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

    /// Run a 'tick' of the scheduler loop.
    ///
    /// A tick is a fixed point in time. Generally values increase as time advances. While the
    /// intent is to tick each second, spawning tasks can take time and seconds may be 'lost'. The
    /// scheduler will catch up by whenever possible. However, if multiple intervals are missed, those
    /// interval will be skipped and the schedule will resume on its next tick time (or after).
    ///
    /// This is a tradeoff between every spawning being important, and being on schedule as much as
    /// possible. By favouring being on schedule, we skip missed intervals. If intervals are being
    /// missed, your scheduler may be overwhelmed. Consider splitting up your schedule configuration
    /// and running multiple schedulers.
    ///
    /// Returns the number of seconds to sleep for.
    pub async fn tick(&mut self, now: DateTime<Utc>) -> i64 {
        for entry in self.entries.iter_mut() {
            if !entry.is_due(now) {
                log::debug!("no more tasks due now");
                break;
            }

            let key = &entry.key;
            log::debug!("Schedule {key} is due");

            let params = if let Some(params) = &entry.params {
                params.as_slice()
            } else {
                b""
            };
            let result = self
                .storage
                .spawn_task(
                    &entry.channel,
                    &entry.taskname,
                    params,
                    entry.options.clone(),
                )
                .await;
            match result {
                Ok(spawn) => {
                    let task_id = spawn.task_id;
                    let run_id = spawn.run_id;
                    log::debug!("Spawned task_id={task_id} run_id={run_id}");

                    entry.last_run = now;
                    let _ = self
                        .storage
                        .set_scheduler_last_run(entry.storage_key().as_ref(), entry.last_run)
                        .await;
                    log::debug!("Updated state of {key:?} to {now:?}");
                }
                Err(err) => {
                    log::error!("Failed to spawn task. Error: {err:?}");
                }
            }
        }
        // Prepare for the next tick by sorting the entries putting the
        // entry with the least time remaining at the front.
        self.sort_entries();

        if let Some(entry) = self.entries.first() {
            return entry.remaining_seconds(now);
        }

        // If we didn't have a new entry sleep 1 second to conserve resources
        1
    }
}

#[cfg(test)]
mod tests {
    use chrono::Timelike;
    use taskturbine_core::testutils::create_storage;

    use super::*;

    mod scheduler {
        use taskturbine_core::models::Task;

        use crate::admin_storage::{AdminStorage, TaskListOptions};

        use super::*;

        fn create_schedule_entry(name: &str, taskname: &str, start: DateTime<Utc>, schedule: ScheduleKind) -> StorageEntry {
            let schedule_config = ScheduleEntry {
                taskname: taskname.to_owned(),
                channel: "default".to_owned(),
                schedule: schedule,
                params: None,
                options: None,
            };
            let schedule = schedule_config.make_schedule().unwrap();
            let entry = StorageEntry::new(name, &schedule_config, start, schedule);
            entry
        }

        async fn find_tasks_by_name(admin: &AdminStorage, name: &str) -> Vec<Task> {
            let options = TaskListOptions {
                channel: Some("default".to_owned()),
                taskname: Some(name.to_owned()),
                state: None,
                limit: 5,
            };
            admin.task_list(options).await.expect("Should find something")
        }

        #[tokio::test]
        async fn scheduler_tick_one_task_simple() {
            let start = "2026-05-30 12:00:00Z".parse::<DateTime<Utc>>().unwrap();
            let storage = create_storage().await;

            let usecase = storage.get_config().usecase;
            let five_seconds = ScheduleKind::Cron("0 */5 * * * * *".to_owned());
            let taskname = format!("tick-one-task-simple-{usecase}");
            let entry = create_schedule_entry("tick-one-task", &taskname, start, five_seconds);

            let config = storage.get_config();
            let mut scheduler = Scheduler::new(storage.clone());
            scheduler.add(entry);

            let now = "2026-05-30 12:05:01Z".parse::<DateTime<Utc>>().unwrap();
            let sleep = scheduler.tick(now).await;
            assert!(sleep > 2, "Should always sleep at least 2 out of 5");

            let admin = AdminStorage::new(config.clone());
            let tasks = find_tasks_by_name(&admin, &taskname).await;
            assert!(tasks.len() >= 1, "At least one task spawned");
        }

        #[tokio::test]
        async fn scheduler_tick_one_task_catch_up() {
            let start = "2026-05-30 12:00:00Z".parse::<DateTime<Utc>>().unwrap();
            let storage = create_storage().await;

            let usecase = storage.get_config().usecase;
            let five_seconds = ScheduleKind::Cron("0 */5 * * * * *".to_owned());
            let taskname = format!("tick-one-task-catch-up-{usecase}");
            let entry = create_schedule_entry("tick-one", &taskname, start, five_seconds);

            let config = storage.get_config();
            let mut scheduler = Scheduler::new(storage.clone());
            scheduler.add(entry);

            // Late by 2 minutes
            let late = "2026-05-30 12:07:01Z".parse::<DateTime<Utc>>().unwrap();
            let sleep = scheduler.tick(late).await;
            assert!(sleep > 2, "Should always sleep at least 2 out of 5");

            let admin = AdminStorage::new(config.clone());
            let tasks = find_tasks_by_name(&admin, &taskname).await;
            assert!(tasks.len() >= 1, "At least one task spawned");
        }

        #[tokio::test]
        async fn scheduler_tick_multiple_task_trigger_all_in_batch() {
            let start = "2026-05-30 12:00:00Z".parse::<DateTime<Utc>>().unwrap();
            let storage = create_storage().await;

            let config = storage.get_config();
            let usecase = &config.usecase;
            let mut scheduler = Scheduler::new(storage.clone());

            let five_min = ScheduleKind::Cron("0 */5 * * * * *".to_owned());
            let taskname = format!("tick-multiple-task-trigger-all-{usecase}");
            let first = create_schedule_entry("first-task", &taskname, start, five_min);
            scheduler.add(first);

            let six_min = ScheduleKind::Cron("0 */6 * * * * *".to_owned());
            let second_name = format!("tick-multiple-task-trigger-all-second-{usecase}");
            let second = create_schedule_entry("second-task", &second_name, start, six_min);
            scheduler.add(second);

            // Late by 2 minutes and 1 minute
            let late = "2026-05-30 12:07:01Z".parse::<DateTime<Utc>>().unwrap();
            let sleep = scheduler.tick(late).await;
            assert!(sleep > 2, "Should always sleep at least 2 out of 5");

            let admin = AdminStorage::new(config.clone());
            let tasks = find_tasks_by_name(&admin, &taskname).await;
            assert!(tasks.len() >= 1, "At least one task spawned");

            let tasks = find_tasks_by_name(&admin, &second_name).await;
            assert!(tasks.len() >= 1, "At least one task spawned");
        }
    }

    mod timedelta_schedule {
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
    }

    mod cron_schedule {
        use super::*;

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
    }

    mod storage_entry {
        use super::*;

        #[test]
        fn storage_entry_remaining_seconds_and_is_due() {
            let config = ScheduleEntry {
                taskname: "update-data".to_owned(),
                channel: "default".to_owned(),
                schedule: ScheduleKind::Cron("0 */5 * * * * *".to_owned()),
                params: None,
                options: None,
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

        #[test]
        fn storage_entry_storage_key() {
            let cron_config = ScheduleEntry {
                taskname: "do_update_data".to_owned(),
                channel: "default".to_owned(),
                schedule: ScheduleKind::Cron("0 */5 * * * * *".to_owned()),
                params: None,
                options: None,
            };
            let now = "2026-05-30 12:00:00Z".parse::<DateTime<Utc>>().unwrap();

            let schedule = cron_config.make_schedule().unwrap();
            let entry = StorageEntry::new("update-data", &cron_config, now, schedule);
            assert_eq!(
                entry.storage_key(),
                "update-data:do_update_data:c:0 */5 * * * * *"
            );

            let td_config = ScheduleEntry {
                taskname: "do_update_data".to_owned(),
                channel: "default".to_owned(),
                schedule: ScheduleKind::Timedelta(TimedeltaData {
                    hours: None,
                    minutes: None,
                    seconds: Some(30),
                }),
                params: None,
                options: None,
            };
            let now = "2026-05-30 12:00:00Z".parse::<DateTime<Utc>>().unwrap();

            let schedule = td_config.make_schedule().unwrap();
            let entry = StorageEntry::new("timedelta-update", &cron_config, now, schedule);
            assert_eq!(entry.storage_key(), "timedelta-update:do_update_data:td:30");
        }
    }
}
