use std::fmt::Display;

use clap::{Parser, Subcommand};
use colored::Colorize;
use figment::{Figment, providers::{Env, Format, Serialized, Toml}};
use serde::{Deserialize, Serialize};

use taskturbine::config::Config;
use taskturbine_core::storage::{Storage, StorageError};

mod admin_storage;
mod cleanup_event;
mod cleanup_task;
mod clear;
mod emit_event;
mod formatters;
mod migrate;
mod run_get;
mod run_list;
mod scheduler;
mod task_cancel;
mod task_get;
mod task_list;
mod task_spawn;
mod upkeep;

#[derive(Debug)]
struct CliError(String);

impl From<StorageError> for CliError {
    fn from(value: StorageError) -> Self {
        let message = format!("Operation failed - StorageError\n{value:?}");
        CliError(message)
    }
}

impl Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Parser, Debug)]
#[command(name = "taskturbine-cli")]
#[command(version = "1.0")]
#[command(about = "Command line tools and interface for taskturbine")]
struct Cli {
    /// The database url to connect to. eg. postgres://user:pass@localhost/dbname.
    #[arg(long)]
    database_url: Option<String>,

    /// The usecase that is being operated on
    #[arg(long)]
    usecase: Option<String>,

    /// Enable verbose/debug output
    #[arg(short, long)]
    verbose: bool,

    /// Read configuration from a TOML file.
    /// Config file options override environment variables.
    #[arg(short, long)]
    config: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Serialize, Deserialize)]
struct CliConfig {
    database_url: Option<String>,
    usecase: Option<String>,
    config: Option<String>,
}

impl From<&Cli> for CliConfig {
    fn from(value: &Cli) -> Self {
        CliConfig {
            database_url: value.database_url.clone(),
            usecase: value.usecase.clone(),
            config: value.config.clone(),
        }
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Clears all data from storage.
    Clear(clear::ClearArgs),
    /// Run a upkeep worker
    ///
    /// Perform periodic upkeep operations on all channels in a usecase.
    /// Upkeep operations include the following:
    ///
    /// * Release expired claims, and fail the run that expired.
    ///
    /// * Cancel tasks that are past their cancellation_max_age
    ///
    /// The worker will continue to run even if there are postgres
    /// connection errors. The hope is for any infrastructure/configuration
    /// issues to be resolved and the application is restarted.
    UpkeepWorker,
    /// Run a scheduler that spawns tasks on periodic schedules
    ///
    /// Will spawn tasks periodically based on the provided configuration file.
    ///
    /// See the README for an example configuration file.
    ///
    Scheduler(scheduler::SchedulerArgs),
    /// Run a retention cleanup on event data.
    CleanupEvent(cleanup_event::CleanupEventArgs),
    /// Run a retention cleanup on task, run and checkpoint data.
    CleanupTask(cleanup_task::CleanupTaskArgs),
    /// Emit an event to storage.
    EmitEvent(emit_event::EmitEventArgs),
    /// Run migrations for the taskturbine schema.
    Migrate,
    /// Get a list of runs with filtering
    ListRun(run_list::RunListArgs),
    /// List tasks with filtering
    ListTask(task_list::TaskListArgs),
    /// Get the details for a run
    GetRun(run_get::RunGetArgs),
    /// Get a single task with filtering
    GetTask(task_get::TaskGetArgs),
    /// Spawn a new task.
    SpawnTask(task_spawn::SpawnArgs),
    /// Cancels a sleeping or pending task
    CancelTask(task_cancel::CancelArgs),
}

/// Extract configuration from the following sources:
/// - Environment variables.
/// - The file provided through --config
/// - CLI args
fn create_config(args: CliConfig) -> Result<Config, CliError> {
    let mut builder = Figment::from(Config::default())
        .merge(Env::prefixed("TASKTURBINE_"));

    if let Some(config_file) = args.config {
        builder = builder.merge(Toml::file(config_file));
    }
    if let Some(db_url) = args.database_url {
        builder = builder.merge(("database_url", db_url));
    }
    if let Some(usecase) = args.usecase {
        builder = builder.merge(("usecase", usecase));
    }

    let config: Config = builder.extract().map_err(|err| CliError(format!("Failed to build config: {err:?}")))?;
    Ok(config)
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    let args = Cli::parse();
    let log_level = if args.verbose {
        log::Level::Debug
    } else {
        log::Level::Info
    };
    simple_logger::init_with_level(log_level).unwrap();
    let config = create_config((&args).into())?;

    println!("{}", "Taskturbine CLI".blue());
    println!(
        "{}: {}",
        "usecase".blue().bold(),
        config.usecase.bright_blue()
    );

    let storage = Storage::new(config.clone().into());
    let result = match args.command {
        Commands::CleanupEvent(args) => cleanup_event::execute(storage, args).await,
        Commands::CleanupTask(args) => cleanup_task::execute(storage, args).await,
        Commands::Clear(args) => clear::clear_storage(storage, args).await,
        Commands::EmitEvent(args) => emit_event::emit_event(storage, args).await,
        Commands::Migrate => migrate::run_migrations(storage).await,
        Commands::ListRun(args) => run_list::execute(storage, args).await,
        Commands::GetRun(args) => run_get::execute(storage, args).await,
        Commands::CancelTask(args) => task_cancel::cancel(storage, args).await,
        Commands::GetTask(args) => task_get::execute(storage, args).await,
        Commands::ListTask(args) => task_list::execute(storage, args).await,
        Commands::Scheduler(args) => scheduler::scheduler(storage, args).await,
        Commands::SpawnTask(args) => task_spawn::spawn_task(storage, config, args).await,
        Commands::UpkeepWorker => upkeep::upkeep(storage, config).await,
    };

    match result {
        Ok(_) => {
            log::info!("Complete");
            Ok(())
        }
        Err(CliError(msg)) => {
            log::error!("Failed: {msg}");
            Err(CliError("Command Failed!".to_string()))
        }
    }
}
