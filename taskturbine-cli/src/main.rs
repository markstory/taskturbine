use clap::{Parser, Subcommand};

use simple_logger::SimpleLogger;
use taskturbine_core::config::Config;
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
mod task_cancel;
mod task_get;
mod task_list;
mod task_spawn;
mod upkeep;

#[derive(Debug)]
enum CliError {
    Message(String),
}
impl From<StorageError> for CliError {
    fn from(value: StorageError) -> Self {
        let message = format!("Operation failed - StorageError\n{value:?}");
        CliError::Message(message)
    }
}

#[derive(Parser, Debug)]
#[command(name = "taskturbine-cli")]
#[command(version = "1.0")]
#[command(about = "Command line tools and interface for taskturbine")]
struct Cli {
    /// The database url to connect to. eg. postgres://user:pass@localhost/dbname.
    /// Will use `TASKTURBINE_DATABASE_URL` as a fallback.
    #[arg(short, long)]
    database_url: Option<String>,

    /// The usecase that is being operated on
    #[arg(short, long, default_value = "default")]
    usecase: String,

    /// Enable verbose/debug output
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
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
    UpkeepWorker,
    /// Run a retention cleanup on event data.
    CleanupEvent(cleanup_event::CleanupArgs),
    /// Run a retention cleanup on task, run and checkpoint data.
    CleanupTask,
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

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    SimpleLogger::new().init().unwrap();

    // Find the database url. Use both CLI options and environment variables.
    let db_url = args.database_url.unwrap_or_else(|| {
        std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Could not determine database url from options or TASKTURBINE_DATABASE_URL")
    });

    let config = Config {
        database_url: db_url,
        usecase: args.usecase,
        ..Config::default()
    };
    println!("Taskturbine CLI");
    println!("usecase: {}", config.usecase);

    let storage = Storage::new(config);
    let result = match args.command {
        Commands::CleanupEvent(args) => cleanup_event::execute(storage, args).await,
        Commands::CleanupTask => cleanup_task::execute(storage).await,
        Commands::Clear(args) => clear::clear_storage(storage, args).await,
        Commands::EmitEvent(args) => emit_event::emit_event(storage, args).await,
        Commands::Migrate => migrate::run_migrations(storage).await,
        Commands::ListRun(args) => run_list::execute(storage, args).await,
        Commands::GetRun(args) => run_get::execute(storage, args).await,
        Commands::CancelTask(args) => task_cancel::cancel(storage, args).await,
        Commands::GetTask(args) => task_get::execute(storage, args).await,
        Commands::ListTask(args) => task_list::execute(storage, args).await,
        Commands::SpawnTask(args) => task_spawn::spawn_task(storage, args).await,
        Commands::UpkeepWorker => upkeep::upkeep(storage).await,
    };
    if result.is_ok() {
        log::info!("Complete");
    } else if let Err(err) = result {
        match err {
            CliError::Message(msg) => {
                log::error!("Failed: {msg}");
            }
        }
    }
}
