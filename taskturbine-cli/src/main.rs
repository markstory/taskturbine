use clap::{Parser, Subcommand};

use simple_logger::SimpleLogger;
use taskturbine_core::config::Config;
use taskturbine_core::storage::{Storage, StorageError};

mod admin_storage;
mod cancel;
mod cleanup;
mod clear;
mod emit_event;
mod migrate;
mod spawn;
mod task_get;
mod task_list;
mod run_list;

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
    /// Cancels a sleeping or pending task
    Cancel(cancel::CancelArgs),
    /// Clears all data from storage.
    Clear(clear::ClearArgs),
    /// Run a cleanup worker
    Cleanup,
    /// Emit an event to storage.
    EmitEvent(emit_event::EmitEventArgs),
    /// Run migrations for the taskturbine schema.
    Migrate,
    /// Spawn a new task.
    Spawn(spawn::SpawnArgs),
    /// List tasks with filtering
    TaskList(task_list::TaskListArgs),
    /// Get a single task with filtering
    TaskGet(task_get::TaskGetArgs),
    /// Get a list of runs with filtering
    RunList(run_list::RunListArgs),
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
        Commands::Cancel(args) => cancel::cancel(storage, args).await,
        Commands::Cleanup => cleanup::cleanup(storage).await,
        Commands::Clear(args) => clear::clear_storage(storage, args).await,
        Commands::EmitEvent(args) => emit_event::emit_event(storage, args).await,
        Commands::Migrate => migrate::run_migrations(storage).await,
        Commands::Spawn(args) => spawn::spawn_task(storage, args).await,
        Commands::TaskList(args) => task_list::execute(storage, args).await,
        Commands::TaskGet(args) => task_get::execute(storage, args).await,
        Commands::RunList(args) => run_list::execute(storage, args).await,
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
