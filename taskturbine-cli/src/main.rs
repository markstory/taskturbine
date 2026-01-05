use clap::{Parser, Subcommand};

use taskturbine_core::config::Config;
use taskturbine_core::storage::Storage;
use simple_logger::SimpleLogger;

mod cancel;
mod cleanup;
mod clear;
mod emit_event;
mod migrate;
mod spawn;

#[derive(Debug)]
enum CliError {
    Message(String),
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
    /// Spawn a new task.
    Spawn(spawn::SpawnArgs),
    /// Clears all data from storage.
    Clear(clear::ClearArgs),
    /// Emit an event to storage.
    EmitEvent(emit_event::EmitEventArgs),
    /// Run a cleanup worker
    Cleanup,
    /// Run migrations for the taskturbine schema.
    Migrate,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    SimpleLogger::new().init().unwrap();

    // Find the database url. Use both CLI options and environment variables.
    let db_url = match args.database_url {
        Some(db_url) => db_url,
        None => match std::env::var("TASKTURBINE_DATABASE_URL") {
            Ok(db_url) => db_url,
            Err(_) => {
                panic!("Could not determine database url from options or TASKTURBINE_DATABSE_URL")
            }
        },
    };

    // TODO it would be nice if taskturbine could provide command line tools
    // that consume a userland application. As spawn_task and worker commands
    // could be provided by the framework then.
    let config = Config {
        database_url: db_url,
        usecase: args.usecase,
        ..Config::default()
    };
    println!("Taskturbine CLI");
    println!("usecase: {}", config.usecase);

    let storage = Storage::new(config);
    let result = match args.command {
        Commands::Migrate => migrate::run_migrations(storage).await,
        Commands::Spawn(args) => spawn::spawn_task(storage, args).await,
        Commands::Clear(args) => clear::clear_storage(storage, args).await,
        Commands::Cleanup => cleanup::cleanup(storage).await,
        Commands::EmitEvent(args) => emit_event::emit_event(storage, args).await,
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
