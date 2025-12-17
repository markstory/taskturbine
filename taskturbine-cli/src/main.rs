use clap::{Parser, Subcommand};

use taskturbine_core::api::Storage;
use taskturbine_core::config::Config;

mod clear;
mod demo;
mod emit_event;
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
    /// The database url to connect to. eg. postgres://user:pass@localhost/dbname
    #[arg(short, long)]
    database_url: Option<String>,

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
    /// Run a demo worker
    Demo,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

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
    let config = Config {
        database_url: db_url,
        database_log_queries: false,
        usecase: "demo".into(),
        worker_concurrency: 3,
        ..Config::default()
    };
    let storage = Storage::new(config);
    let result = match args.command {
        Commands::Spawn(args) => spawn::spawn_task(storage, args).await,
        Commands::Clear(args) => clear::clear_storage(storage, args).await,
        Commands::Demo => demo::demo(storage).await,
        Commands::EmitEvent(args) => emit_event::emit_event(storage, args).await,
    };
    if result.is_ok() {
        println!("Complete");
    } else if let Err(err) = result {
        match err {
            CliError::Message(msg) => {
                println!("Failed: {msg}");
            }
        }
    }
}
