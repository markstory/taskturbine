use clap::{Parser, Subcommand};

use taskturbine_core::api::Storage;

mod clear;
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
    #[arg(short, long)]
    database_url: Option<String>,

    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Spawn(spawn::SpawnArgs),
    Clear(clear::ClearArgs),
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    // Find the database url. Use both CLI options and environment variables.
    let db_url = match args.database_url {
        Some(db_url) => db_url,
        None => match std::env::var("TASKTURBINE_DATABASE_URL") {
            Ok(db_url) => db_url,
            Err(_) => panic!("Could not determine database url from options or TASKTURBINE_DATABSE_URL"),
        }
    };
    let config = taskturbine_core::config::Config {
        database_url: db_url,
    };
    let storage = Storage::new(config);
    let result = match args.command {
        Commands::Spawn(args) => spawn::spawn_task(storage, args).await,
        Commands::Clear(args) => clear::clear_storage(storage, args).await,
    };
    if let Ok(_) = result {
        println!("Complete");
    } else if let Err(err) = result {
        match err {
            CliError::Message(msg) => {
                println!("Failed: {msg}");
            },
        }
    }
}
