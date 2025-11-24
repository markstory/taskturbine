use clap::{Args, Parser, Subcommand};

use taskturbine_core::api::{Storage, TaskOptions};

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
    Spawn(SpawnArgs),
    Clear,
}

#[derive(Args, Debug)]
struct SpawnArgs {
    #[arg(
        short,
        long,
        default_value = "default",
        help = "The namespace to spawn a task into."
    )]
    namespace: String,

    #[arg(short, long, help = "The name of the task to execute.")]
    taskname: String,

    #[arg(
        long,
        help = "A JSON encoded parameter set. Use `args` to provide a list of arguments."
    )]
    params: Option<String>,

    #[arg(
        long,
        help = "A JSON encoded map of headers. Key and Values should be strings."
    )]
    headers: Option<String>,

    #[arg(
        long,
        default_value = "3",
        help = "The maximum number of attempts that the task should have."
    )]
    max_attempts: Option<i32>,

    #[arg(
        long,
        default_value = "10",
        help = "The minimum number of seconds between retries."
    )]
    retry_seconds: Option<i32>,

    #[arg(
        long,
        default_value = "1.0",
        help = "The scaling multiplier for retries.
multiplier * retry_seconds = next retry delay."
    )]
    retry_factor: Option<f64>,

    #[arg(
        long,
        default_value = "900",
        help = "The max duration between a task's first attempt,
and the time after which it should be considered cancelled."
    )]
    cancellation_max_age: Option<i32>,
}

impl Into<TaskOptions> for SpawnArgs {
    fn into(self) -> TaskOptions {
        let mut options = TaskOptions::default();
        if let Some(headers) = self.headers {
            // TODO use serde
        }
        if let Some(max_attempts) = self.max_attempts {
            options.max_attempts = max_attempts;
        }
        if let Some(retry_seconds) = self.retry_seconds {
            options.retry_seconds = retry_seconds;
        }
        if let Some(retry_factor) = self.retry_factor {
            options.retry_factor = retry_factor;
        }
        if let Some(cancellation_max_age) = self.cancellation_max_age {
            options.cancellation_max_age = cancellation_max_age;
        }

        options
    }
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
        Commands::Spawn(args) => spawn_task(storage, args).await,
        Commands::Clear => clear_storage(storage).await,
    };
    if let Ok(_) = result {
        println!("Complete");
    } else {
        let err = result.err().unwrap();
        println!("Failed {err:?}");
    }
}

/// Spawn a task based on the command like parameters.
async fn spawn_task(storage: Storage, args: SpawnArgs) -> Result<(), CliError> {
    let taskname = args.taskname.clone();
    let namespace = args.namespace.clone();
    println!("Spawning task in namespace={namespace} for task={taskname}");

    let params = args.params.clone().unwrap_or("{\"args\":[]}".to_string());
    let res = storage.spawn_task(&namespace, &taskname, params.as_ref(), Some(args.into())).await;

    return match res {
        Ok(spawned) => {
            let run_id = spawned.run_id;
            let task_id = spawned.task_id;
            println!("Spawned task_id={task_id} run_id={run_id}");

            Ok(())
        },
        Err(err) => Err(CliError::Message(format!("Failed to spawn task {err:?}"))),
    }
}

async fn clear_storage(storage: Storage) -> Result<(), CliError> {
    println!("Clearing all tasks from the database");

    // TODO this is a big footgun.
    let res = storage.clear_storage().await;

    return match res {
        Ok(_) => Ok(()),
        Err(err) => Err(CliError::Message(format!("Failed to clear tasks {err:?}"))),
    }
}
