use std::collections::HashMap;

use clap::Args;

use crate::CliError;
use taskturbine_core::storage::{Storage, TaskOptions};

#[derive(Args, Clone, Debug)]
pub struct SpawnArgs {
    #[arg(
        short,
        long,
        help = "The channel to spawn a task into. The channel name is not validated/checked."
    )]
    channel: Option<String>,

    #[arg(short, long, help = "The name of the task to execute.")]
    taskname: String,

    #[arg(
        long,
        help = "A JSON encoded parameter set. Use `args` to provide a list of arguments."
    )]
    params: Option<String>,

    #[arg(long, help = "How many copies of the task you want.")]
    repeat: Option<i32>,

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

impl From<SpawnArgs> for TaskOptions {
    fn from(val: SpawnArgs) -> Self {
        let mut options = TaskOptions::default();
        if let Some(headers) = val.headers {
            options.headers =
                serde_json::from_str::<HashMap<String, String>>(&headers).unwrap_or_default();
        }
        if let Some(max_attempts) = val.max_attempts {
            options.max_attempts = max_attempts;
        }
        if let Some(retry_seconds) = val.retry_seconds {
            options.retry_seconds = retry_seconds;
        }
        if let Some(retry_factor) = val.retry_factor {
            options.retry_factor = retry_factor;
        }
        if let Some(cancellation_max_age) = val.cancellation_max_age {
            options.cancellation_max_age = cancellation_max_age;
        }

        options
    }
}

/// Spawn a task based on the command like parameters.
pub async fn spawn_task(storage: Storage, args: SpawnArgs) -> Result<(), CliError> {
    let taskname = args.taskname.clone();

    let channel_name = args
        .channel
        .clone()
        .unwrap_or(storage.get_config().default_channel);
    println!("Spawning task in channel={channel_name} for task={taskname}");

    let params = args.params.clone().unwrap_or("{\"args\":[]}".to_string());
    let mut results = vec![];
    let repeat = args.repeat;
    let options: TaskOptions = args.into();
    for _ in 1..repeat.unwrap_or(1) {
        let res = storage
            .spawn_task(
                &channel_name,
                &taskname,
                params.as_ref(),
                Some(options.clone()),
            )
            .await;
        results.push(res);
    }

    for item in results.iter() {
        match item {
            Ok(spawned) => {
                let run_id = spawned.run_id;
                let task_id = spawned.task_id;
                println!("Spawned task_id={task_id} run_id={run_id}");
            }
            Err(err) => return Err(CliError::Message(format!("Failed to spawn task {err:?}"))),
        }
    }

    Ok(())
}
