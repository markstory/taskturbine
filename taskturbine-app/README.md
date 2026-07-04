# Taskturbine

This packages contains the async rust client for taskturbine, a cross-platform durable task
framework.

## What are durable tasks?

Durable tasks are operations that are resilient to failure and interruptions. Instead of having
to manually manage retries, state and scheduling, you express your logic as a workflow of 
operations or functions. Each 'step' in a durable task will store its result, and retries will
resume from the last completed step.

See the [project homepage](https://github.com/markstory/taskturbine) for more background documentation.

## Installation

```
cargo add taskturbine
```

## Setup

To get started with taskturbine, you'll need to ensure you have a **postgres 14+** server, with
a database created. You'll need a DSN to your database, generally this is done via an environment
variable.


```rust
pub fn make_taskturbine_app() -> TaskturbineApp {
    let database_url =
        env::var("TASKTURBINE_DATABASE_URL").expect("Missing TASKTURBINE_DATABASE_URL in env");

    let task_config = Config {
        database_url,
        ..Config::default()
    };

    TaskturbineApp::new(task_config)
}
```
Taskturbine uses postgres tables to store all of its state and make it durable. On first use,
`taskturbine` will install its tables within the `taskturbine` schema in your application's
database.

## Defining Tasks

With our application created, we can register tasks that can be run. As your application gets larger,
you can create packages and modules of tasks:

```rust
/// Register all the user related tasks
pub fn register_user_tasks(app: TaskturbineApp) -> TaskturbineApp {
    app
      .register_task("sync-user", sync_user)
      .register_task("suspend-user", suspend_user)
}
```

Our task functions have to implement the `TaskHandler` trait. This trait is implicitly implemented for 
functions like:

```rust
use taskturbine::app::TaskResult;
use taskturbine::context::TaskContext;

pub async fn sync_user(mut ctx: TaskContext) -> TaskResult {
    Ok(None)
}
```

This task doesn't do much. Let's add a few steps:

```rust
use serde::{Deserialize, Serialize};
use taskturbine::app::TaskResult;
use taskturbine::context::TaskContext;

#[derive(sqlx::FromRow, Debug, PartialEq, Deserialize, Serialize)]
pub struct SyncUserParams {
    pub user_id i64;
}

pub async fn sync_user(mut ctx: TaskContext) -> TaskResult {
    let payload: SyncUserParams = serde_json::from_slice(ctx.param_bytes())?;
    
    // Define a step function
    async fn get_last_sync_state(ctx: TaskContext) -> TaskResult {
        // Load state and return it
        Ok(bytes)
    }
    // Run the step step. If this task already has a completed value for the step name,
    // The stored result will be used, and the function will *not* be called.
    let last_state = ctx.async_step("get-last-sync", get_last_sync_state).await?;
    
    async fn load_state(ctx: TaskContext) -> TaskResult {
        // Fetch new data from a remote resource
        // and return the response bytes
        Ok(bytes)
    }
    // This step will also only run successfully once.
    let new_state = ctx.async_step("load-new-state", load_state).await?
    
    // Steps can also be defined as closures
    let _ = ctx.async_step("mutate-user", async |ctx: TaskContext| -> TaskResult {
        // Update the row and persist it.
        Ok(vec![])
    }).await?
    Ok(None)
}
```

Defining tasks as a series of steps, lets taskturbine make your tasks resilient against blips in network, storage systems, and downstream system failures. When a step fails, it should return a `FlowControl` error to indicate what failed:

```rust
let _ = ctx.async_step("load-new-state", async |ctx: TaskContext| -> TaskResult {
    // If our read fails, we want this task run to fail, so we can retry.
    let res = backend_service::get_latest_state(payload.user_id)
        .await
        .map_err(|err| FlowControl::Failure("Failed to download state"))?;
    // Transform the result into data to be stored.
}).await?
````

As run attempts fail, the impacted tasks will be rescheduled to run again in the future.

## Spawning tasks

You can spawn tasks with a reference to `TaskturbineApp` or a `TaskContext`.

```rust
let res = app.spawn_task("task-name", b"{\"user_id\": 123}", None).await;
```

### Task Channels

Tasks can be spawned onto specific 'channels' of work. Channels let you separate workloads from each other and run multiple workers pools for your different workloads.

```rust
// Register a channel
let app = app.add_channel("reports");

// Spawn a task onto a defined channel
let res = app.channel("reports").spawn_task("process-revenue-daily", b"{}", None).await;
```

### Task Options

When spawning tasks, you can define `TaskOptions` to configure retries, timeouts and idempotency.

- `idempotency_key`: A unique identifier used to prevent duplicate tasks from
  being spawned. By providing an idempotency_key only one copy of a task can be
  scheduled at a time. The uniqueness constraint will end when the task is
  cleaned up after completion/failure.
- `headers`: Map of headers to include with the task activation
- `max_attempts`: The maximum number of attempts to make on this task
- `retry_seconds`: The minimum number of seconds to wait between retries.
- `retry_factor`: The multipier to apply to retry delays between attempts. Use > 1.0 to create exponential backoff.
- `retry_max_seconds`: The maximum number of seconds to wait between retries.
- `cancellation_max_age`: The maximum age of a task before it should not be run.
  Measured in seconds from when the task was created.

```rust
let options = TaskOptions {
    retry_seconds: 60,
    max_attempts: 10,
    ..TaskOptions::default()
};
let res = app.channel("reports").spawn_task("process-revenue-daily", b"{}", Some(options)).await;
```

## Running workers

Workers are a tokio based runtime that claims and executes tasks. Tasks are claimed in batches by workers, and then processed by one of the worker threads. While taskturbine provides the tools to build a worker, you'll need to define a worker binary entrypoint in your application code. First, add a new entry to `cargo.toml`.

```toml
[[bin]]
name = "worker"
path = "src/worker.rs"
```

Our `worker.rs` file should look like:

```rust
use simple_logger::SimpleLogger;
use taskturbine::app::run_worker;

use tasks::make_taskturbine_app;

#[tokio::main]
async fn main() {
    SimpleLogger::new().init().unwrap();

    log::info!("Starting worker");
    let app = make_taskturbine_app();

    let worker = app.create_worker("worker-1", vec![]);
    run_worker(worker).await;
}
```

Each worker can consume from one or more channels. When an empty channel list is provided, a worker consumes from all channels.


The `run_worker` function will run a worker loop until the process receives `SIGINT`, `SIGTERM` or `SIGKILL`. Each worker will run `config.worker_concurrency` number of threads to process tasks concurrently.

## Metrics and Logging

This package uses the `log` and `metrics` packages to record logs and metrics. You can configure a metrics/logging backend in your application before starting the worker.
