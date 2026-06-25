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

Tasks are composed of several 'steps'. Each step represents a unit of work in the task. If a task fails, it will resume from the last successfully completed step.

- Show multi step task

## Spawning tasks

- Use app to spawn tasks.
- Use context to spawn tasks

## Running tasks

- Running a worker
- Running an upkeep worker
