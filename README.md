# Taskturbine

Taskturbine is a cross-language durable task framework for Rust & Python, using postgres for task storage.

## What are durable tasks?

Durable tasks are operations that are resilient to failure and interruptions. Instead of having
to manually manage retries, state and scheduling, you express your logic as a workflow of 
operations or functions. Each 'step' in a durable task will store its result, and will
resume from the last completed step.

```python
import os
from taskturbine import TaskturbineApp, Config, TaskContext

config = Config(
    database_url=os.getenv("DB_URL"),
    app_module="testapp:app",
    worker_concurrency=4,
)
app = TaskturbineApp(config)


@app.register_task("hello-world")
def hello_world(ctx: TaskContext) -> None:
    logger.info(f"Hello world! {ctx.params_bytes.decode()}")

    # We start off defining the steps in our task.
    @ctx.step(name="compute_user_data")
    def compute_user_data(ctx: TaskContext) -> Payload:
        params = ctx.params
        # do some compute/query and returns a dict value.
        return {"name": params["name"], "id": uuid.uuid4().hex, "started": time.time()}
        
    @ctx.step(name="send-email")
    def send_email(ctx: TaskContext, user: Payload, event: Payload) -> Payload:
        email.send_message("hello-user", data={"user": user, "event": event})

    @ctx.step(name="process-complete")
    def process_complete(ctx: TaskContext, user: Payload, event: Payload) -> Payload:
        # Do more IO
        return user

    # Once we have defined our steps, we wire them together with control flow.
    # Steps have their return values stored and if a step/task fails, it will
    # resume from the last completed step.
    # Any logic in the task body will be run *each* time the task is executed.
    user = compute_user_data(ctx)
    assert user, "User must exist"
    send_email(ctx, user)

    completed = process_complete(ctx, user, event)

    print("Completed:", completed)
```

Taskturbine provides a simple durable cross-platform task framework for Rust, and
Python.

## Installation

For python:

```bash
uv add taskturbine
```

For python:

```bash
cargo add taskturbine
```

## Concepts

TODO

## Getting started

TODO

## Command line tools

TODO

## Comparisons

TODO

## Project History

The schema and library interfaces were inspired by
[Absurd](https://github.com/earendil-works/absurd). However, instead of leaning
on stored procedures, the 'core' library interface is implemented as a rust
crate, and libraries are provided for both Rust and Python, with more language
support planned in the future.

## License

Apache 2.0 License

Copyright 2025 earendil-works
Copyright 2025 Mark Story
