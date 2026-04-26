# Taskturbine Python

Python SDK for taskturbine. Built on top of the taskturbine-core package in this repository. This library contains both synchronous and asyncio APIs.

## Synchronous API Usage

```python
import json
from datetime import timedelta

from taskturbine import Config, TaskturbineApp, TaskContext

# Config reflects options available in rust package.
config = Config(
    database_url="postgres://app:password@localhost:5432/my_app",
    usecase="docket-app"
)

# Build an app that can have tasks attached.
app = TaskturbineApp(config=config)

# Define additional channels that tasks can be spawned on
app.add_channel("reports")

# Register a task that can be spawned
@app.register_task("process-signup")
def process_signup(ctx: TaskContext) -> None:

    # Tasks are composed of steps that run at least once.
    @ctx.step(name="create-user")
    def create_user(ctx: TaskContext) -> str:
        # insert user
        user_data = {...}
        return json.dumps(user_data)

    @ctx.step(name="send-registration-code")
    def send_registration_code(ctx: TaskContext) -> str:
        # Send registration code
        return ""

    @ctx.step(name="complete_registration")
    def complete_registration(ctx: TaskContext) -> str:
        # Provision the rest of the account
        return ""

    # Run the steps
    user_data = create_user(ctx)
    regstration_data = send_registration_code(ctx)

    # Wait for an external event.
    payload = ctx.await_event(registration_data['event_name'], timeout=timedelta(minutes=10))

    result = complete_registration(ctx)

    return result


# Spawn a task with a dict of parameters and options
# parameters will be JSON encoded automatically.
app.spawn_task("process-signup", parameters, retry_seconds=30)

# Spawn a task on a defined channel.
app.spawn_task("process-signup", parameters, channel="reports")

# Emit an external event. Payload is expected to be bytes
# containing the event.
app.emit_event("event-123", payload)

# Run a worker consuming from two channels
worker = app.worker("worker-812", ["reports", "default"])
worker.run()

# Run a dedicated upkeep worker
worker = app.worker("worker-upkeep")
worker.run_upkeep()
```

The synchronous `Worker` uses `multiprocessing` to increase throughput. You can
control how many child processes are spawned with `Config.worker_concurrency`.

## Asyncio API Usage

```python
import json
from datetime import timedelta

from taskturbine import Config
from taskturbine.asynclib import AsyncTaskturbineApp, AsyncTaskContext

# Config reflects options available in rust package.
config = Config(
    database_url="postgres://app:password@localhost:5432/my_app",
    usecase="docket-app"
)

# Build an app that can have tasks attached.
app = AsyncTaskturbineApp(config=config)

# Define additional channels that tasks can be spawned on
app.add_channel("reports")

# Register a task that can be spawned
@app.register_task("process-signup")
async def process_signup(ctx: AsyncTaskContext) -> None:

    # Tasks are composed of steps that run at least once.
    @ctx.step(name="create-user")
    async def create_user(ctx: AsyncTaskContext) -> str:
        # insert user
        user_data = {...}
        return json.dumps(user_data)

    @ctx.step(name="send-registration-code")
    async def send_registration_code(ctx: AsyncTaskContext) -> str:
        # Send registration code
        return ""

    @ctx.step(name="complete_registration")
    async def complete_registration(ctx: AsyncTaskContext) -> str:
        # Provision the rest of the account
        return ""

    # Run the steps
    user_data = await create_user(ctx)
    regstration_data = await send_registration_code(ctx)

    # Wait for an external event.
    payload = await ctx.await_event(registration_data['event_name'], timeout=timedelta(minutes=10))

    result = await complete_registration(ctx)

    return result


# Spawn a task with a dict of parameters and options
# parameters will be JSON encoded automatically.
await app.spawn_task("process-signup", parameters, retry_seconds=30)

# Spawn a task on a defined channel.
await app.spawn_task("process-signup", parameters, channel="reports")

# Emit an external event. Payload is expected to be bytes
# containing the event.
await app.emit_event("event-123", payload)

# Run a worker consuming from two channels
worker = app.worker("worker-812", ["reports", "default"])
await worker.run()

# Run an upkeep worker
worker = app.worker("worker-upkeep")
await worker.run_upkeep()
```

Async workers are single threaded and will run up to `Config.worker_concurrency`
tasks simultaneously.

## Development setup

You'll need `uv` installed. You can use  `uv init && uv sync` to setup a development
environment.

## Building and running tests

This library uses `maturin` to build the native extension:

1. Run `uv run maturin develop` or `uv run maturin develop --release`
2. Run `uv run maturin build` to produce a wheel.

To run tests you'll need `TASKTURBINE_DATABASE_URL` set:

```bash
export TASKTURBINE_DATABASE_URL="postgres://postgres:@127.0.0.1:5432/my_app"
```

Then tests can be run with `uv run pytest`.
