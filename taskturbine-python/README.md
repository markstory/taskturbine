# Taskturbine Python

Python SDK for taskturbine. Built on top of the taskturbine-core package in this repository.

## Development setup

1. Choose a supported python (python3.13) `pyenv local 3.13.5`
2. Run `python -m venv .venv`
3. Run `source .venv/bin/activate`
4. Run `uv sync`

## Building

1. Run `cargo build`
2. Run `maturin develop` or `maturin develop --release`
3. Use `python` to run scripts that can import the built module.
4. Run `maturin build` o produce a wheel


## API Usage

```python
from datetime import timedelta
import json
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

# Register a task and define steps.
@app.register_task("process-signup")
def process_signup(ctx: TaskContext) -> None:
    def create_user(ctx: TaskContext) -> str:
        # insert user
        user_data = {...}
        return json.dumps(user_data)

    user_data = ctx.step("create-user", create_user)

    def send_registration_code(ctx: TaskContext) -> str:
        # Send registration code
        return ""

    event_name = ctx.step("send-registration-code", send_registration_code)

    # Wait for an external event.
    payload = ctx.await_event(event_name, timeout=timedelta(minutes=10))
    
    def complete_registration(ctx: TaskContext) -> str:
        # Provision the rest of the account
        return ""

    result = ctx.step("complete_registration", complete_registration)

    return result


# Spawn a task with a dict of parameters and options
# parameters will be JSON encoded automatically.
options = {"retry_seconds": 30}
app.spawn_task("process-signup", parameters, options)

# Spawn a task on a defined channel.
app.channel("reports").spawn_task("process-signup", parameters)

# Emit an external event. Payload is expected to be bytes
# containing the event.
app.emit_event("event-123", payload)


# Run a worker consuming from two channels
worker = app.worker("worker-812", ["reports", "default"])
worker.run()

# Run a cleanup worker
worker = app.cleanup_worker()
worker.run()
```
