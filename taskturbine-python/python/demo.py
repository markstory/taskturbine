"""
A simple demo app used for integration testing.
"""
import os
from typing import Any
from taskturbine import TaskturbineApp, Config, TaskContext

# TODO this is all brittle and not very extensible.
# Perhaps we can have an importable module in the test suite?
db_url = os.getenv("TASKTURBINE_DATABASE_URL")
assert db_url, "Required environment variable TASKTURBINE_DATABASE_URL undefined"

config = Config(app_module="demo:app", database_url=db_url)
app = TaskturbineApp(config)


@app.register_task(name="worker-task")
def worker_task(ctx: TaskContext) -> dict[str, Any]:
    return {"complete": "ok"}
