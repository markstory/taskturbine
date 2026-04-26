"""
A simple demo app used for integration testing.
"""

import os
from typing import Any
from taskturbine import TaskturbineApp, Config, TaskContext

# TODO Find a more elegant solution to this.
# Having a demo application in a top level module isn't ideal.
# Perhaps we can have an importable module in the test suite?
db_url = os.getenv("TASKTURBINE_DATABASE_URL")
assert db_url, "Required environment variable TASKTURBINE_DATABASE_URL undefined"

config = Config(app_module="tests.demo:app", database_url=db_url)
app = TaskturbineApp(config)


@app.register_task(name="ok-task")
def worker_task(ctx: TaskContext) -> dict[str, Any]:
    return {"complete": "ok"}


@app.register_task(name="type-error-fail")
def type_error_fail(ctx: TaskContext) -> dict[str, Any]:
    raise TypeError("oh no")


@app.register_task(name="oid-partial-failure")
def oid_partial_failure(ctx: TaskContext) -> dict[str, Any]:
    if ctx.params["oid"] == 123:
        raise TypeError("oh no")
    return {"ok": "ok"}
