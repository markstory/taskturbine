from typing import Any
from datetime import timedelta

from .conftest import row_factory
from taskturbine import Config, TaskturbineApp, Task, TaskContext

import psycopg2
import pytest


Connection = psycopg2._psycopg.connection

def test_claimedtask_retry_in_defaults(config: Config, channel: str) -> None:
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="claim-retry")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        return {"complete": "ok"}

    app.spawn_task("claim-retry", {"oid": 123}, channel=channel)
    worker = app.create_worker("worker-1", [channel])
    claimed = worker._inner.claim_tasks()
    assert len(claimed), "Claimed tasks"
    claim = claimed[0]
    assert claim.next_retry_in() == timedelta(seconds=30)


def test_worker_execute_batch_simple_success(config: Config, db_connection: Connection, channel: str) -> None:
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="worker-task")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        return {"complete": "ok"}

    first = app.spawn_task("worker-task", {"oid": 123}, channel=channel)
    second = app.spawn_task("worker-task", {"oid": 456}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    worker.execute_batch()

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM taskturbine.runs WHERE run_id IN (%s, %s)",
        [first.run_id, second.run_id],
    )
    rows = list(map(lambda row: row_factory(cursor, row), cursor.fetchall()))
    assert len(rows) == 2
    assert rows[0]["state"] == "completed"
    assert rows[1]["state"] == "completed"


def test_worker_execute_batch_simple_failure(config: Config, db_connection: Connection, channel: str) -> None:
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        raise TypeError("oh no")

    first = app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)
    second = app.spawn_task("worker-task-fail", {"oid": 456}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    worker.execute_batch()

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM taskturbine.runs WHERE run_id IN (%s, %s) AND state = 'failed'",
        [first.run_id, second.run_id],
    )
    rows = list(map(lambda row: row_factory(cursor, row), cursor.fetchall()))
    assert len(rows) == 2


def test_worker_execute_batch_error_handler(config: Config, channel: str) -> None:
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        raise TypeError("oh no")

    app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)
    def error_handler(err: Exception) -> None:
        assert isinstance(err, Exception), "should be an exception"
        assert str(err) == "oh no", "Should have the error from the step"

    worker = app.create_worker("worker-1", [channel], error_handler=error_handler)
    worker.execute_batch()


def test_worker_execute_batch_mixed_failure(config: Config, db_connection: Connection, channel: str) -> None:
    channel = "execute_batch_mixed_failure"
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        if ctx.params["oid"] == 123:
            raise TypeError("oh no")
        return {"ok": "ok"}

    first = app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)
    second = app.spawn_task("worker-task-fail", {"oid": 456}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    worker.execute_batch()

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM taskturbine.runs WHERE run_id IN (%s, %s) ORDER BY state",
        [first.run_id, second.run_id],
    )
    rows = list(map(lambda row: row_factory(cursor, row), cursor.fetchall()))
    assert len(rows) == 2
    assert rows[0]["state"] == "completed"
    assert rows[1]["state"] == "failed"
