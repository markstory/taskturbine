from datetime import datetime, timedelta
from typing import Any

import psycopg2

from taskturbine import Config, TaskContext, TaskturbineApp
from tests.demo import app as demo_app
from taskturbine.taskturbine import ClaimedTask

from .conftest import fetch_all, fetch_one

Connection = psycopg2._psycopg.connection


def test_claimedtask_dict_methods(config: Config, channel: str) -> None:
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="claim-retry")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        return {"complete": "ok"}

    app.spawn_task(
        "claim-retry",
        {"oid": 123},
        channel=channel,
        retry_seconds=15,
        retry_max_seconds=23,
    )
    worker = app.create_worker("worker-1", [channel])
    claimed = worker._inner.claim_tasks()

    assert len(claimed)
    first = claimed[0]
    res = first.to_dict()
    assert res, "empty dict"
    for key, value in res.items():
        assert getattr(first, key) == value, f"Difference in {key}"

    rebuild = ClaimedTask.from_dict(res)
    assert rebuild
    for key, value in res.items():
        assert getattr(rebuild, key) == value, f"Difference in {key}"


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


def test_worker_execute_batch_simple_success(
    config: Config, db_connection: Connection, channel: str
) -> None:
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
    rows = fetch_all(cursor)
    assert len(rows) == 2
    assert rows[0]["state"] == "completed"
    assert rows[0]["result"].tobytes() == b'{"complete": "ok"}'
    assert rows[1]["state"] == "completed"
    assert rows[1]["result"].tobytes() == b'{"complete": "ok"}'


def test_worker_execute_batch_simple_failure(
    config: Config, db_connection: Connection, channel: str
) -> None:
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
    rows = fetch_all(cursor)
    assert len(rows) == 2


def test_worker_execute_batch_error_handler(config: Config, channel: str) -> None:
    def error_handler(err: Exception) -> None:
        assert isinstance(err, Exception), "should be an exception"
        assert str(err) == "oh no", "Should have the error from the step"

    app = TaskturbineApp(config, error_handler=error_handler)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        raise TypeError("oh no")

    app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    worker.execute_batch()


def test_worker_execute_batch_mixed_failure(
    config: Config, db_connection: Connection, channel: str
) -> None:
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
    rows = fetch_all(cursor)
    assert len(rows) == 2
    assert rows[0]["state"] == "completed"
    assert rows[1]["state"] == "failed"


def test_worker_cleanup(
    config: Config, db_connection: Connection, channel: str
) -> None:
    app = TaskturbineApp(config)
    app.add_channel(channel)

    # TODO continue here, this test needs to be re-written
    @app.register_task(name="worker-cleanup")
    def worker_task(ctx: TaskContext) -> dict[str, Any]:
        return {"ok": "ok"}

    first = app.spawn_task("worker-cleanup", {"oid": 123}, channel=channel)
    second = app.spawn_task("worker-cleanup", {"oid": 123}, channel=channel)

    # Update state to allow cleanup to make changes
    with db_connection.cursor() as cursor:
        cursor.execute("BEGIN")
        # Expired claim
        cursor.execute(
            """
            UPDATE taskturbine.runs
            SET state = 'running',
              claim_expires_at = %s,
              claimed_by = %s
            WHERE task_id = %s
            """,
            [
                (datetime.now() - timedelta(minutes=1)).isoformat(),
                "worker-1",
                first.task_id,
            ],
        )

        # Expired task
        cursor.execute(
            """
            UPDATE taskturbine.tasks
            SET state = 'sleeping', 
              first_started_at = %s,
              cancellation_max_age = %s
            WHERE task_id = %s
            """,
            [
                (datetime.now() - timedelta(minutes=10, seconds=1)).isoformat(),
                600,
                second.task_id,
            ],
        )
        cursor.execute("COMMIT")
    worker = app.create_worker("worker-1", [channel])
    worker._inner.run_upkeep()

    with db_connection.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM taskturbine.runs WHERE task_id = %s",
            [first.task_id],
        )
        row = fetch_one(cursor)
        assert row
        assert row["state"] == "failed", "claimed task should be failed now"

        cursor.execute(
            "SELECT * FROM taskturbine.tasks WHERE task_id = %s",
            [second.task_id],
        )
        row = fetch_one(cursor)
        assert row
        assert row["state"] == "cancelled", "should be cancelled now"


def test_worker_run_simple_success(
    config: Config, db_connection: Connection, channel: str
) -> None:
    # The demo_app is a bit of a wart.
    demo_app.add_channel(channel)

    first = demo_app.spawn_task("worker-task", {"oid": 123}, channel=channel)
    second = demo_app.spawn_task("worker-task", {"oid": 456}, channel=channel)

    worker = demo_app.create_worker("worker-1", [channel])
    worker.run(stop_on_idle=True)

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM taskturbine.runs WHERE run_id IN (%s, %s)",
        [first.run_id, second.run_id],
    )
    rows = fetch_all(cursor)
    assert len(rows) == 2
    assert rows[0]["state"] == "completed"
    assert rows[1]["state"] == "completed"
