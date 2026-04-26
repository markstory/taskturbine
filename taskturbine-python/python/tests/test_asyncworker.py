from datetime import datetime, timedelta
from typing import Any

import psycopg2
import pytest

from taskturbine import Config
from taskturbine.asynclib import AsyncTaskContext, AsyncTaskturbineApp
from tests.demo import app as demo_app
from taskturbine.taskturbine import ClaimedTask

from .conftest import fetch_all, fetch_one

Connection = psycopg2._psycopg.connection


@pytest.mark.asyncio
async def test_worker_run_simple_success(
    async_config: Config, db_connection: Connection, channel: str
) -> None:
    app = AsyncTaskturbineApp(async_config)
    app.add_channel(channel)

    @app.register_task(name="worker-task")
    async def worker_task(ctx: AsyncTaskContext) -> dict[str, Any]:
        return {"complete": "ok"}

    first = await app.spawn_task("worker-task", {"oid": 123}, channel=channel)
    second = await app.spawn_task("worker-task", {"oid": 456}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    await worker.run(stop_on_idle=True)

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


@pytest.mark.asyncio
async def test_worker_run_simple_failure(
    async_config: Config, db_connection: Connection, channel: str
) -> None:
    app = AsyncTaskturbineApp(async_config)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    async def worker_task(ctx: AsyncTaskContext) -> dict[str, Any]:
        raise TypeError("oh no")

    first = await app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)
    second = await app.spawn_task("worker-task-fail", {"oid": 456}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    await worker.run(stop_on_idle=True)

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM taskturbine.runs WHERE run_id IN (%s, %s) AND state = 'failed'",
        [first.run_id, second.run_id],
    )
    rows = fetch_all(cursor)
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_worker_execute_batch_error_handler(async_config: Config, channel: str) -> None:
    def error_handler(err: Exception) -> None:
        assert isinstance(err, Exception), "should be an exception"
        assert str(err) == "oh no", "Should have the error from the step"

    app = AsyncTaskturbineApp(async_config, error_handler=error_handler)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    async def worker_task(ctx: AsyncTaskContext) -> dict[str, Any]:
        raise TypeError("oh no")

    await app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    await worker.run(stop_on_idle=True)


@pytest.mark.asyncio
async def test_worker_execute_batch_mixed_failure(
    async_config: Config, db_connection: Connection, channel: str
) -> None:
    app = AsyncTaskturbineApp(async_config)
    app.add_channel(channel)

    @app.register_task(name="worker-task-fail")
    async def worker_task(ctx: AsyncTaskContext) -> dict[str, Any]:
        if ctx.params["oid"] == 123:
            raise TypeError("oh no")
        return {"ok": "ok"}

    first = await app.spawn_task("worker-task-fail", {"oid": 123}, channel=channel)
    second = await app.spawn_task("worker-task-fail", {"oid": 456}, channel=channel)

    worker = app.create_worker("worker-1", [channel])
    await worker.run(stop_on_idle=True)

    cursor = db_connection.cursor()
    cursor.execute(
        "SELECT * FROM taskturbine.runs WHERE run_id IN (%s, %s) ORDER BY state",
        [first.run_id, second.run_id],
    )
    rows = fetch_all(cursor)
    assert len(rows) == 2
    assert rows[0]["state"] == "completed"
    assert rows[1]["state"] == "failed"


@pytest.mark.asyncio
async def test_worker_cleanup(
    async_config: Config, db_connection: Connection, channel: str
) -> None:
    app = AsyncTaskturbineApp(async_config)
    app.add_channel(channel)

    # TODO continue here, this test needs to be re-written
    @app.register_task(name="worker-cleanup")
    async def worker_task(ctx: AsyncTaskContext) -> dict[str, Any]:
        return {"ok": "ok"}

    first = await app.spawn_task("worker-cleanup", {"oid": 123}, channel=channel)
    second = await app.spawn_task("worker-cleanup", {"oid": 123}, channel=channel)

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
    await worker._inner.run_upkeep()

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
