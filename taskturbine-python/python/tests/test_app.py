import psycopg2
import pytest
from datetime import datetime, timedelta
import json
import os

from taskturbine import Config, SuspendError, TaskturbineApp, Task


@pytest.fixture
def config(database_url) -> Config:
    return Config(app_module="", database_url=database_url)


@pytest.fixture
def database_url() -> str:
    value = os.getenv("TASKTURBINE_DATABASE_URL")
    assert value, "Required environment variable TASKTURBINE_DATABASE_URL undefined"
    return value

@pytest.fixture
def db_connection(database_url):
    return psycopg2.connect(database_url)


def row_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def test_add_channel(config) -> None:
    app = TaskturbineApp(config)
    app.add_channel("reports")
    assert app.channels == ["default", "reports"]


def test_register_task(config) -> None:
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    # App has some basic methods to get tasks
    # The worker will use this API
    assert app.has_task("nope") is False
    assert app.has_task("First-task") is False
    assert app.has_task("first-task")

    with pytest.raises(KeyError):
        app.get_task("nope")

    # Task functions get wrapped in decorator objects
    task = app.get_task("first-task")
    assert isinstance(task, Task)
    # Name attribute is added
    assert task.name == "first-task"
    # The class will proxy to the wrapped function
    assert task("one") == "called one"


def test_spawn_task_unregistered(config):
    app = TaskturbineApp(config)
    with pytest.raises(ValueError) as err:
        app.spawn_task("undefined", {})
    assert "task `undefined` is not registered" in str(err.value)


def test_spawn_task(config):
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res
    assert res.task_id
    assert res.run_id


def test_spawn_task_with_options(config, db_connection):
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {}, retry_seconds=5, max_attempts=10)
    assert res
    assert res.task_id
    assert res.run_id

    cur = db_connection.cursor()
    cur.execute(
        "SELECT * FROM taskturbine.tasks WHERE task_id = %s",
        [res.task_id]
    )
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["task_id"] == res.task_id
    assert row["retry_seconds"] == 5
    assert row["max_attempts"] == 10

def test_emit_event(config, db_connection):
    app = TaskturbineApp(config)

    data = {"key": "value"}
    app.emit_event("event-1", data)

    cur = db_connection.cursor()
    cur.execute(
        "SELECT * FROM taskturbine.events WHERE event_name = %s",
        ["event-1"]
    )
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["payload"].tobytes() == json.dumps(data).encode()


def test_context_await_event_event_present(config):
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res.task_id
    assert res.run_id

    five_min = timedelta(minutes=5)
    app.emit_event("context_await_event", {"status": "ok"})

    # Claim a task so that it is 'running' and TaskContext can wait for the event.
    claims = app.claim_task(["default"], "worker-1", five_min, 1)
    assert len(claims) >= 1

    context = app.create_context(claims[0])
    result = context.await_event("context_await_event")
    assert result
    assert result["status"] == "ok"


def test_context_await_event_no_event(config):
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res.task_id
    assert res.run_id

    # Claim a task so that it is 'running' and TaskContext can wait for the event.
    five_min = timedelta(minutes=5)
    claims = app.claim_task(["default"], "worker-1", five_min, 1)
    assert len(claims) >= 1

    context = app.create_context(claims[0])
    with pytest.raises(SuspendError) as err:
        context.await_event("context_await_event_no_event")
    assert err
    # Duration is none, as a wait is registered for the event
    # and the task is suspended at the same time.
    assert err.value.duration is None


def test_context_await_event_event_present(config):
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res.task_id
    assert res.run_id

    five_min = timedelta(minutes=5)
    app.emit_event("context_await_event", {"status": "ok"})

    # Claim a task so that it is 'running' and TaskContext can wait for the event.
    claims = app.claim_task(["default"], "worker-1", five_min, 1)
    assert len(claims) >= 1

    context = app.create_context(claims[0])
    result = context.await_event("context_await_event")
    assert result
    assert result["status"] == "ok"


def test_context_emit_event(config):
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    claims = app.claim_task(["default"], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    res = app.emit_event("context_emit_event", {"status": "ok"})
    assert res is None


def test_context_sleep_for(config) -> None:
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    five_min = timedelta(minutes=5)
    claims = app.claim_task(["default"], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    with pytest.raises(SuspendError) as err:
        context.sleep_for("sleep-timer", timedelta(minutes=3))
    assert err.value
    assert err.value.duration == timedelta(minutes=3)
