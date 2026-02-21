import json
from typing import Any

import psycopg2
import pytest
from taskturbine.taskturbine import TaskOptions

Connection = psycopg2._psycopg.connection

from taskturbine import Config, Task, TaskSerializer, TaskturbineApp

from .conftest import row_factory


def test_add_channel(config: Config) -> None:
    app = TaskturbineApp(config)
    app.add_channel("reports")
    assert app.channels == {"default", "reports"}


def test_register_task(config: Config) -> None:
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


def test_spawn_task_unregistered(config: Config) -> None:
    app = TaskturbineApp(config)
    with pytest.raises(ValueError) as err:
        app.spawn_task("undefined", {})
    assert "task `undefined` is not registered" in str(err.value)


def test_spawn_task(config: Config) -> None:
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res
    assert res.task_id
    assert res.run_id


def test_spawn_task_uses_serialize_hooks(
    config: Config, db_connection: Connection
) -> None:
    class Serializer(TaskSerializer):
        def serialize(self, value: Any) -> bytes:
            return f"--{json.dumps(value)}--".encode()

        def deserialize(self, value: bytes) -> Any | None:
            content = value[2:-2]
            return json.loads(content)

    app = TaskturbineApp(config, serializer=Serializer())

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {"a": 123})
    assert res
    assert res.task_id

    cur = db_connection.cursor()
    cur.execute("SELECT * FROM taskturbine.tasks WHERE task_id = %s", [res.task_id])
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["task_id"] == res.task_id
    assert row["params"].tobytes() == b'--{"a": 123}--'


def test_spawn_task_with_options(config: Config, db_connection: Connection) -> None:
    app = TaskturbineApp(config)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task(
        "first-task",
        {},
        retry_seconds=5,
        max_attempts=10,
        retry_factor=2.0,
        retry_max_seconds=320,
        cancellation_max_age=150,
    )
    assert res
    assert res.task_id
    assert res.run_id

    cur = db_connection.cursor()
    cur.execute("SELECT * FROM taskturbine.tasks WHERE task_id = %s", [res.task_id])
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["task_id"] == res.task_id
    assert row["retry_seconds"] == 5
    assert row["max_attempts"] == 10
    assert row["retry_factor"] == 2.0
    assert row["retry_max_seconds"] == 320
    assert row["cancellation_max_age"] == 150


def test_spawn_task_uses_task_options(
    config: Config, db_connection: Connection
) -> None:
    app = TaskturbineApp(config)
    options = TaskOptions(
        retry_seconds=5,
        max_attempts=10,
        retry_factor=2.0,
        retry_max_seconds=200,
        cancellation_max_age=75,
    )

    @app.register_task(name="first-task", options=options)
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res
    assert res.task_id

    cur = db_connection.cursor()
    cur.execute("SELECT * FROM taskturbine.tasks WHERE task_id = %s", [res.task_id])
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["task_id"] == res.task_id
    assert row["retry_seconds"] == 5
    assert row["max_attempts"] == 10
    assert row["retry_factor"] == 2.0
    assert row["retry_max_seconds"] == 200
    assert row["cancellation_max_age"] == 75


def test_set_spawn_options(config: Config, db_connection: Connection) -> None:
    app = TaskturbineApp(config)
    app.set_spawn_options(
        retry_seconds=5,
        max_attempts=10,
        retry_factor=2.0,
        retry_max_seconds=200,
        cancellation_max_age=75,
    )

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    res = app.spawn_task("first-task", {})
    assert res
    assert res.task_id

    cur = db_connection.cursor()
    cur.execute("SELECT * FROM taskturbine.tasks WHERE task_id = %s", [res.task_id])
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["task_id"] == res.task_id
    assert row["retry_seconds"] == 5
    assert row["max_attempts"] == 10
    assert row["retry_factor"] == 2.0
    assert row["retry_max_seconds"] == 200
    assert row["cancellation_max_age"] == 75


def test_emit_event(config: Config, db_connection: Connection) -> None:
    app = TaskturbineApp(config)

    data = {"key": "value"}
    app.emit_event("event-1", data)

    cur = db_connection.cursor()
    cur.execute("SELECT * FROM taskturbine.events WHERE event_name = %s", ["event-1"])
    row = row_factory(cur, cur.fetchone())
    assert row
    assert row["payload"].tobytes() == json.dumps(data).encode()
