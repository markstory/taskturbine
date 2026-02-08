from typing import Any
from datetime import timedelta

from taskturbine import (
    Config,
    SuspendError,
    TaskturbineApp,
    Task,
    TaskContext,
)

import pytest

five_min = timedelta(minutes=5)


def test_context_attributes(config, channel):
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    app.spawn_task("first-task", {"str": "value", "int": 123}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    assert len(claims) >= 1
    claim = claims[0]
    context = app.create_context(claims[0])

    assert context.task_id == claim.task_id
    assert context.run_id == claim.run_id
    assert context.params == {"int": 123, "str": "value"}
    assert context.params_bytes == b'{"str": "value", "int": 123}'


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


def test_context_emit_event(config, channel):
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    app.spawn_task("first-task", {}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    res = context.emit_event("context_emit_event", {"status": "ok"})
    assert res is None


def test_context_emit_event_duplicate(config, channel):
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    app.spawn_task("first-task", {}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    res = context.emit_event("context_emit_event_duplicate", {"status": "ok"})
    assert res is None
    res = context.emit_event("context_emit_event_duplicate", {"status": "not-ok"})
    assert res is None

    event = context.await_event("context_emit_event_duplicate")
    assert event["status"] == "not-ok", "Last event is retained"


def test_context_sleep_for(config, channel) -> None:
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    app.spawn_task("first-task", {}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    with pytest.raises(SuspendError) as err:
        context.sleep_for("sleep-timer", timedelta(minutes=3))
    assert err.value
    assert err.value.duration == timedelta(minutes=3)


def test_context_step_return_result(config, channel) -> None:
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(ctx: TaskContext) -> dict[str, Any]:
        def step_one(ctx) -> dict[str, Any]:
            assert isinstance(ctx, TaskContext)
            return {"step": "one"}

        step_data = ctx.step("first-step", step_one)
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    app.spawn_task("first-task", {}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    task_result = first_task(context)
    assert task_result
    assert task_result["step"] == "one"

    checkpoint = context._inner.get_checkpoint("first-step")
    assert checkpoint
    assert checkpoint.step_name == "first-step"
    assert checkpoint.state == b'{"step": "one"}'


def test_context_step_raise_error(config, channel) -> None:
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(ctx: TaskContext) -> dict[str, Any]:
        def step_one(ctx) -> dict[str, Any]:
            raise KeyError("oh no")

        step_data = ctx.step("first-step", step_one)
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    app.spawn_task("first-task", {}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    with pytest.raises(KeyError) as err:
        first_task(context)
    assert err.value
    assert "oh no" in str(err.value)

    with pytest.raises(ValueError) as err:
        context._inner.get_checkpoint("first-step")


def test_context_step_duplicate_runs(config, channel) -> None:
    five_min = timedelta(minutes=5)
    app = TaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="context-step-duplicate-task")
    def first_task(ctx: TaskContext) -> dict[str, Any]:
        def step_one(ctx) -> dict[str, Any]:
            return {"step": "first"}

        step_data = ctx.step("first-step", step_one)
        assert isinstance(step_data, dict)
        assert step_data["step"] == "first"

        return step_data

    app.spawn_task("context-step-duplicate-task", {}, channel=channel)
    claims = app.claim_task([channel], "worker-1", five_min, 1)
    context = app.create_context(claims[0])

    result = first_task(context)
    assert result == {"step": "first"}

    result = first_task(context)
    assert result == {"step": "first"}

    checkpoint = context._inner.get_checkpoint("first-step")
    assert checkpoint
