from datetime import timedelta
from typing import Any

import psycopg2
import pytest

from taskturbine import Config
from taskturbine.asynclib AsyncTaskturbineApp
from taskturbine.context import TaskContext
from taskturbine.models import SuspendError

Connection = psycopg2._psycopg.connection

five_min = timedelta(minutes=5)


@pytest.mark.skip(reason="no create_worker yet")
@pytest.mark.asyncio
async def test_context_attributes(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    def first_task(a: str) -> str:
        return f"called {a}"

    await app.spawn_task("first-task", {"str": "value", "int": 123}, channel=channel)
    worker = app.create_worker("worker-1", [channel])
    claims = await worker.claim_tasks()
    assert len(claims) >= 1
    claim = claims[0]
    context = app.create_context(claims[0])

    assert context.task_id == claim.task_id
    assert context.run_id == claim.run_id
    assert context.params == {"int": 123, "str": "value"}
    assert context.params_bytes == b'{"str": "value", "int": 123}'


@pytest.mark.skip(reason="no create_worker yet")
@pytest.mark.asyncio
async def test_context_await_event_event_present(config: Config) -> None:
    app = AsyncTaskturbineApp(config)

    @app.register_task(name="first-task")
    async def first_task(a: str) -> str:
        return f"called {a}"

    res = await app.spawn_task("first-task", {})
    assert res.task_id
    assert res.run_id

    await app.emit_event("context_await_event", {"status": "ok"})

    # Claim a task so that it is 'running' and TaskContext can wait for the event.
    claims = await app.create_worker("worker-1", ["default"]).claim_tasks()
    assert len(claims) >= 1

    context = app.create_context(claims[0])
    result = await context.await_event("context_await_event")
    assert result
    assert result["status"] == "ok"


@pytest.mark.skip(reason="no create_worker yet")
@pytest.mark.asyncio
async def test_context_await_event_no_event(config: Config) -> None:
    app = AsyncTaskturbineApp(config)

    @app.register_task(name="first-task")
    async def first_task(a: str) -> str:
        return f"called {a}"

    res = await app.spawn_task("first-task", {})
    assert res.task_id
    assert res.run_id

    # Claim a task so that it is 'running' and TaskContext can wait for the event.
    claims = await app.create_worker("worker-1", ["default"]).claim_tasks()
    assert len(claims) >= 1

    context = app.create_context(claims[0])
    with pytest.raises(SuspendError) as err:
        await context.await_event("context_await_event_no_event")
    assert err
    # Duration is none, as a wait is registered for the event
    # and the task is suspended at the same time.
    assert err.value.duration is None


@pytest.mark.skip(reason="no create_worker yet")
@pytest.mark.asyncio
async def test_context_emit_event(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(a: str) -> str:
        return f"called {a}"

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    await context.emit_event("context_emit_event", {"status": "ok"})


@pytest.mark.asyncio
async def test_context_emit_event_duplicate(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(a: str) -> str:
        return f"called {a}"

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    await context.emit_event("context_emit_event_duplicate", {"status": "ok"})
    await context.emit_event("context_emit_event_duplicate", {"status": "not-ok"})

    event = await context.await_event("context_emit_event_duplicate")
    assert event["status"] == "not-ok", "Last event is retained"


@pytest.mark.asyncio
async def test_context_sleep_for(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(a: str) -> str:
        return f"called {a}"

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    with pytest.raises(SuspendError) as err:
        context.sleep_for("sleep-timer", timedelta(minutes=3))
    assert err.value
    assert err.value.duration == timedelta(minutes=3)


@pytest.mark.asyncio
async def test_context_step_return_result(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        @ctx.step(name="first-step")
        async def step_one(ctx: TaskContext) -> dict[str, Any]:
            assert isinstance(ctx, TaskContext)
            return {"step": "one"}

        step_data = await step_one(ctx)
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    task_result = await first_task(context)
    assert task_result
    assert task_result["step"] == "one"

    checkpoint = await context._inner.get_checkpoint("first-step")
    assert checkpoint
    assert checkpoint.step_name == "first-step"
    assert checkpoint.state == b'{"step": "one"}'


@pytest.mark.asyncio
async def test_context_step_raise_error(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        @ctx.step("first-step")
        async def step_one(ctx: TaskContext) -> None:
            assert isinstance(ctx, TaskContext)
            raise KeyError("oh no")

        step_data = await step_one(ctx)
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    with pytest.raises(KeyError) as err:
        await first_task(context)
    assert err.value
    assert "oh no" in str(err.value)

    with pytest.raises(ValueError):
        await context._inner.get_checkpoint("first-step")


@pytest.mark.asyncio
async def test_context_step_duplicate_runs(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="context-step-duplicate-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        @ctx.step("first-step")
        async def step_one(ctx: TaskContext) -> dict[str, Any]:
            return {"step": "first"}

        step_data = await step_one(ctx)
        assert isinstance(step_data, dict)
        assert step_data["step"] == "first"

        return step_data

    await app.spawn_task("context-step-duplicate-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    result = await first_task(context)
    assert result == {"step": "first"}

    result = await first_task(context)
    assert result == {"step": "first"}

    checkpoint = await context._inner.get_checkpoint("first-step")
    assert checkpoint


@pytest.mark.asyncio
async def test_context_step_run_return_result(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        async def step_one(ctx: TaskContext) -> dict[str, Any]:
            assert isinstance(ctx, TaskContext)
            return {"step": "one"}

        step_data = await ctx.step_run("first-step", async lambda: step_one(ctx))
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    task_result = await first_task(context)
    assert task_result
    assert task_result["step"] == "one"

    checkpoint = await context._inner.get_checkpoint("first-step")
    assert checkpoint
    assert checkpoint.step_name == "first-step"
    assert checkpoint.state == b'{"step": "one"}'


@pytest.mark.asyncio
async def test_context_step_run_raise_error(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        async def step_one(ctx: TaskContext) -> None:
            assert isinstance(ctx, TaskContext)
            raise KeyError("oh no")

        step_data = await ctx.step_run("first-step", async lambda: step_one(ctx))
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    with pytest.raises(KeyError) as err:
        await first_task(context)
    assert err.value
    assert "oh no" in str(err.value)

    with pytest.raises(ValueError):
        await context._inner.get_checkpoint("first-step")


@pytest.mark.asyncio
async def test_context_step_cb_duplicate_runs(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="context-step-duplicate-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        async def step_one(ctx: TaskContext) -> dict[str, Any]:
            return {"step": "first"}

        step_data = await ctx.step_run("first-step", lambda: step_one(ctx))
        assert isinstance(step_data, dict)
        assert step_data["step"] == "first"

        return step_data

    await app.spawn_task("context-step-duplicate-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    result = await first_task(context)
    assert result == {"step": "first"}

    result = await first_task(context)
    assert result == {"step": "first"}

    checkpoint = await context._inner.get_checkpoint("first-step")
    assert checkpoint


@pytest.mark.asyncio
async def test_context_run_step_return_result(config: Config, channel: str) -> None:
    app = AsyncTaskturbineApp(config)
    app.add_channel(channel)

    @app.register_task(name="first-task")
    async def first_task(ctx: TaskContext) -> dict[str, Any]:
        @ctx.step(name="first-step")
        async def step_one(ctx: TaskContext) -> dict[str, Any]:
            assert isinstance(ctx, TaskContext)
            return {"step": "one"}

        step_data = await ctx.step_run("first", lambda: step_one(ctx))
        assert isinstance(step_data, dict)
        assert step_data["step"] == "one"

        return step_data

    await app.spawn_task("first-task", {}, channel=channel)
    claims = await app.create_worker("worker-1", [channel]).claim_tasks()
    context = app.create_context(claims[0])

    task_result = await first_task(context)
    assert task_result
    assert task_result["step"] == "one"

    checkpoint = await context._inner.get_checkpoint("first-step")
    assert checkpoint
    assert checkpoint.step_name == "first-step"
    assert checkpoint.state == b'{"step": "one"}'
