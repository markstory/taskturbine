import asyncio
import functools
import logging
import os
import time
import random
from typing import Any
import uuid

from taskturbine import Config
from taskturbine.asynclib import AsyncTaskturbineApp, AsyncTaskContext

logging.basicConfig(level=logging.DEBUG)

db_url = os.getenv("TASKTURBINE_DATABASE_URL")
assert db_url, "TASKTURBINE_DATABASE_URL is required"

logger = logging.getLogger(__name__)

# Setup application. This would likely be in a module imported after the application
# is bootstrapped.
config = Config(
    database_url=db_url,
    app_module="testappasync:app",
    worker_concurrency=4,
)
app = AsyncTaskturbineApp(config)

type Payload = dict[str, Any]


@app.register_task("hello-world")
async def hello_world(ctx: AsyncTaskContext) -> None:
    logger.info(f"Hello world! {ctx.params_bytes.decode()}")

    # We start off defining the steps in our task.
    @ctx.step(name="compute_user_data")
    async def compute_user_data(ctx: AsyncTaskContext) -> Payload:
        logger.info("starting compute_user_data")
        # a step that does some compute/query and returns a dict value.
        params = ctx.params
        return {"name": params["name"], "id": uuid.uuid4().hex, "started": time.time()}

    # a step that fails when the context counter is at the wrong value
    #   on the second retry enough time has elapsed that it continues and returns a value.
    @ctx.step(name="delay_via_retry_errors")
    async def check_duration(ctx: AsyncTaskContext, user: Payload) -> None:
        logger.info("starting check_duration")
        now = time.time()
        if now - user["started"] < 3:
            raise ValueError("Too soon, die and retry")

    @ctx.step(name="process-complete")
    async def process_complete(
        ctx: AsyncTaskContext, user: Payload, event: Payload
    ) -> Payload:
        logger.info("starting process_complete")
        # Do some IO
        user["complete"] = True
        user["event_id"] = event["id"]
        return user

    # Once we have defined our steps, we wire them together with control flow.
    user = await compute_user_data(ctx)
    assert user, "User must exist"
    await check_duration(ctx, user)
    # wait until we hear from an external workflow
    event = await ctx.await_event(f"verified-{user['name']}")
    # a step that uses the event results and other results to make a new value.
    completed = await process_complete(ctx, user, event)

    logger.info("Completed:", completed)


@app.register_task("sleep-time")
async def sleep_time(ctx: AsyncTaskContext) -> None:
    delay = ctx.params.get("duration", 0.1)
    logger.info(f"started sleep_time. Sleeping for {delay}")
    await asyncio.sleep(delay)
    logger.info("sleep_time complete")


@app.register_task("random-failure")
async def random_failure(ctx: AsyncTaskContext) -> None:
    failure_rate = ctx.params.get("failure_rate", 0.3)
    logger.info("started random_failure")
    if random.random() < failure_rate:
        raise ValueError("Task failed. Retry maybe.")
    logger.info("random failure complete")


@app.register_task("loop-step")
async def loop_step(ctx: AsyncTaskContext) -> None:
    failure_rate = ctx.params.get("failure_rate", 0.05)
    iterations = ctx.params.get("iterations", 10)
    max_sleep = ctx.params.get("max_sleep", 0.5)

    async def do_step(loop_id: int) -> None:
        sleep = max(max_sleep, random.random())
        logger.info(f"Starting loop {loop_id} sleeping {sleep}")
        await asyncio.sleep(sleep)

        if random.random() < failure_rate:
            raise ValueError("Step failed. Retry maybe.")

    logger.info(f"started loop_step: iterations={iterations}")
    for i in range(iterations):
        # Note: This will run the iterations serially.
        await ctx.step_run("do-step", functools.partial(do_step, i))

    logger.info("loop_step complete")


async def main() -> None:
    worker = app.create_worker("worker-1", ["default"])

    try:
        await worker.run()
    except (KeyboardInterrupt, asyncio.CancelledError):
        await worker.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
