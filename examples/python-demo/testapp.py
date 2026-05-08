import atexit
import functools
import logging
import os
import time
import random
from typing import Any
import uuid

from taskturbine import TaskturbineApp, Config, TaskContext

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

db_url = os.getenv("TASKTURBINE_DATABASE_URL")
assert db_url, "TASKTURBINE_DATABASE_URL is required"

logger = logging.getLogger(__name__)

# Setup application. This would likely be in a module imported after the application
# is bootstrapped.
config = Config(
    database_url=db_url,
    app_module="testapp:app",
    worker_concurrency=4,
    worker_sleep_ms=100
)
app = TaskturbineApp(config)

type Payload = dict[str, Any]


@app.register_task("hello-world")
def hello_world(ctx: TaskContext) -> None:
    logger.info(f"Hello world! {ctx.params_bytes.decode()}")

    # We start off defining the steps in our task.
    @ctx.step(name="compute_user_data")
    def compute_user_data(ctx: TaskContext) -> Payload:
        logger.info("starting compute_user_data")
        # a step that does some compute/query and returns a dict value.
        params = ctx.params
        return {"name": params["name"], "id": uuid.uuid4().hex, "started": time.time()}

    # a step that fails when the context counter is at the wrong value
    #   on the second retry enough time has elapsed that it continues and returns a value.
    @ctx.step(name="delay_via_retry_errors")
    def check_duration(ctx: TaskContext, user: Payload) -> None:
        logger.info("starting check_duration")
        now = time.time()
        if now - user["started"] < 3:
            raise ValueError("Too soon, die and retry")

    @ctx.step(name="process-complete")
    def process_complete(ctx: TaskContext, user: Payload, event: Payload) -> Payload:
        logger.info("starting process_complete")
        # Do some IO
        user["complete"] = True
        user["event_id"] = event["id"]
        return user

    # Once we have defined our steps, we wire them together with control flow.
    user = compute_user_data(ctx)
    assert user, "User must exist"
    check_duration(ctx, user)
    # wait until we hear from an external workflow
    event = ctx.await_event(f"verified-{user['name']}")
    # a step that uses the event results and other results to make a new value.
    completed = process_complete(ctx, user, event)
    logger.info("Completed:", completed)


@app.register_task("sleep-time")
def sleep_time(ctx: TaskContext) -> None:
    delay = ctx.params.get("duration", 0.1)
    logger.info(f"started sleep_time. Sleeping for {delay}")
    time.sleep(delay)
    logger.info("sleep_time complete")


@app.register_task("random-failure")
def random_failure(ctx: TaskContext) -> None:
    failure_rate = ctx.params.get("failure_rate", 0.3)
    logger.info("started random_failure")
    if random.random() < failure_rate:
        raise ValueError("Task failed. Retry maybe.")
    logger.info("random failure complete")


@app.register_task("loop-step")
def loop_step(ctx: TaskContext) -> None:
    failure_rate = ctx.params.get("failure_rate", 0.05)
    iterations = ctx.params.get("iterations", 10)
    max_sleep = ctx.params.get("max_sleep", 0.5)

    def do_step(loop_id: int) -> None:
        sleep = max(max_sleep, random.random())
        logger.info(f"Starting loop {loop_id} sleeping {sleep}")
        time.sleep(sleep)

        if random.random() < failure_rate:
            raise ValueError("Step failed. Retry maybe.")

    logger.info(f"started loop_step: iterations={iterations}")
    for i in range(iterations):
        ctx.step_run("do-step", functools.partial(do_step, i))

    logger.info("loop_step complete")


# TODO test out the worker more:
# - build task that does a fan out. Make a loop with 100 steps.


def main() -> None:
    worker = app.create_worker("worker-1", ["default"])

    def shutdown() -> None:
        worker.shutdown()

    atexit.register(shutdown)
    worker.run(stop_on_idle=True)


if __name__ == "__main__":
    main()
