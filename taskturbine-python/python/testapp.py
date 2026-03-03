import logging
import os
import time
from typing import Any
import uuid

from taskturbine import TaskturbineApp, Config
from taskturbine.context import TaskContext

logging.basicConfig(level=logging.DEBUG)

# Setup application. This would likely be in a module imported after the application
# is bootstrapped.
config = Config(
    database_url=os.getenv("TASKTURBINE_DATABASE_URL"),
    app_module="testapp:app",
    worker_concurrency=4,
)
app = TaskturbineApp(config)

type Payload = dict[str, Any]

@app.register_task("hello-world")
def hello_world(ctx: TaskContext) -> None:
    print(f"Hello world! {ctx.params_bytes.decode()}")

    # We start off defining the steps in our task.
    @ctx.step(name="compute_user_data")
    def compute_user_data(ctx: TaskContext) -> Payload:
        logging.info("starting compute_user_data")
        # a step that does some compute/query and returns a dict value.
        params = ctx.params
        return {"name": params["name"], "id": uuid.uuid4().hex, "started": time.time()}


    # a step that fails when the context counter is at the wrong value
    #   on the second retry enough time has elapsed that it continues and returns a value.
    @ctx.step(name="delay_via_retry_errors")
    def check_duration(ctx: TaskContext, user: Payload) -> None:
        logging.info("starting check_duration")
        now = time.time()
        if now - user["started"] < 3:
            raise ValueError("Too soon, die and retry")

    @ctx.step(name="process-complete")
    def process_complete(ctx: TaskContext, user: Payload, event: Payload) -> Payload:
        logging.info("starting process_complete")
        # Do some IO
        user["complete"] = True
        user["event_id"] = event["id"]
        return user


    # Once we have defined our steps, we wire them together with control flow.
    user = compute_user_data(ctx)
    check_duration(ctx, user)
    # wait until we hear from an external workflow
    event = ctx.await_event(f"verified-{user['name']}")
    # a step that uses the event results and other results to make a new value.
    completed = process_complete(ctx, user, event)
    logging.info("Completed:", completed)


def main() -> None:
    worker = app.create_worker("worker-1", ["default"])
    worker.run()

if __name__ == "__main__":
    main()
