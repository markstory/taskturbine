import logging
import os

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

@app.register_task("hello-world")
def hello_world(ctx: TaskContext) -> None:
    print(f"Hello world! {ctx.params_bytes.decode()}")


def main() -> None:
    worker = app.create_worker("worker-1", ["default"])
    worker.run()

if __name__ == "__main__":
    main()
