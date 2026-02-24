from __future__ import annotations

import enum
import dataclasses
import importlib
import time
from datetime import timedelta
from multiprocessing.pool import AsyncResult, Pool
from typing import Any, Callable, Mapping, TYPE_CHECKING

from taskturbine.context import TaskContext
from taskturbine.models import Task, SuspendError
from taskturbine.taskturbine import ClaimedTask, WorkerInner

if TYPE_CHECKING:
    from taskturbine import TaskturbineApp

import logging

logger = logging.getLogger(__name__)


class TaskOutcome(enum.Enum):
    Complete = "complete"
    Suspend = "suspend"
    Failure = "failure"
    Missing = "missing"
    # Expects a payload of strbytes

    Fatal = "fatal"
    # Expects a payload of strbytes


@dataclasses.dataclass
class TaskResult:
    outcome: TaskOutcome
    run_id: str
    payload: bytes | None = None
    duration: timedelta | None = None


def load_app(app_module: str) -> TaskturbineApp:
    # Need for assertion, but TYPE_CHECKING guard above hides runtime error.
    from taskturbine import TaskturbineApp

    if ":" not in app_module:
        raise ValueError("Invalid module name. Expected app.tasks.runtime:app format")
    (module_name, var_name) = app_module.split(":", 2)
    module = importlib.import_module(module_name)
    if not hasattr(module, var_name):
        raise ValueError(f"Could not access `{var_name}` in {module_name}")
    app = getattr(module, var_name)
    assert isinstance(app, TaskturbineApp), f"`{var_name}` must be a TaskturbineApp instance"
    return app


def worker_execute_task(app_module: str, claimed: ClaimedTask) -> TaskResult:
    """
    Import the application module, and then execute the task.

    These concerns are separated to make testing simpler.
    """
    try:
        app = load_app(app_module)
    except Exception as e:
        logger.exception(f"Could not import `{app_module}`")
        return TaskResult(outcome=TaskOutcome.Fatal, run_id=claimed.run_id, payload=str(e).encode())

    return execute_task(app, claimed)


def execute_task(app: TaskturbineApp, claimed: ClaimedTask) -> TaskResult:
    """
    Actually execute the task.

    Requires a reference to the application so that registered tasks, and `create_context()`
    can be accessed safely.
    """
    if not app.has_task(claimed.task_name):
        logger.warning(f"Task with {claimed.task_name} is not registered")
        return TaskResult(outcome=TaskOutcome.Missing, run_id=claimed.run_id, payload=claimed.task_name.encode())

    task_fn = app.get_task(claimed.task_name)
    context = app.create_context(claimed)
    try:
        # Call userland code
        res = task_fn(context)
        res_bytes = b""
        if res is not None:
            res_bytes = context._serialize(res)
        return TaskResult(outcome=TaskOutcome.Complete, run_id=claimed.run_id, payload=res_bytes)
    except SuspendError as suspend:
        return TaskResult(outcome=TaskOutcome.Suspend, duration=suspend.duration, run_id=claimed.run_id)
    except Exception as fail:
        # TODO Once we have the error handler on app, we can use it to call the error handler.
        retry_at = claimed.next_retry_in()
        return TaskResult(outcome=TaskOutcome.Failure, duration=retry_at, run_id=claimed.run_id)


class Worker:
    """
    Used to operate a worker.

    Workers are best created by TaskturbineApp.create_worker()
    as Worker depends on rust internals.
    """

    def __init__(
        self,
        inner: WorkerInner,
        tasks: Mapping[str, Task[..., Any]],
        context_factory: Callable[[ClaimedTask], TaskContext],
        error_handler: Callable[[Exception], None] | None = None,
    ) -> None:
        self._inner = inner
        self._tasks = tasks
        self._context_factory = context_factory
        self._error_handler = error_handler

    def run(self) -> None:
        """
        Run the worker run loop.

        Intended to run in a while loop that the application
        starts. Will periodically sleep based on Config.
        """
        last_cleanup = time.time() - 1
        app_module = self._inner.app_module

        # TODO fix hardcoded maxtasksperchild value
        # start process pool to receive work.
        logger.debug("Starting worker processes")
        with Pool(processes=self._inner.worker_concurrency, maxtasksperchild=1000) as pool:
            futures: list[AsyncResult[TaskResult]] = []
            inflight_count = self._inner.worker_concurrency * 2

            while True:
                # We want to limit the inflght amount of work to avoid over-aquiring work
                # which impacts throughput.
                if len(futures) < inflight_count:
                    # Fetch a batch of tasks and send the tasks to the worker pool.
                    claimed_tasks = self._inner.claim_tasks()
                    count = len(claimed_tasks)
                    logger.debug(f"Claimed {count} tasks")
                    if count == 0:
                        logger.info("no tasks claimed. Sleeping.")
                        time.sleep(self._inner.worker_sleep_secs)
                        continue

                    for claimed in claimed_tasks:
                        fut = pool.apply_async(worker_execute_task, (app_module, claimed, ))
                        futures.append(fut)

                keep: list[AsyncResult[TaskResult]] = []
                for fut in futures:
                    if fut.ready():
                        self._process_result(fut.get())
                    else:
                        keep.append(fut)
                futures = keep

                if self._inner.should_run_cleanup(int(last_cleanup)):
                    self._inner.run_cleanup()
                    last_cleanup = time.time()

    def _process_result(self, task_result: TaskResult) -> None:
        """
        Apply the TaskResult to the worker inner & storage layer
        """
        logger.debug("Processing result for {task_result.run_id}")
        match task_result.outcome:
            case TaskOutcome.Fatal:
                message = "unknown"
                if task_result.payload:
                    message = task_result.payload.decode()
                logger.warning(f"Worker crashed with: {message}")
            case TaskOutcome.Missing:
                message = "unknown"
                if task_result.payload:
                    message = task_result.payload.decode()
                logger.warning(f"Task with name {message} was not registered")
            case TaskOutcome.Complete:
                self._inner.complete_run(task_result.run_id, task_result.payload or b"")
            case TaskOutcome.Suspend:
                duration = task_result.duration
                if not duration:
                    logger.debug("Task suspended/waiting run_id={task_result.run_id}")
                    return
                else:
                    logger.debug(
                        "Task suspended for {duration.total_seconds()} seconds run_id={task_result.run_id}"
                    )
                    self._inner.schedule_run(task_result.run_id, duration)
            case TaskOutcome.Failure:
                assert task_result.duration, "Failures should always have duration"
                self._inner.fail_run(task_result.run_id, task_result.duration)

    def run_cleanup(self) -> None:
        """
        Run a worker cleanup loop.

        Intended to run in a while loop that the application
        starts. Will periodically sleep based on Config.
        """
        interval = self._inner.worker_cleanup_interval_secs
        while True:
            self._inner.run_cleanup()
            time.sleep(interval)

    def claim_tasks(self) -> list[ClaimedTask]:
        return self._inner.claim_tasks()

    def execute_batch(self) -> None:
        """Deprecated: Working towards using the multiprocessing worker loop"""
        claimed_tasks = self._inner.claim_tasks()
        for claimed in claimed_tasks:
            self.execute_task(claimed)

    def execute_task(self, claimed: ClaimedTask) -> None:
        if claimed.task_name not in self._tasks:
            logger.warning(f"Task with {claimed.task_name} is not registered")
            return

        task_fn = self._tasks[claimed.task_name]
        context = self._context_factory(claimed)
        try:
            res = task_fn(context)
            res_bytes = b""
            if res is not None:
                res_bytes = context._serialize(res)
            self._inner.complete_run(claimed.run_id, res_bytes)
        except SuspendError as suspend:
            duration = suspend.duration
            if not duration:
                logger.debug("Task suspended/waiting run_id={claimed.run_id}")
                return
            else:
                logger.debug(
                    "Task suspended for {duration.total_seconds()} seconds run_id={claimed.run_id}"
                )
                self._inner.schedule_run(claimed.run_id, duration)
        except Exception as fail:
            if self._error_handler:
                self._error_handler(fail)
            else:
                logger.error(
                    f"Task run failed task_id={claimed.task_id} run_id={claimed.run_id}"
                )
                logger.exception(fail)

            retry_at = claimed.next_retry_in()
            self._inner.fail_run(claimed.run_id, retry_at)
