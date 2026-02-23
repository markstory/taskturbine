import enum
import dataclasses
import time
from datetime import timedelta
from multiprocessing.pool import AsyncResult, Pool
from typing import Any, Callable, Mapping

from taskturbine.context import TaskContext
from taskturbine.models import Task, SuspendError
from taskturbine.taskturbine import ClaimedTask, WorkerInner

import logging

logger = logging.getLogger(__name__)


class TaskOutcome(enum.Enum):
    Complete = "complete"
    Suspend = "suspend"
    Failure = "failure"
    Missing = "missing"


@dataclasses.dataclass
class TaskResult:
    outcome: TaskOutcome
    run_id: str
    payload: bytes | None = None
    duration: timedelta | None = None


def worker_execute_task(claimed: ClaimedTask) -> TaskResult:
    """
    I really need a reference to the application module and symbol
    so that I can load it and use it. Hell, I could import it here,
    and rely on import cache.

    The client application needs to provide a module path / symbol.

    inspect.getmodule(object) could be useful!
    The client application would need to pass a startup initializer to load state
    and crap. Would need to test this with a django application setup.
    """
    _tasks: dict[str, Callable[..., Any]] = {}

    # How do I get the app here?
    if claimed.task_name not in _tasks:
        logger.warning(f"Task with {claimed.task_name} is not registered")
        return TaskResult(outcome=TaskOutcome.Missing, run_id=claimed.run_id)

    task_fn = _tasks[claimed.task_name]
    context = _context_factory(claimed)
    try:
        res = task_fn(context)
        res_bytes = b""
        if res is not None:
            res_bytes = context._serialize(res)
        return TaskResult(outcome=TaskOutcome.Complete, run_id=claimed.run_id, payload=res_bytes)
    except SuspendError as suspend:
        return TaskResult(outcome=TaskOutcome.Suspend, duration=suspend.duration, run_id=claimed.run_id)
    except Exception as fail:
        # TODO Once we have the app, we can use it to call the error handler.
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

        # TODO fix hardcoded maxtasksperchild value
        # start process pool to receive work.
        with Pool(processes=self._inner.worker_concurrency, maxtasksperchild=1000) as pool:
            futures: list[AsyncResult[TaskResult]] = []
            inflight_count = self._inner.worker_concurrency * 2
            while True:
                if len(futures) < inflight_count:
                    # Fetch a batch of tasks and send the tasks to the worker pool.
                    claimed_tasks = self._inner.claim_tasks()
                    if len(claimed_tasks) == 0:
                        logger.info("no tasks claimed. Sleeping.")
                        time.sleep(self._inner.worker_sleep_secs)
                        continue

                    for claimed in claimed_tasks:
                        fut = pool.apply_async(worker_execute_task, (claimed, ))
                        futures.append(fut)

                keep: list[AsyncResult[TaskResult]] = []
                for fut in futures:
                    if fut.ready():
                        self._process_result(fut.get())
                    else:
                        keep.append(fut)
                futures = keep

                if self._inner.should_run_cleanup(last_cleanup):
                    self._inner.run_cleanup()
                    last_cleanup = time.time()

    def _process_result(self, task_result: TaskResult) -> None:
        """
        Apply the TaskResult to the worker inner & storage layer
        """
        match task_result.outcome:
            case TaskOutcome.Missing:
                # TODO how to get taskname here?
                logger.warning("Task with was not registered")
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
