from __future__ import annotations

import enum
import dataclasses
import importlib
import json
import logging
import queue
import threading
import time
from datetime import timedelta
from multiprocessing.pool import AsyncResult, Pool
from typing import Any, Callable, Mapping, TYPE_CHECKING

from taskturbine.context import TaskContext
from taskturbine.models import Task, SuspendError
from taskturbine.taskturbine import ClaimedTask, WorkerInner

if TYPE_CHECKING:
    from taskturbine import TaskturbineApp, ClaimedTaskDict


logger = logging.getLogger(__name__)


class TaskOutcome(enum.Enum):
    Complete = "complete"
    Suspend = "suspend"
    Failure = "failure"
    Missing = "missing"
    """The missing outcome expects a payload of bytes"""

    Fatal = "fatal"
    """The fatal outcome expects a payload of bytes"""


@dataclasses.dataclass
class TaskResult:
    outcome: TaskOutcome
    run_id: str
    payload: bytes | None = None
    duration: timedelta | None = None


def load_app(app_module: str) -> TaskturbineApp:
    # Need for assertion, but TYPE_CHECKING guard above hides runtime error.
    from . import TaskturbineApp
    from .asynclib import AsyncTaskturbineApp

    if ":" not in app_module:
        raise ValueError("Invalid module name. Expected app.tasks.runtime:app format")
    (module_name, var_name) = app_module.split(":", 2)
    module = importlib.import_module(module_name)
    if not hasattr(module, var_name):
        raise ValueError(f"Could not access `{var_name}` in {module_name}")
    app = getattr(module, var_name)

    assert isinstance(app, TaskturbineApp, AsyncTaskturbineApp), (
        f"`{var_name}` must be a TaskturbineApp instance"
    )
    return app


def worker_execute_task(app_module: str, claimed: ClaimedTaskDict) -> TaskResult:
    """
    Import the application module, and then execute the task.

    These concerns are separated to make testing simpler.
    """
    try:
        app = load_app(app_module)
    except Exception as e:
        logger.exception(f"Could not import `{app_module}`")
        return TaskResult(
            outcome=TaskOutcome.Fatal,
            run_id=claimed.get("run_id", "unknown"),
            payload=str(e).encode(),
        )

    claimed_task = ClaimedTask.from_dict(claimed)
    return execute_task(app, claimed_task)


def execute_task(app: TaskturbineApp, claimed: ClaimedTask) -> TaskResult:
    """
    Actually execute the task.

    Requires a reference to the application so that registered tasks, and `create_context()`
    can be accessed safely.
    """
    if not app.has_task(claimed.task_name):
        logger.warning(f"Task with {claimed.task_name} is not registered")
        return TaskResult(
            outcome=TaskOutcome.Missing,
            run_id=claimed.run_id,
            payload=claimed.task_name.encode(),
        )

    task_fn = app.get_task(claimed.task_name)
    context = app.create_context(claimed)
    try:
        # Call userland code
        res = task_fn(context)
        res_bytes = b""
        if res is not None:
            res_bytes = context._serialize(res)
        return TaskResult(
            outcome=TaskOutcome.Complete, run_id=claimed.run_id, payload=res_bytes
        )
    except SuspendError as suspend:
        return TaskResult(
            outcome=TaskOutcome.Suspend,
            duration=suspend.duration,
            run_id=claimed.run_id,
        )
    except Exception as fail:
        logger.exception("Task execution failed")
        retry_at = claimed.next_retry_in()
        if app.error_handler:
            app.error_handler(fail)
        return TaskResult(
            outcome=TaskOutcome.Failure, duration=retry_at, run_id=claimed.run_id
        )


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
        self._claimed_tasks: queue.Queue[dict[str, Any]] = queue.Queue(
            maxsize=inner.worker_concurrency
        )
        self._task_results: queue.Queue[TaskResult] = queue.Queue(
            maxsize=inner.worker_concurrency
        )
        self._shutdown = threading.Event()
        self._inflight: list[AsyncResult[TaskResult]] = []

    def _make_claim_thread(self) -> threading.Thread:
        """
        Create a thread that claims tasks

        claimed tasks are consumed by the main thread and dispatched
        to worker child processes.
        """

        def run_claim_thread(
            shutdown: threading.Event,
            claim_queue: queue.Queue[ClaimedTaskDict],
            inner: WorkerInner,
        ):
            last_fetch = None
            while True:
                # During graceful shutdown we want to immediately
                # stop claiming tasks so that inflight work can drain out.
                if shutdown.is_set():
                    logger.debug("claim_queue receive shutdown")
                    break

                if claim_queue.full():
                    logger.debug("claim_queue full")
                    time.sleep(0.1)
                    continue

                now = time.time()
                if last_fetch and now - last_fetch < 1:
                    logger.debug("claim last_fetch rate limit")
                    time.sleep(0.2)
                    continue

                claimed_tasks = inner.claim_tasks()
                logger.debug(f"claimed {len(claimed_tasks)} tasks")
                last_fetch = now
                for item in claimed_tasks:
                    claim_queue.put(item.to_dict())

        claim_thread = threading.Thread(
            name="claim-tasks",
            target=run_claim_thread,
            args=(self._shutdown, self._claimed_tasks, self._inner),
            daemon=True,
        )
        return claim_thread

    def _make_result_thread(self) -> threading.Thread:
        """
        Create a thread that commits results from the child process worker pool.

        results are read from the queue and commit.
        """

        def run_result_thread(
            shutdown: threading.Event,
            result_queue: queue.Queue[TaskResult],
            inner: WorkerInner,
        ):
            while True:
                try:
                    task_result = result_queue.get(timeout=1.0)
                except queue.Empty:
                    # Graceful shutdown drains all results.
                    if shutdown.is_set():
                        logger.debug("result-tasks receive shutdown")
                        break
                    logger.info("result_queue.get empty timeout")
                    time.sleep(0.2)
                    continue

                logger.debug(f"Processing result for {task_result.run_id}")
                match task_result.outcome:
                    case TaskOutcome.Fatal:
                        message = "unknown"
                        if task_result.payload:
                            message = task_result.payload.decode()
                        reason_message = f"Worker crashed with: {message}"
                        inner.fail_run(
                            task_result.run_id,
                            json.dumps({"reason": reason_message}),
                            None,
                        )
                        logger.warning(reason_message)
                    case TaskOutcome.Missing:
                        message = "unknown"
                        if task_result.payload:
                            message = task_result.payload.decode()
                        reason_message = f"Task with name {message} was not registered"
                        inner.fail_run(
                            task_result.run_id,
                            json.dumps({"reason": reason_message}),
                            None,
                        )
                        logger.warning(reason_message)
                    case TaskOutcome.Complete:
                        inner.complete_run(
                            task_result.run_id, task_result.payload or b""
                        )
                    case TaskOutcome.Suspend:
                        duration = task_result.duration
                        if not duration:
                            logger.debug(
                                "Task suspended/waiting run_id={task_result.run_id}"
                            )
                        else:
                            logger.debug(
                                "Task suspended for {duration.total_seconds()} seconds run_id={task_result.run_id}"
                            )
                            inner.schedule_run(task_result.run_id, duration)
                    case TaskOutcome.Failure:
                        inner.fail_run(
                            task_result.run_id,
                            json.dumps({"reason": "failure outcome"}),
                            task_result.duration,
                        )

                result_queue.task_done()

        result_thread = threading.Thread(
            name="result-tasks",
            target=run_result_thread,
            args=(self._shutdown, self._task_results, self._inner),
            daemon=True,
        )
        return result_thread

    def run(self, stop_on_idle: bool = False) -> None:
        """
        Run the worker run loop.

        Intended to run in a while loop that the application
        starts. Will periodically sleep based on Config.

        :param stop_on_idle: Set to true to have run() break its loop when
          there are no more tasks fetched.
        """
        last_cleanup = time.time() - 1
        app_module = self._inner.app_module

        self._claim_thread = self._make_claim_thread()
        self._claim_thread.start()

        self._result_thread = self._make_result_thread()
        self._result_thread.start()

        # start process pool to receive work.
        logger.debug("Starting worker processes")
        with Pool(
            processes=self._inner.worker_concurrency,
            maxtasksperchild=self._inner.worker_max_tasks_per_child,
        ) as pool:
            while True:
                try:
                    claimed = self._claimed_tasks.get(timeout=1.0)
                except queue.Empty:
                    logger.debug("claimed_tasks.get() empty timeout")
                    claimed = None

                if claimed:
                    fut = pool.apply_async(
                        worker_execute_task,
                        (app_module, claimed),
                    )
                    self._inflight.append(fut)
                    self._claimed_tasks.task_done()

                remaining = self._poll_inflight()

                # If this worker is shutting down wait until
                # all inflight work is complete.
                if remaining == 0:
                    if stop_on_idle:
                        logger.info("all work complete, and idle reached")
                        return self.shutdown()

                    if self._shutdown.is_set():
                        break

                if not self._shutdown.is_set() and self._inner.should_run_upkeep(
                    int(last_cleanup)
                ):
                    logger.debug("run_upkeep start")
                    self._inner.run_upkeep()
                    last_cleanup = time.time()

    def _poll_inflight(self) -> int:
        keep: list[AsyncResult[TaskResult]] = []
        for fut in self._inflight:
            if fut.ready():
                self._task_results.put(fut.get())
            else:
                keep.append(fut)
        self._inflight = keep

        return len(keep)

    def shutdown(self) -> None:
        logger.info("shutting down")

        # Trigger thread shutdown
        self._shutdown.set()

        logger.debug("waiting for claim-thread shutdown")
        self._claim_thread.join()

        while True:
            waiting = self._poll_inflight()
            logger.debug(f"Waiting on {waiting} inflight tasks")
            if waiting > 0:
                time.sleep(0.5)
            else:
                break

        logger.debug("waiting for result-thread shutdown")
        self._result_thread.join()

    def _process_result(self, task_result: TaskResult) -> None:
        """
        Apply the TaskResult to the worker inner & storage layer
        """
        logger.debug(f"Processing result for {task_result.run_id}")
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

    def run_upkeep(self) -> None:
        """
        Run a worker cleanup loop.

        Intended to run in a while loop that the application
        starts. Will periodically sleep based on Config.
        """
        interval = self._inner.worker_upkeep_interval_secs
        while True:
            self._inner.run_upkeep()
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
            fail_reason = json.dumps({"reason": str(fail)}).encode()
            self._inner.fail_run(claimed.run_id, fail_reason, retry_at)
