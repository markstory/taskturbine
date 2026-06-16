"""
Thread & Multiprocess based worker.

For more CPU heavy workloads, multiprocessing + threads yield better utilization.
"""

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

from taskturbine.metrics import MetricsBackend, NoopMetrics
from taskturbine.context import TaskContext
from taskturbine.models import Task, SuspendError, ClaimedTaskDict
from taskturbine.taskturbine import ClaimedTask, Config, WorkerInner

if TYPE_CHECKING:
    from taskturbine import TaskturbineApp


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

    if ":" not in app_module:
        raise ValueError("Invalid module name. Expected app.tasks.runtime:app format")
    (module_name, var_name) = app_module.split(":", 2)
    module = importlib.import_module(module_name)
    if not hasattr(module, var_name):
        raise ValueError(f"Could not access `{var_name}` in {module_name}")
    app = getattr(module, var_name)

    assert isinstance(app, TaskturbineApp), (
        f"`{app_module}` must be a TaskturbineApp instance"
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
    tags = _metrics_tags(app._inner.config.usecase, claimed)
    app.metrics.incr("worker.execute_task", 1, tags)

    if not app.has_task(claimed.task_name):
        app.metrics.incr("worker.execute_task.not_found", 1, tags)
        logger.warning(f"Task with {claimed.task_name} is not registered")
        return TaskResult(
            outcome=TaskOutcome.Missing,
            run_id=claimed.run_id,
            payload=claimed.task_name.encode(),
        )

    task_fn = app.get_task(claimed.task_name)
    context = app.create_context(claimed)
    start = time.monotonic()
    try:
        # Call userland code
        res = task_fn(context)
        res_bytes = b""
        if res is not None:
            res_bytes = context._serialize(res)
        tags["outcome"] = str(TaskOutcome.Complete)
        app.metrics.incr("worker.execute_task.outcome", 1, tags)

        return TaskResult(
            outcome=TaskOutcome.Complete, run_id=claimed.run_id, payload=res_bytes
        )
    except SuspendError as suspend:
        logger.debug("Task suspended")
        tags["outcome"] = str(TaskOutcome.Suspend)
        app.metrics.incr("worker.execute_task.outcome", 1, tags)

        return TaskResult(
            outcome=TaskOutcome.Suspend,
            duration=suspend.duration,
            run_id=claimed.run_id,
        )
    except Exception as fail:
        logger.exception("Task execution failed")
        tags["outcome"] = str(TaskOutcome.Failure)
        app.metrics.incr("worker.execute_task.outcome", 1, tags)

        retry_at = claimed.next_retry_in()
        if app.error_handler:
            app.error_handler(fail)
        return TaskResult(
            outcome=TaskOutcome.Failure, duration=retry_at, run_id=claimed.run_id
        )
    finally:
        app.metrics.histogram(
            "worker.execute_task.call.duration", time.monotonic() - start, tags
        )


def _metrics_tags(usecase: str, claimed: ClaimedTask | None) -> dict[str, str]:
    tags = {
        "usecase": usecase,
    }
    if claimed:
        tags["channel"] = claimed.channel
        tags["taskname"] = claimed.task_name
    return tags


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
        metrics: MetricsBackend | None = None,
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
        self._metrics = metrics or NoopMetrics()

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
        ) -> None:
            last_fetch = None
            worker_sleep = self._inner.worker_sleep_ms / 1000
            while True:
                # During graceful shutdown we want to immediately
                # stop claiming tasks so that inflight work can drain out.
                if shutdown.is_set():
                    logger.debug("claim_queue receive shutdown")
                    break

                if claim_queue.full():
                    tags = _metrics_tags(self.usecase, None)
                    self.metrics.incr("worker.claim_queue.full", 1, tags)
                    # This could be another utilization metric to collect
                    logger.debug("claim_queue full, sleeping")
                    time.sleep(worker_sleep)
                    continue

                now = time.time()

                # If we missed, backoff for a bit
                if last_fetch and now - last_fetch < worker_sleep:
                    # This could be another utilization metric to collect
                    logger.debug("last fetch was less than %ss, sleeping", worker_sleep)
                    time.sleep(worker_sleep)
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
        ) -> None:
            worker_sleep = self._inner.worker_sleep_ms / 1000
            while True:
                try:
                    task_result = result_queue.get(timeout=worker_sleep)
                except queue.Empty:
                    # Graceful shutdown drains all results.
                    if shutdown.is_set():
                        logger.debug("result-tasks receive shutdown")
                        break
                    # These sleeps would be a good place to collect utilization metrics.
                    logger.debug("result thread empty, sleeping")
                    time.sleep(worker_sleep)
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
                            json.dumps({"reason": reason_message}).encode(),
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
                            json.dumps({"reason": reason_message}).encode(),
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
                            json.dumps({"reason": "failure outcome"}).encode(),
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

    def run(self) -> None:
        """
        Run the worker run loop.

        Intended to run in a while loop that the application
        starts. Will periodically sleep based on Config.
        """
        last_cleanup = time.time() - 1
        app_module = self._inner.app_module

        self._claim_thread = self._make_claim_thread()
        self._claim_thread.start()

        self._result_thread = self._make_result_thread()
        self._result_thread.start()

        # start process pool to receive work.
        logger.debug("Starting worker %s processes", self._inner.worker_concurrency)
        worker_sleep = self._inner.worker_sleep_ms / 1000
        with Pool(
            processes=self._inner.worker_concurrency,
            maxtasksperchild=self._inner.worker_max_tasks_per_child,
        ) as pool:
            while True:
                try:
                    claimed = self._claimed_tasks.get(timeout=worker_sleep)
                except queue.Empty:
                    # This could be another utilization metric to collect
                    logger.debug("claimed_tasks.get() empty timeout")
                    claimed = None

                if claimed:
                    fut = pool.apply_async(
                        worker_execute_task,
                        (app_module, claimed),
                    )
                    self._inflight.append(fut)
                    self._claimed_tasks.task_done()

                running = self._poll_inflight()

                # If this worker is shutting down wait until
                # all inflight work is complete.
                if running == 0 and not claimed:
                    time.sleep(self._inner.worker_sleep_ms / 1000)

                # If all workers appear idle
                if running == 0 and self._inner.should_shutdown():
                    logger.info("all work complete, and idle reached")
                    return self.shutdown()

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

        return len(self._inflight)

    def shutdown(self) -> None:
        logger.info("shutting down")

        # Trigger thread shutdown
        self._shutdown.set()

        if self._claim_thread:
            logger.debug("waiting for claim-thread shutdown")
            self._claim_thread.join()

        while True:
            waiting = self._poll_inflight()
            logger.debug(f"Waiting on {waiting} inflight tasks")
            if waiting > 0:
                time.sleep(0.5)
            else:
                break

        if self._result_thread:
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
                self._inner.fail_run(task_result.run_id, b"", task_result.duration)

    def run_upkeep(self) -> None:
        """
        Run a worker upkeep loop.

        The worker will run an upkeep operation each `Config.worker_upkeep_interval_secs`
        """
        interval = self._inner.worker_upkeep_interval_secs
        while True:
            self._inner.run_upkeep()
            time.sleep(interval)

    def claim_tasks(self) -> list[ClaimedTask]:
        return self._inner.claim_tasks()
