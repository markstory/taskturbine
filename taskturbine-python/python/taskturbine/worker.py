from datetime import timedelta
from typing import Any, Callable, Mapping

from taskturbine.context import TaskContext
from taskturbine.models import Task, SuspendError
from taskturbine.taskturbine import ClaimedTask, WorkerInner
import logging

logger = logging.getLogger(__name__)


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
        interval = self._inner.worker_sleep_secs
        last_cleanup = time.time() - 1

        while True:
            self.execute_batch()
            time.sleep(interval)
            if self._inner.should_run_cleanup(last_cleanup):
                self._inner.run_cleanup()
                last_cleanup = time.time()

    def run_cleanup(self) -> None:
        """
        Run a worker cleanup loop.

        Intended to run in a while loop that the application
        starts. Will periodically sleep based on Config.
        """
        interval = self._inner.cleanup_interval_secs
        while True:
            self._inner.run_cleanup()
            time.sleep(interval)

    def claim_tasks(self) -> list[ClaimedTask]:
        return self._inner.claim_tasks()

    def execute_batch(self) -> None:
        claimed_tasks = self._inner.claim_tasks()
        # TODO - Use multiprocessing to execute tasks in parallel
        # The number of processes should == worker_concurrency
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
