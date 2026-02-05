"""
Taskturbine python SDK

This module contains the python components of the taskturbine
durable function framework. While all the IO operations are built
with rust, the parts of tasks that interact directly with your code
are in python.
"""
from datetime import datetime, timedelta
from functools import update_wrapper
from typing import Any, Callable, Generic, Mapping, MutableMapping, ParamSpec, Self, TypeVar
import json

# Import from the rust library
from .taskturbine import Config, TaskOptions, SpawnResult, ClaimedTask
from .taskturbine import Task as TaskRs
from .taskturbine import TaskturbineApp as AppRs
from .taskturbine import ContextInner, WorkerInner

__all__ = ["Config", "TaskturbineApp"]

P = ParamSpec("P")
R = TypeVar("R")


class Task(Generic[P, R]):
    def __init__(
        self,
        name: str,
        func: Callable[P, R],
    ):
        self._func = func
        self.task_rs = TaskRs(module_name=func.__module__, task_name=name)
        update_wrapper(self, func)

    @property
    def name(self) -> str:
        return self.task_rs.task_name

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Call the task function immediately.
        """
        return self._func(*args, **kwargs)


class TaskContext:
    def __init__(
        self,
        inner: ContextInner,
        serialize: Callable[[dict[str, Any]], bytes],
        deserialize: Callable[[bytes], dict[str, Any] | None],
    ) -> None:
        self._inner = inner
        self._serialize = serialize
        self._deserialize = deserialize
        self._checkpoint_counters: dict[str, int] = {}

    def await_event(self, event_name: str, timeout: float|timedelta|None = None) -> dict[str, Any]:
        """
        Wait for an event. Will return the event payload if the event has been emit.
        If the event has not happened, a SuspendError will be raised.
        """
        timeout_secs = self._inner.await_event_default_timeout_secs()
        if isinstance(timeout, float):
            timeout_secs = timeout
        elif isinstance(timeout, timedelta):
            timeout_secs = timeout.total_seconds()
        assert timeout_secs
        wait = self._inner.get_event_payload(event_name, timeout_secs)
        if wait.should_suspend:
            raise SuspendError()
        return json.loads(wait.payload)

    def emit_event(self, event_name: str, payload: dict[str, Any]) -> None:
        """
        Record an external event that a task/run is waiting for.

        Payload can be an arbitrary JSON encodable value that
        can be retrieved later.
        """
        self._inner.emit_event(event_name, self._serialize(payload))

    def _checkpoint_name(self, step_name: str) -> str:
        """
        Resolve a step name into a checkpoint name.
        A task can contain steps with duplicate names, and each
        instance of a name needs to resolve to a distinct checkpoint
        """
        if step_name not in self._checkpoint_counters:
            self._checkpoint_counters[step_name] = 0
        self._checkpoint_counters[step_name] += 1
        value = self._checkpoint_counters[step_name]
        if value == 1:
            return step_name
        return f"{step_name}#{value}"

    def sleep_for(self, step_name: str, duration: timedelta):
        """
        Pause the current task until `duration` has elapsed.

        Will create a checkpoint, and raise a SuspendError with
        the duration the current task should sleep for.
        """
        checkpoint_name = self._checkpoint_name(step_name)
        try:
            self._inner.get_checkpoint(checkpoint_name)
            return 
        except ValueError:
            # An exception here means that the checkpoint was not found.
            pass
        self._inner.set_checkpoint(checkpoint_name, step_name.encode(), None)

        raise SuspendError(duration=duration)

    def step(self, step_name: str, func: Callable[[Self], dict[str, Any] | None]) -> dict[str, Any] | None:
        """
        Run a durable step

        Create a step with the given name. If a name is used multiple times, a suffix
        will be added based on call order.

        If the step has been completed the captured state will be used. If the step raises an error
        it will be considered 'failed' and a retry will be scheduled according to the task's retry
        configuration.
        """
        checkpoint_name = self._checkpoint_name(step_name)
        try:
            checkpoint = self._inner.get_checkpoint(checkpoint_name)
            return self._deserialize(checkpoint.state)
        except ValueError:
            # No checkpoint data.
            pass

        try:
            step_result = func(self)
        except Exception as err:
            # Step failed, record failure
            raise StepFailed()

        result_bytes = b""
        if step_result:
            result_bytes = self._serialize(step_result)
        self._inner.set_checkpoint(checkpoint_name, result_bytes, None)

        return step_result


class Worker:
    def __init__(
        self,
        inner: WorkerInner,
        tasks: Mapping[str, Task],
    ) -> None:
        self._inner = inner
        self._tasks = tasks

    def start(self):
        """
        Start the worker run loop
        """
        while True:
            self.run_once()

    def run_once(self):
        # claim a batch of tasks
        # run the batch of tasks sequentially
        # create context for task
        # call task
        # collect result
        # Report the results of all tasks to _inner.
        #
        # Figure out how multiprocessing could work.
        # Perhaps worker sends task to child, and child
        # sends result back, and all the pg interactions happen
        # in the parent.
        # Doing multiple processes will require threads for
        # io operations.
        ...



class TaskturbineApp:
    def __init__(self, config: Config) -> None:
        self._app_rs = AppRs(config)
        self._tasks: MutableMapping[str, Task] = {}

        # TODO add method to set default spawn options
        # Or define options per task that is registered.
        self._default_spawn_options = TaskOptions(
            max_attempts=5,
            retry_seconds=30,
            retry_factor=1.0,
            retry_max_seconds=300,
            cancellation_max_age=86400,
        )

    def add_channel(self, name: str) -> None:
        """
        Add a channel that tasks can be spawned on.

        Channels let you separate backlogs and worker pools
        """
        self._app_rs.add_channel(name)

    @property
    def channels(self) -> list[str]:
        """Get the list of channels"""
        return self._app_rs.channels

    def register_task(
        self,
        name: str
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """
        Decorator to register task functions.

        Tasks are expected to implement a signature of:

        ```
        def func_name(context: TaskContext) -> str | None
        ```
        """
        def wrapped(func: Callable[P, R]) -> Task[P, R]:
            task = Task(name=name, func=func)
            self._tasks[name] = task
            self._app_rs.register_task(task.task_rs)
            return task

        return wrapped

    def has_task(self, name: str) -> bool:
        """Check if a task is defined"""
        return self._app_rs.has_task(name)

    def get_task(self, name: str) -> Task:
        """Get a task by name. Raises KeyError on unknown values"""
        return self._tasks[name]

    def serialize_value(self, params: dict[str, Any]) -> bytes:
        """Convert parameters into bytes

        TODO make this a hook method so other serializers can be used.
        """
        return json.dumps(params).encode()

    def deserialize_value(self, blob: bytes) -> dict[str, Any] | None:
        """Convert a bytestring into a dict

        TODO make this a hook method so other serializers can be used.
        """
        if blob == b"":
            return None
        return json.loads(blob)

    def spawn_task(
        self,
        taskname: str,
        params: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
        max_attempts: int | None = None,
        retry_seconds: int | None = None,
        retry_factor: float | None = None,
        retry_max_seconds: int | None = None,
        cancellation_max_age: int | None = None,
    ) -> SpawnResult:
        """
        Spawn a task to be run later by a worker.
        """
        options = self._default_spawn_options.copy_with(
            headers=headers,
            max_attempts=max_attempts,
            retry_seconds=retry_seconds,
            retry_factor=retry_factor,
            retry_max_seconds=retry_max_seconds,
            cancellation_max_age=cancellation_max_age,
        )
        return self._app_rs.spawn_task(
            taskname, self.serialize_value(params), options
        )

    def emit_event(
        self,
        event_name: str,
        payload: dict[str, Any],
    ) -> None:
        """
        Record an external event that a task/run is waiting for.

        Payload can be an arbitrary JSON encodable value that
        can be retrieved later.
        """
        self._app_rs.emit_event(event_name, self.serialize_value(payload))

    def claim_task(
        self,
        channels: list[str],
        worker_id: str,
        claim_timeout: timedelta,
        qty: int,
   ) -> list[ClaimedTask]:
        """
        Claim one or more tasks for the provided worker_id
        """
        return self._app_rs.claim_task(channels, worker_id, claim_timeout, qty)

    def create_context(self, claimed_task: ClaimedTask) -> TaskContext:
        """
        Create a TaskContext with links to the rust context.
        """
        context = TaskContext(
            inner=self._app_rs.create_context(claimed_task),
            serialize=self.serialize_value,
            deserialize=self.deserialize_value,
        )
        return context

    def create_worker(self, worker_id: str, channels: list[str]) -> Worker:
        """
        Create a Worker that is connected to Rust storage API.
        """
        worker = Worker(
            inner=self._app_rs.create_worker(worker_id, channels),
            tasks=self._tasks,
        )
        return worker


class SuspendError(Exception):
    """Signal the worker runtime to suspend this task for its retry timeout, or sleep time"""

    def __init__(self, duration: timedelta | None = None) -> None:
        super().__init__("Task suspended")
        self.duration = duration



class StepFailed(Exception):
    """Signal that a step/task failed"""
    # TODO capture execution context
