"""
Taskturbine python SDK

This module contains the python components of the taskturbine
durable function framework. While all the IO operations are built
with rust, the parts of tasks that interact directly with your code
are in python.
"""

import abc
import logging
from typing import (
    Any,
    Callable,
    MutableMapping,
    ParamSpec,
    TypeVar,
)

# Import from the rust library
from .taskturbine import (
    AppInner,
    ClaimedTask,
    Config,
    SpawnResult,
    TaskOptions,
)
from .context import TaskContext
from .models import Task
from .serializer import TaskSerializer, JsonSerializer
from .worker import Worker

__all__ = [
    "Config",
    "JsonSerializer",
    "TaskturbineApp",
    "Task",
    "TaskContext",
    "TaskSerializer",
    "TaskOptions",
    "Worker",
]

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)


class BaseApp(abc.ABC):
    """Abstract base class for App implementations"""

    def __init__(
        self,
        serializer: TaskSerializer | None = None,
        error_handler: Callable[[Exception], None] | None = None,
    ) -> None:
        self._default_spawn_options = TaskOptions(
            max_attempts=5,
            retry_seconds=30,
            retry_factor=1.0,
            retry_max_seconds=300,
            cancellation_max_age=86400,
            idempotency_key=None,
        )
        self.error_handler = error_handler
        if serializer is None:
            serializer = JsonSerializer()
        self.serializer = serializer

    def set_spawn_options(
        self,
        *,
        headers: dict[str, str] | None = None,
        max_attempts: int | None = None,
        retry_seconds: int | None = None,
        retry_factor: float | None = None,
        retry_max_seconds: int | None = None,
        cancellation_max_age: int | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        """
        Update the default options that are used to spawn tasks.
        """
        self._default_spawn_options = self._default_spawn_options.copy_with(
            headers=headers,
            max_attempts=max_attempts,
            retry_seconds=retry_seconds,
            retry_factor=retry_factor,
            retry_max_seconds=retry_max_seconds,
            cancellation_max_age=cancellation_max_age,
            idempotency_key=idempotency_key,
        )

    def serialize_value(self, params: dict[str, Any]) -> bytes:
        """Convert parameters into bytes"""
        return self.serializer.serialize(params)

    def deserialize_value(self, blob: bytes) -> Any | None:
        """Convert a bytestring into a decoded value"""
        if blob == b"":
            return None
        return self.serializer.deserialize(blob)


class TaskturbineApp(BaseApp):
    """
    The entry point to defining and executing tasks.

    Your application should create a `TaskturbineApp` instance
    using `Config` to define preferred behavior.

    Then you need to register your tasks, and include all the modules
    that include your tasks. Your tasks define all the tasks you want to spawn.

    Spawning tasks can be done with `spawn_task()`. You can run a Worker to execute tasks with
    `create_worker()`. The worker will draw from the application config. You can run many workers
    concurrently, to process larger workloads.

    At a large enough scale, you'll want to move cleanup operations to a dedicated worker.
    Use `Worker.run_cleanup()`.
    """

    def __init__(
        self,
        config: Config,
        serializer: TaskSerializer | None = None,
        error_handler: Callable[[Exception], None] | None = None,
    ) -> None:
        self._inner = AppInner(config)
        self._tasks: MutableMapping[str, Task[..., Any]] = {}
        super().__init__(serializer, error_handler)

    def add_channel(self, name: str) -> None:
        """
        Add a channel that tasks can be spawned on.

        Channels let you separate backlogs and worker pools
        """
        self._inner.add_channel(name)

    @property
    def channels(self) -> set[str]:
        """Get the list of channels"""
        return self._inner.channels

    def set_channels(self, names: list[str]) -> None:
        """
        Define the set of channels overwriting any defined channel names.
        """
        self._inner.channels.clear()
        for name in names:
            self._inner.add_channel(name)

    def has_task(self, name: str) -> bool:
        """Check if a task is defined"""
        return name in self._tasks

    def get_task(self, name: str) -> Task[..., Any]:
        """Get a task by name. Raises KeyError on unknown values"""
        return self._tasks[name]

    def update_schema(self) -> None:
        """
        Create or update the taskturbine schema and tables.
        """
        self._inner.update_schema()

    def register_task(
        self,
        name: str,
        *,
        options: TaskOptions | None = None,
    ) -> Callable[[Callable[P, R]], Task[P, R]]:
        """
        Decorator to register task functions.

        Tasks are expected to implement a signature of:

        ```
        def func_name(context: TaskContext) -> models.JsonData | None
        ```

        The `context` parameter enables you to use :py:class:`TaskContext`
        to define steps and then call your steps within your flow control
        logic.
        """

        def wrapped(func: Callable[P, R]) -> Task[P, R]:
            task = Task(name=name, func=func, options=options)
            self._tasks[name] = task
            return task

        return wrapped

    def spawn_task(
        self,
        taskname: str,
        params: dict[str, Any],
        *,
        channel: str | None = None,
        headers: dict[str, str] | None = None,
        max_attempts: int | None = None,
        retry_seconds: int | None = None,
        retry_factor: float | None = None,
        retry_max_seconds: int | None = None,
        cancellation_max_age: int | None = None,
        idempotency_key: str | None = None,
    ) -> SpawnResult:
        """
        Spawn a task to be run later by a worker.

        :param taskname: The name of the task to run.
        :param channel: The channel to spawn the task on.
        :param headers: An dict of headers to send with the task. These can be used by application logic.
        :param max_attempts: The maximum number of attempts.
        :param retry_seconds: The number of seconds to add between each retry.
        :param retry_factor: The scaling factor applied to retry_seconds to grow seconds.
        :param retry_max_seconds: The maximum number of seconds that a retry can be.
        :param cancellation_max_age: The age after which a task is cancelled.
        :param idempotency_key: A key to make spawn_task idempotent.
        :return: Details about the spawned task.

        The headers, max_attempts, retry_seconds, retry_factors, retry_max_seconds, and
        cancellation_max_age parameters are inherited from the task default options or default task
        options.
        """
        if taskname not in self._tasks:
            raise ValueError(f"The task `{taskname}` is not registered.")
        task = self._tasks[taskname]
        base_options = self._default_spawn_options
        if task.options:
            base_options = task.options

        options = base_options.copy_with(
            headers=headers,
            max_attempts=max_attempts,
            retry_seconds=retry_seconds,
            retry_factor=retry_factor,
            retry_max_seconds=retry_max_seconds,
            cancellation_max_age=cancellation_max_age,
            idempotency_key=idempotency_key,
        )
        if channel:
            return self._inner.channel_spawn_task(
                channel, taskname, self.serialize_value(params), options
            )
        else:
            return self._inner.spawn_task(
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
        self._inner.emit_event(event_name, self.serialize_value(payload))

    def create_context(self, claimed_task: ClaimedTask) -> TaskContext:
        """
        Create a TaskContext with links to the rust context.
        """
        context = TaskContext(
            inner=self._inner.create_context(claimed_task),
            serialize=self.serialize_value,
            deserialize=self.deserialize_value,
        )
        return context

    def create_worker(
        self,
        worker_id: str,
        channels: list[str],
    ) -> Worker:
        """
        Create a Worker that is connected to Rust storage API.
        """
        worker = Worker(
            inner=self._inner.create_worker(worker_id, channels),
            tasks=self._tasks,
            context_factory=self.create_context,
            error_handler=self.error_handler,
        )
        return worker
