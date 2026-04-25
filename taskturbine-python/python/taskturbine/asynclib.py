from __future__ import annotations
from datetime import timedelta
import functools
import json
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Mapping,
    MutableMapping,
    ParamSpec,
    TypeVar,
)
from taskturbine import BaseApp
from taskturbine.context import BaseContext
from taskturbine.models import JsonData, OptionalJsonData, SuspendError
from taskturbine.taskturbine import (
    AsyncContextInner,
    AsyncAppInner,
    AsyncWorkerInner,
    ClaimedTask,
    Config,
    SpawnResult,
    TaskOptions,
)
from taskturbine.serializer import JsonSerializer, TaskSerializer


P = ParamSpec("P")
R = TypeVar("R")


class AsyncTask(Generic[P, R]):
    def __init__(
        self,
        name: str,
        func: Callable[P, Awaitable[R]],
        options: TaskOptions | None = None,
    ):
        self.name = name
        self._func = func
        self.options = options

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Awaitable[R]:
        """
        Call the task function immediately.
        """
        return self._func(*args, **kwargs)


class AsyncTaskContext(BaseContext):
    """
    asyncio implementation of TaskContext
    """

    def __init__(
        self,
        inner: AsyncContextInner,
        serialize: Callable[[JsonData], bytes],
        deserialize: Callable[[bytes], JsonData | None],
    ) -> None:
        self._inner = inner
        self._serialize = serialize
        self._deserialize = deserialize
        super().__init__(inner.claimed_task)

    async def await_event(
        self, event_name: str, timeout: float | timedelta | None = None
    ) -> Any:
        """
        Wait for an event. Will return the event payload if the event has been
        emit. If the event has not happened, a SuspendError will be raised.
        """
        if timeout is None:
            timeout = self._inner.await_event_default_timeout_secs
        if isinstance(timeout, (float, int)):
            timeout = timedelta(seconds=timeout)
        assert isinstance(timeout, timedelta)

        wait = await self._inner.get_event_payload(event_name, timeout)
        if wait.should_suspend:
            raise SuspendError()
        return json.loads(wait.payload)

    async def emit_event(self, event_name: str, payload: JsonData) -> None:
        """
        Record an external event that a task/run is waiting for.

        Payload can be an arbitrary JSON encodable value that
        can be retrieved later.
        """
        return await self._inner.emit_event(event_name, self._serialize(payload))

    async def sleep_for(self, step_name: str, duration: timedelta) -> None:
        """
        Pause the current task until `duration` has elapsed.

        Will create a checkpoint, and raise a SuspendError with
        the duration the current task should sleep for.
        """
        checkpoint_name = self._checkpoint_name(step_name)
        try:
            await self._inner.get_checkpoint(checkpoint_name)
            return
        except ValueError:
            # An exception here means that the checkpoint was not found.
            pass
        await self._inner.set_checkpoint(checkpoint_name, step_name.encode(), None)

        raise SuspendError(duration=duration)

    def step(
        self, name: str
    ) -> Callable[
        [Callable[P, Awaitable[OptionalJsonData]]],
        Callable[P, Awaitable[OptionalJsonData]],
    ]:
        """
        Decorate a function as a durable step.

        Wrap a function with a decorator that makes it a durable step of the current task. The
        decorated function can then be called by application logic as required, giving you control
        of the parameters and return values.

        If the step's name is used more than once, a suffix will be added
        based on the order steps are defined.

        If the step has been completed the captured state from the completed run
        will be used. If the step raises an error the run is considered 'failed'
        and a retry will be scheduled according to the task's retry configuration.
        """
        checkpoint_name = self._checkpoint_name(name)

        def decorator(
            func: Callable[P, Awaitable[OptionalJsonData]],
        ) -> Callable[P, Awaitable[OptionalJsonData]]:
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> OptionalJsonData:
                return await self._execute_step(checkpoint_name, func, *args, **kwargs)

            functools.update_wrapper(wrapper, func)
            return wrapper

        return decorator

    async def step_run(
        self,
        step_name: str,
        func: Callable[P, Awaitable[OptionalJsonData]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> JsonData | None:
        """
        Run a function as a durable step

        Create a step with the given name. If a name is used multiple times, a suffix
        will be added based on call order.

        If the step has been completed the captured state will be used. If the step raises an error
        it will be considered 'failed' and a retry will be scheduled according to the task's retry
        configuration.
        """
        checkpoint_name = self._checkpoint_name(step_name)
        return await self._execute_step(checkpoint_name, func, *args, **kwargs)

    async def _execute_step(
        self,
        checkpoint_name: str,
        func: Callable[P, Awaitable[OptionalJsonData]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> JsonData | None:
        """
        Execute an async step function.
        """
        try:
            checkpoint = await self._inner.get_checkpoint(checkpoint_name)
            return self._deserialize(checkpoint.state)
        except ValueError:
            # No checkpoint data.
            pass

        # Step functions may raise, but worker.execute_task will catch
        step_result = await func(*args, **kwargs)

        result_bytes = b""
        if step_result:
            result_bytes = self._serialize(step_result)
        await self._inner.set_checkpoint(checkpoint_name, result_bytes, None)

        return step_result


class AsyncTaskturbineApp(BaseApp):
    """
    The entry point to defining and executing tasks in an async runtime.

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
        self._inner = AsyncAppInner(config)
        self._tasks: MutableMapping[str, AsyncTask[..., Any]] = {}
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

    def has_task(self, name: str) -> bool:
        """Check if a task is defined"""
        return name in self._tasks

    def get_task(self, name: str) -> AsyncTask[..., Any]:
        """Get a task by name. Raises KeyError on unknown values"""
        return self._tasks[name]

    async def update_schema(self) -> None:
        """
        Create or update the taskturbine schema and tables.
        """
        await self._inner.update_schema()

    def register_task(
        self,
        name: str,
        *,
        options: TaskOptions | None = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], AsyncTask[P, R]]:
        """
        Decorator to register task functions.

        Tasks are expected to implement a signature of:

        ```
        async def func_name(context: AsyncTaskContext) -> models.JsonData | None
        ```

        The `context` parameter enables you to use :py:class:`AsyncTaskContext`
        to define steps and then call your steps within your flow control
        logic.
        """

        def wrapped(func: Callable[P, Awaitable[R]]) -> AsyncTask[P, R]:
            task = AsyncTask(name=name, func=func, options=options)
            self._tasks[name] = task
            return task

        return wrapped

    async def spawn_task(
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
            return await self._inner.channel_spawn_task(
                channel, taskname, self.serialize_value(params), options
            )
        else:
            return await self._inner.spawn_task(
                taskname, self.serialize_value(params), options
            )

    async def emit_event(
        self,
        event_name: str,
        payload: dict[str, Any],
    ) -> None:
        """
        Record an external event that a task/run is waiting for.

        Payload can be an arbitrary JSON encodable value that
        can be retrieved later.
        """
        await self._inner.emit_event(event_name, self.serialize_value(payload))

    def create_context(self, claimed_task: ClaimedTask) -> AsyncTaskContext:
        """
        Create an AsyncTaskContext with links to the rust context.
        """
        context = AsyncTaskContext(
            inner=self._inner.create_context(claimed_task),
            serialize=self.serialize_value,
            deserialize=self.deserialize_value,
        )
        return context

    def create_worker(
        self,
        worker_id: str,
        channels: list[str],
    ) -> AsyncWorker:
        """
        Create a AsyncWorker that is connected to Rust storage API.
        """
        worker = AsyncWorker(
            inner=self._inner.create_worker(worker_id, channels),
            tasks=self._tasks,
            context_factory=self.create_context,
            error_handler=self.error_handler,
        )
        return worker


class AsyncWorker:
    """
    Used to operate a worker.

    Workers are best created by TaskturbineApp.create_worker()
    as Worker depends on rust internals.
    """

    def __init__(
        self,
        inner: AsyncWorkerInner,
        tasks: Mapping[str, AsyncTask[..., Any]],
        context_factory: Callable[[ClaimedTask], AsyncTaskContext],
        error_handler: Callable[[Exception], None] | None = None,
    ) -> None:
        self._inner = inner
        self._tasks = tasks
        self._context_factory = context_factory
        self._error_handler = error_handler

    async def claim_tasks(self) -> list[ClaimedTask]:
        return await self._inner.claim_tasks()
