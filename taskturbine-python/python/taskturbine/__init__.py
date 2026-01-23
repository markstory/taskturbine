"""
Taskturbine python SDK

This module contains the python components of the taskturbine
durable function framework. While all the IO operations are built
with rust, the parts of tasks that interact directly with your code
are in python.
"""
from functools import update_wrapper
from typing import Any, Callable, Generic, MutableMapping, ParamSpec, TypeVar
import json

# Import from the rust library
from .taskturbine import Config, TaskOptions, SpawnResult
from .taskturbine import Task as TaskRs
from .taskturbine import TaskturbineApp as AppRs

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


class TaskturbineApp:
    def __init__(self, config: Config) -> None:
        self._app_rs = AppRs(config)
        self._tasks: MutableMapping[str, Task] = {}

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

    def serialize_params(self, params: dict[str, Any]) -> bytes:
        """Convert parameters into bytes

        TODO make this a hook method so other serializers can be used.
        """
        return json.dumps(params).encode()

    def spawn_task(self, taskname: str, params: dict[str, Any], ) -> SpawnResult:
        options = TaskOptions()
        try:
            self._app_rs.spawn_task(taskname, self.serialize_params(params), options)
        except ValueError:
            raise

        """
        TODO continue from here.

        options to add as kwargs
        these options should be marshalled into TaskOptions
        and sent to app_rs method that converts to rust

        app_rs should have a reference to the storage object

    /// Map of headers to include with the task activation
    pub headers: HashMap<String, String>,

    /// The maximum number of attempts to make on this task
    pub max_attempts: i32,

    /// The minimum number of seconds to wait between retries.
    pub retry_seconds: i32,

    /// The multipier to apply to retry delays between attempts.
    /// Use > 1.0 to create exponential backoff.
    pub retry_factor: f64,

    /// The maximum number of seconds to wait between retries.
    pub retry_max_seconds: i32,

    /// The maximum age of a task before it should not be run.
    /// Measured in seconds from when the task was created.
    pub cancellation_max_age: i32,
        """
