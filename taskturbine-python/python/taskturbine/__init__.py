"""
Taskturbine python SDK

This module contains the python components of the taskturbine
durable function framework. While all the IO operations are built
with rust, the parts of tasks that interact directly with your code
are in python.
"""
from functools import update_wrapper
from typing import Callable, Generic, MutableMapping, ParamSpec, TypeVar

# Import from the rust library
from .taskturbine import Config
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
        return self._task_rs.task_name

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Call the task function immediately.
        """
        return self._func(*args, **kwargs)


class TaskturbineApp:
    def __init__(self, config: Config) -> None:
        self._app_rs = AppRs(config)
        self._tasks: MutableMapping[str, Task] = {}

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


