from functools import update_wrapper
from datetime import timedelta
from typing import (
    Callable,
    Generic,
    ParamSpec,
    TypeVar,
)
from .taskturbine import TaskOptions

P = ParamSpec("P")
R = TypeVar("R")


class SuspendError(Exception):
    """Signal the worker runtime to suspend this task for its retry timeout, or sleep time"""

    def __init__(self, duration: timedelta | None = None) -> None:
        super().__init__("Task suspended")
        self.duration = duration


class Task(Generic[P, R]):
    def __init__(
        self, name: str, func: Callable[P, R], options: TaskOptions | None = None
    ):
        self.name = name
        self._func = func
        self.options = options
        update_wrapper(self, func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Call the task function immediately.
        """
        return self._func(*args, **kwargs)
