from datetime import timedelta
import functools
import json
from typing import Any, Callable, ParamSpec
from taskturbine.models import JsonData, OptionalJsonData, SuspendError
from taskturbine.taskturbine import (
    ContextInner,
)

P = ParamSpec("P")


class TaskContext:
    def __init__(
        self,
        inner: ContextInner,
        serialize: Callable[[JsonData], bytes],
        deserialize: Callable[[bytes], JsonData | None],
    ) -> None:
        self._inner = inner
        self._serialize = serialize
        self._deserialize = deserialize
        self._checkpoint_counters: dict[str, int] = {}
        self._claimed_task = inner.claimed_task

    @property
    def task_id(self) -> str:
        return self._claimed_task.task_id

    @property
    def run_id(self) -> str:
        return self._claimed_task.run_id

    @property
    def params(self) -> Any:
        """Get the parameters a JSON parsed value"""
        return json.loads(self._claimed_task.params)

    @property
    def params_bytes(self) -> bytes:
        """Get the parameters a byte string"""
        return self._claimed_task.params

    def await_event(
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

        wait = self._inner.get_event_payload(event_name, timeout)
        if wait.should_suspend:
            raise SuspendError()
        return json.loads(wait.payload)

    def emit_event(self, event_name: str, payload: JsonData) -> None:
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

    def sleep_for(self, step_name: str, duration: timedelta) -> None:
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

    def step(
        self, name: str
    ) -> Callable[[Callable[P, OptionalJsonData]], Callable[P, OptionalJsonData]]:
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
            func: Callable[P, OptionalJsonData],
        ) -> Callable[P, OptionalJsonData]:
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> OptionalJsonData:
                return self._execute_step(checkpoint_name, func, *args, **kwargs)

            functools.update_wrapper(wrapper, func)
            return wrapper

        return decorator

    def step_run(
        self,
        step_name: str,
        func: Callable[P, OptionalJsonData],
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
        return self._execute_step(checkpoint_name, func, *args, **kwargs)

    def _execute_step(
        self,
        checkpoint_name: str,
        func: Callable[P, OptionalJsonData],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> JsonData | None:
        """
        Execute a step function.
        """
        try:
            checkpoint = self._inner.get_checkpoint(checkpoint_name)
            return self._deserialize(checkpoint.state)
        except ValueError:
            # No checkpoint data.
            pass

        # Step functions may raise, but worker.execute_task will catch
        step_result = func(*args, **kwargs)

        result_bytes = b""
        if step_result:
            result_bytes = self._serialize(step_result)
        self._inner.set_checkpoint(checkpoint_name, result_bytes, None)

        return step_result
