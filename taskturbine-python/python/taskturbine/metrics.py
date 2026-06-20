import contextlib
from typing import Any, Protocol
import time

from taskturbine.taskturbine import ClaimedTask


def task_metrics_tags(usecase: str, claimed: ClaimedTask | None) -> dict[str, str]:
    tags = {
        "usecase": usecase,
    }
    if claimed:
        tags["channel"] = claimed.channel
        tags["taskname"] = claimed.task_name
    return tags


class MetricsBackend(Protocol):
    """
    Interface definition for metrics backends.

    You can integrate with your existing metrics system of choice.
    """

    def incr(self, key: str, value: float, tags: dict[str, Any] | None) -> None: ...

    def gauge(self, key: str, value: float, tags: dict[str, Any] | None) -> None: ...

    def histogram(
        self, key: str, value: float, tags: dict[str, Any] | None
    ) -> None: ...

    @contextlib.contextmanager
    def timer(self, key: str, tags: dict[str, Any]):
        start = time.monotonic()
        try:
            yield None
        finally:
            self.histogram(key, time.monotonic() - start, tags)


class NoopMetrics(MetricsBackend):
    def incr(self, key: str, value: float, tags: dict[str, Any] | None) -> None:
        pass

    def gauge(self, key: str, value: float, tags: dict[str, Any] | None) -> None:
        pass

    def histogram(self, key: str, value: float, tags: dict[str, Any] | None) -> None:
        pass
