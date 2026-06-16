from typing import Any, Protocol


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


class NoopMetrics(MetricsBackend):
    def incr(self, key: str, value: float, tags: dict[str, Any] | None) -> None:
        pass

    def gauge(self, key: str, value: float, tags: dict[str, Any] | None) -> None:
        pass

    def histogram(self, key: str, value: float, tags: dict[str, Any] | None) -> None:
        pass
