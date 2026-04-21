import json
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class TaskSerializer(Protocol):
    """Interface for task serialization"""

    def serialize(self, value: Any) -> bytes: ...

    """Convert parameter and result values into bytes"""

    def deserialize(self, value: bytes) -> Any: ...

    """Convert bytes into structures for parameters and results"""


class JsonSerializer(TaskSerializer):
    """JSON encoding TaskSerializer"""

    def serialize(self, value: Any) -> bytes:
        return json.dumps(value).encode()

    def deserialize(self, value: bytes) -> Any:
        return json.loads(value)


