from datetime import timedelta
from typing import Self

class AwaitResult:
    payload: bytes
    should_suspend: bool

class SpawnResult:
    run_id: str
    task_id: str

class Checkpoint:
    task_id: str
    step_name: str
    state: bytes
    owner_run_id: str
    updated_at: int

class Task:
    module_name: str
    task_name: str

class ClaimedTask:
    task_id: str
    run_id: str
    channel: str
    task_name: str
    params: bytes
    retry_seconds: int
    retry_factor: float
    retry_max_seconds: int
    attempt: int
    max_attempts: int

    def next_retry_in(self) -> timedelta: ...

class Config:
    app_module: str
    """
    The path to the `package.module:app_var` of the python application to work with. The worker
    runtime will import this symbol and use it to lookup and execute tasks
    """

    database_url: str
    # TODO move rest of documentation here instead of in py03 objects
    database_log_queries: bool
    usecase: str
    default_channel: str
    worker_claim_timeout_secs: int
    worker_cleanup_cutoff_secs: int
    worker_cleanup_inline: bool
    worker_cleanup_limit: int
    worker_concurrency: int
    worker_sleep_secs: int
    await_event_default_timeout_secs: int

    def __init__(
        self,
        app_module: str,
        database_url: str,
        *,
        database_log_queries: bool = False,
        usecase: str = "default",
        default_channel: str = "default",
        worker_claim_timeout_secs: int = 600,
        worker_cleanup_cutoff_secs: int = 30,
        worker_cleanup_inline: bool = True,
        worker_cleanup_limit: int = 1000,
        worker_concurrency: int = 3,
        worker_sleep_secs: int = 2,
        await_event_default_timeout_secs: int = 120,
    ) -> None: ...

class TaskOptions:
    headers: dict[str, str]
    max_attempts: int
    retry_seconds: int
    retry_factor: float
    retry_max_seconds: int
    cancellation_max_age: int

    def __init__(
        self,
        max_attempts: int,
        retry_seconds: int,
        retry_factor: float,
        retry_max_seconds: int,
        cancellation_max_age: int,
    ) -> None: ...
    def copy_with(
        self,
        headers: dict[str, str] | None,
        max_attempts: int | None,
        retry_seconds: int | None,
        retry_factor: float | None,
        retry_max_seconds: int | None,
        cancellation_max_age: int | None,
    ) -> Self: ...

class WorkerInner:
    worker_sleep_secs: int
    worker_cleanup_interval_secs: int

    def claim_tasks(self) -> list[ClaimedTask]: ...
    def run_cleanup(self) -> None: ...
    def fail_run(self, run_id: str, retry_at: timedelta) -> None: ...
    def complete_run(self, run_id: str, run_result: bytes) -> None: ...
    def schedule_run(self, run_id: str, wait_for: timedelta) -> None: ...

class ContextInner:
    claimed_task: ClaimedTask
    def await_event_default_timeout_secs(self) -> int: ...
    def emit_event(self, event_name: str, payload: bytes) -> None: ...
    def get_checkpoint(self, checkpoint_name: str) -> Checkpoint: ...
    def set_checkpoint(
        self, checkpoint_name: str, state: bytes, extend_claim: timedelta | None
    ) -> None: ...
    def get_event_payload(self, event_name: str, timeout: timedelta) -> AwaitResult: ...

class TaskturbineApp:
    config: Config
    channels: set[str]

    def __init__(self, config: Config) -> None: ...
    def add_channel(self, value: str) -> None: ...
    def register_task(self, task: Task) -> None: ...
    def has_task(self, name: str) -> bool: ...
    def spawn_task(
        self, task_name: str, params: bytes, options: TaskOptions
    ) -> SpawnResult: ...
    def channel_spawn_task(
        self, channel: str, task_name: str, params: bytes, options: TaskOptions
    ) -> SpawnResult: ...
    def emit_event(self, event_name: str, payload: bytes) -> None: ...
    def create_worker(self, worker_id: str, channels: list[str]) -> WorkerInner: ...
    def create_context(self, claimed_task: ClaimedTask) -> ContextInner: ...
