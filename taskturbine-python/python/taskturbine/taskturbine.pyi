from datetime import timedelta
from typing import Any

class AwaitResult:
    """The metadata for the result of await_event"""

    payload: bytes
    """
    The event payload that was awaited upon.
    Application logic is responsible for decoding bytes.
    """

    should_suspend: bool
    """Whether or not the runtime should suspend as we're still waiting for the event."""

class SpawnResult:
    """The result of spawning a task."""

    run_id: str
    """The run_id of the spawned task"""

    task_id: str
    """The task_id of the spawned task"""

class Checkpoint:
    """
    A saved checkpoint created by either a step completing, sleep_for expiring
    or an event await being fulfilled. Checkpoints for a task are shared by
    all runs as each run can add checkpoints to a task's state.
    """

    task_id: str
    """The task_id of the checkpoint"""

    step_name: str
    """
    The step name of the checkpoint. Step names are made
    unique per task to handle duplicate step names.
    """

    state: bytes
    """
    The payload/state of the checkpoint in bytes.
    By default checkpoint state is JSON encoded.
    """

    owner_run_id: str
    """The run that created this checkpoint."""

    updated_at: int
    """The timestamp the checkpoint was created or updated."""

class ClaimedTask:
    """
    Entity structure for a task that has been claimed
    by a worker for execution. This is a snapshot of the state
    from when the claim was made.
    """

    task_id: str
    """The task id of the spawned task."""

    run_id: str
    """The run id of the spawned run."""

    channel: str
    """The channel name the task was spawned in."""

    task_name: str
    """The name of the task that was claimed."""

    params: bytes
    """The parameters of the task in bytes."""

    retry_seconds: int
    """The number of seconds betwen retries."""

    retry_factor: float
    """The factor to multiple retries by attempt count."""

    retry_max_seconds: int
    """The maximum number of seconds to wait between retries."""

    attempt: int
    """The current attempt count."""

    max_attempts: int
    """The maximum number of attempts allowed."""

    def next_retry_in(self) -> timedelta: ...
    """Get the timedelta between now and the next retry time."""

    def to_dict(self) -> dict[str, Any]: ...
    """
    Convert the ClaimedTask to a dict.
    This is required when sending a ClaimedTask to a child process.
    """

    @staticmethod
    def from_dict(value: dict[str, Any]) -> ClaimedTask: ...
    """Build a ClaimedTask from a dict"""

class Config:
    """
    Configuration for Taskturbine
    This object contains all of the configuration settings for task creation,
    workers and cleanup operations.
    """

    app_module: str
    """
    The path to the `package.module:app_var` of the python application to work with. The worker
    runtime will import this symbol and use it to lookup and execute tasks
    """

    database_url: str
    """
    The URI of the database your are connecting to.
    Example: postgresql://app:password@localhost/taskturbine
    """

    database_log_queries: bool
    """Enable database logging at DEBUG level"""

    usecase: str
    """
    The application or client that is connecting.
    Workers are bound to a specific usecase and can conditionally
    consume from one or more channel (aka. queue/topic)
    """

    default_channel: str
    """
    The default channel that tasks are spawned into.
    This channel will automatically be registered into the application
    using a config instance.
    """

    worker_claim_timeout_secs: int
    """
    The number of seconds that workers will claim tasks for.
    Workers are expected to complete tasks within their claim timeout.
    After a claim timeout is exceeded, the task will be made pending again.
    Default value is 600 (10m)
    """

    worker_cleanup_cutoff_secs: int
    """
    The age of completed tasks and events in seconds
    after now() that are safe to delete.
    """

    worker_cleanup_inline: bool
    """
    Whether or not workers should run cleanup operations inline.
    Set to false if you are going to run cleanup workers separately.
    """

    worker_cleanup_limit: int
    """
    The maximum number of completed tasks and events
    a worker will delete in a single cleanup operation.
    """

    worker_cleanup_interval_secs: int
    """The minimum number of seconds between each cleanup operation."""

    worker_concurrency: int
    """
    The number of task execution slots to start.
    More slots will enable more tasks to run concurrently.
    """

    worker_sleep_secs: int
    """The number of seconds a worker should sleep when no tasks are available."""

    await_event_default_timeout_secs: int
    """The default number of seconds that events are waited on for."""

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
    """
    The runtime options used to spawn a task
    """

    headers: dict[str, str]
    """A dictionary of headers for the task"""

    max_attempts: int
    """The maximum number of attempts that a task will have before it is cancelled"""

    retry_seconds: int
    """The number of seconds between retries."""

    retry_factor: float
    """
    The multiplier applied to `retry_seconds` each time a retry is made.
    Setting this to a value greater than 1.0 will provide exponential backoffs.
    """

    retry_max_seconds: int
    """The max number of seconds between retries."""

    cancellation_max_age: int
    """
    The number of seconds after creation that a
    task is considered stale and should be cancelled.
    """

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
    """Create a clone of TaskOptions with updated values"""


class WorkerInner:
    """
    The python -> rust binding boundary for a Worker.
    """

    app_module: str
    """Path to the module and variable that contain the application being run."""

    worker_concurrency: int
    """Number of child processes to spawn as task executors."""

    worker_sleep_secs: int
    """Number of seconds workers should sleep between run loops."""

    worker_cleanup_interval_secs: int
    """Number of seconds between cleanup operations."""

    def claim_tasks(self) -> list[ClaimedTask]: ...
    """Claim a list of tasks based on configuration"""

    def should_run_cleanup(self, timestamp: int) -> bool: ...
    """Should the current worker run a cleanup loop"""

    def run_cleanup(self) -> None: ...
    """
    Run a cleanup operation that purges old:
    - events
    - tasks & runs
    """

    def fail_run(self, run_id: str, retry_at: timedelta) -> None: ...
    """Mark a run as having failed"""

    def complete_run(self, run_id: str, run_result: bytes) -> None: ...
    """Mark a run as complete. The related task will also be marked complete."""

    def schedule_run(self, run_id: str, wait_for: timedelta) -> None: ...
    """Schedule a run in the future."""

class ContextInner:
    claimed_task: ClaimedTask
    """The task that was claimed for this context"""

    await_event_default_timeout_secs: int
    """The number of seconds await_event should use as a timeout by default"""

    def emit_event(self, event_name: str, payload: bytes) -> None: ...
    """Record an event taking place."""

    def get_checkpoint(self, checkpoint_name: str) -> Checkpoint: ...
    """
    Get a checkpoint by name for a task.
    `checkpoint_name` is expected to be a unique name.
    """

    def set_checkpoint(
        self, checkpoint_name: str, state: bytes, extend_claim: timedelta | None
    ) -> None: ...
    """
    Set the state for a named checkpoint.
    The caller is responsible for making checkpoint_names unique.
    """

    def get_event_payload(self, event_name: str, timeout: timedelta) -> AwaitResult: ...
    """
    Read the payload for an event. Will raise an exception if the read fails
    """

class AppInner:
    """
    The rust/python interface. This class is wrapped by
    """

    config: Config
    channels: set[str]

    def __init__(self, config: Config) -> None: ...
    def add_channel(self, value: str) -> None: ...
    """Add a channel to the list of channels this application can publish and consume from."""

    def register_task(self, task: Task) -> None: ...
    def has_task(self, name: str) -> bool: ...
    def spawn_task(
        self, task_name: str, params: bytes, options: TaskOptions
    ) -> SpawnResult: ...
    """
    Spawn a task on the default channel and initialize the first run.
    An error is returned if the task name is not registered.
    """
    def channel_spawn_task(
        self, channel: str, task_name: str, params: bytes, options: TaskOptions
    ) -> SpawnResult: ...
    """Spawn a task on a named channel"""

    def emit_event(self, event_name: str, payload: bytes) -> None: ...
    """
    Record an event as having completed.
    Events allow you to synchronize tasks with external actions
    that can be recorded as events. Events can have a Payload of bytes.
    """

    def create_worker(self, worker_id: str, channels: list[str]) -> WorkerInner: ...
    """
    Create a worker for the application tasks
    A worker will only claim tasks in `channels` if channels is not-empty.
    If `channels` is empty, tasks in all channels will be processed.
    """

    def create_context(self, claimed_task: ClaimedTask) -> ContextInner: ...
    """Create a ContextInner which bridges into the python client."""
