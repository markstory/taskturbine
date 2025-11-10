CREATE SCHEMA if not exists taskturbine;

CREATE TABLE taskturbine.tasks (
    task_id uuid primary key,
    namespace text not null,
    task_name text not null,
    params bytea not null,
    headers bytea,
    -- Each retry increments enqueue_at = enqueue_at + (retry_seconds * retry_factor * attempts)
    retry_seconds integer,
    retry_factor integer,
    retry_max_seconds integer,
    -- Incremented on each claim
    attempts integer not null default 0,
    max_attempts integer,
    -- Cancel a task if (now() - first_started_at >= max_age) to prevent tasks retrying infinitely
    cancellation_max_age int,
    -- When to start running the task
    enqueue_at timestamptz not null default current_timestamp,
    -- When the task was moved to running.
    first_started_at timestamptz,
    state text not null check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    last_attempt_run uuid,
    cancelled_at timestamptz
);

CREATE TABLE taskturbine.runs (
    run_id uuid primary key,
    task_id uuid not null,
    attempt integer not null,
    state text not null check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    claimed_by text,
    claim_expires_at timestamptz,
    available_at timestamptz not null,
    wake_event text,
    event_payload bytea,
    started_at timestamptz,
    completed_at timestamptz,
    failed_at timestamptz,
    result bytea,
    failure_reason bytea,
    created_at timestamptz not null default current_timestamp
) with (fillfactor=70);
CREATE INDEX runs_sai ON taskturbine.runs (state, available_at);
CREATE INDEX runs_taskid ON taskturbine.runs (task_id);
 
CREATE TABLE taskturbine.checkpoints (
    task_id uuid not null,
    checkpoint_name text not null,
    state bytea,
    status text not null default 'committed',
    owner_run_id uuid,
    updated_at timestamptz not null default current_timestamp,
    primary key (task_id, checkpoint_name)
);

CREATE TABLE taskturbine.events (
    event_name text primary key,
    payload bytea,
    emitted_at timestamptz not null default current_timestamp
);

CREATE TABLE taskturbine.waits (
    task_id uuid not null,
    run_id uuid not null,
    step_name text not null,
    event_name text not null,
    timeout_at timestamptz,
    created_at timestamptz not null default current_timestamp,
    primary key (run_id, step_name)
);
CREATE INDEX waits_event ON taskturbine.waits (event_name);
