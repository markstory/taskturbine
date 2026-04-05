-- Copyright 2025 earendil-works
-- Copyright 2026 Mark Story
--
-- Derived from https://github.com/earendil-works/absurd/blob/main/sql/absurd.sql
-- Schema modified from absurd and adapted for taskturbine.
CREATE TABLE taskturbine.tasks (
    task_id uuid primary key,
    usecase text not null,
    channel text not null,
    task_name text not null,
    params bytea not null,
    headers bytea,
    idempotency_key varchar,
    -- Each retry increments enqueue_at = enqueue_at + (retry_seconds * retry_factor * attempts)
    retry_seconds integer,
    retry_factor real,
    retry_max_seconds integer,
    -- Incremented on each claim
    attempts integer not null default 0,
    max_attempts integer,
    -- Cancel a task if (now() - first_started_at >= max_age) to prevent tasks retrying infinitely
    cancellation_max_age int,
    created_at timestamptz not null default current_timestamp,
    -- When the task was moved to running for the first time
    first_started_at timestamptz,
    state text not null check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    last_attempt_run uuid,
    -- When the task was completed/failed/cancelled.
    completed_at timestamptz,
    CONSTRAINT task_name_idempotent UNIQUE (task_name, idempotency_key)
);
CREATE INDEX tasks_usecase_channel ON taskturbine.tasks (usecase, channel);

CREATE TABLE taskturbine.runs (
    run_id uuid primary key,
    task_id uuid not null,
    attempt integer not null,
    state text not null check (state in ('pending', 'running', 'sleeping', 'completed', 'failed', 'cancelled')),
    claimed_by text,
    claim_expires_at timestamptz,
    available_at timestamptz not null,
    -- When the run was moved to running the first time.
    started_at timestamptz,
    -- Timestamp of when the run was completed/failed/cancelled.
    completed_at timestamptz,
    result bytea,
    failure_reason bytea,
    created_at timestamptz not null default current_timestamp
) with (fillfactor=70);
CREATE INDEX runs_sai ON taskturbine.runs (state, available_at);
CREATE INDEX runs_taskid ON taskturbine.runs (task_id);
 
CREATE TABLE taskturbine.checkpoints (
    task_id uuid not null,
    step_name text not null,
    state bytea,
    status text not null default 'committed',
    owner_run_id uuid,
    updated_at timestamptz not null default current_timestamp,
    primary key (task_id, step_name)
);

CREATE TABLE taskturbine.events (
    usecase text not null,
    event_name text not null,
    payload bytea,
    created_at timestamptz not null default current_timestamp,
    primary key (usecase, event_name),
    CONSTRAINT unique_event_name UNIQUE (usecase, event_name)
);

CREATE TABLE taskturbine.waits (
    task_id uuid not null,
    run_id uuid not null,
    step_name text not null,
    event_name text not null UNIQUE,
    timeout_at timestamptz,
    created_at timestamptz not null default current_timestamp,
    primary key (run_id, step_name)
);
