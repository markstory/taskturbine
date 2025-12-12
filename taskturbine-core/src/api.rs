use std::collections::HashMap;

use crate::config::Config;
use crate::models::{Checkpoint, ClaimedTask, RunId, Task, TaskId, TaskState};
use chrono::{DateTime, Utc};
use sqlx::{
    ConnectOptions, PgConnection, PgPool, Postgres, QueryBuilder, Row, Transaction,
    migrate::MigrateError,
    postgres::{PgConnectOptions, PgRow},
    query::Query,
};
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug)]
pub enum TaskTurbineError {
    EncodeError(serde_json::Error),
    SqlError(sqlx::Error),
    NotFound(Uuid),
    NotRunning(Uuid),
    ValidationError(&'static str),
}

/// Result of spawning a task.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpawnResult {
    pub task_id: TaskId,
    pub run_id: RunId,
}

/// Options for spawning a task.
/// Default values are drawn from the TaskRuntime and TaskOptions defaults.
#[non_exhaustive]
pub struct TaskOptions {
    /// Map of headers to include with the task activation
    pub headers: HashMap<String, String>,

    /// The maximum number of attempts to make on this task
    pub max_attempts: i32,

    /// The minimum number of seconds to wait between retries.
    pub retry_seconds: i32,

    /// The multipier to apply to retry delays between attempts.
    /// Use > 1.0 to create exponential backoff.
    pub retry_factor: f64,

    /// The maximum number of seconds to wait between retries.
    pub retry_max_seconds: i32,

    /// The maximum age of a task before it should not be run.
    /// Measured in seconds from when the task was created.
    pub cancellation_max_age: i32,
}

impl Default for TaskOptions {
    fn default() -> Self {
        TaskOptions {
            headers: HashMap::new(),
            max_attempts: 5,
            retry_seconds: 10,
            retry_factor: 2.0,
            retry_max_seconds: 300,
            cancellation_max_age: 86400,
        }
    }
}

/// A structure for interacting with the storage layer of TaskTurbine
pub struct Storage {
    config: Config,
    pool: PgPool,
}

impl Storage {
    /// Create a new runtime from the given configuration.
    pub fn new(config: Config) -> Self {
        let pool = PgPool::connect_lazy(&config.database_url)
            .expect("Failed to create database connection pool");
        let options: Result<PgConnectOptions, _> = config.database_url.parse();
        if let Ok(mut opts) = options {
            opts = opts.log_statements(log::LevelFilter::Debug);
            pool.set_connect_options(opts);
        }
        Self { config, pool }
    }

    /// Get a copy of the current [`Config`]
    pub fn get_config(&self) -> Config {
        self.config.clone()
    }

    /// Garbage collect events.
    ///
    /// Delete events that have created_at older than `older_than`.
    /// Only `limit` or fewer records will be deleted.
    /// Returns the number of events that were deleted.
    pub async fn cleanup_events(
        &self,
        older_than: DateTime<Utc>,
        limit: i32,
    ) -> Result<u64, TaskTurbineError> {
        let res = sqlx::query(
            "DELETE FROM taskturbine.events WHERE event_name IN (
                SELECT event_name FROM taskturbine.events
                WHERE created_at < $1 LIMIT $2
            )",
        )
        .bind(older_than)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        Ok(res.rows_affected())
    }

    /// Garbage collect tasks and related data.
    ///
    /// Delete tasks
    pub async fn cleanup_tasks(
        &self,
        older_than: DateTime<Utc>,
        limit: i32,
    ) -> Result<u64, TaskTurbineError> {
        let mut builder = QueryBuilder::new(
            "WITH finished_tasks AS (
                SELECT task_id FROM taskturbine.tasks
                WHERE state IN (",
        );
        let mut separated = builder.separated(", ");
        separated.push_bind(TaskState::Completed);
        separated.push_bind(TaskState::Failed);
        separated.push_bind(TaskState::Cancelled);
        separated.push_unseparated(")");

        let res = builder
            .push("AND completed_at <")
            .push_bind(older_than)
            .push(format!(" LIMIT {limit}"))
            .push(
                "),
                del_waits AS (
                    DELETE FROM taskturbine.waits 
                    WHERE task_id IN (SELECT task_id FROM finished_tasks)
                ),
                del_runs AS (
                    DELETE FROM taskturbine.runs
                    WHERE task_id IN (SELECT task_id FROM finished_tasks)
                ),
                del_checkpoints AS (
                    DELETE FROM taskturbine.checkpoints
                    WHERE task_id IN (SELECT task_id FROM finished_tasks)
                )
                DELETE FROM taskturbine.tasks 
                WHERE task_id IN (SELECT task_id FROM finished_tasks)
            ",
            )
            .build()
            .execute(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res.rows_affected())
    }

    /// Delete all data from the storage tables.
    /// Primarily intended for testing and local development purposes.
    pub async fn clear_storage(&self) -> Result<(), TaskTurbineError> {
        let tables = ["events", "waits", "checkpoints", "runs", "tasks"];
        for table in tables.iter() {
            let query = format!("TRUNCATE taskturbine.{table} CASCADE");
            sqlx::query(&query)
                .execute(&self.pool)
                .await
                .map_err(TaskTurbineError::SqlError)?;
        }
        Ok(())
    }

    /// {{{ Testing helpers
    /// Testing Helper: setting run + task to a specific state.
    #[cfg(test)]
    async fn set_run_state(&self, task_id: TaskId, state: TaskState) -> Result<(), TaskTurbineError> {
        let res = sqlx::query(
            "UPDATE taskturbine.runs
            SET state = $1
            WHERE task_id = $2",
        )
        .bind(state)
        .bind(task_id.0)
        .execute(&self.pool)
        .await;
        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }

        let res = sqlx::query(
            "UPDATE taskturbine.tasks
            SET state = $1
            WHERE task_id = $2",
        )
        .bind(state)
        .bind(task_id.0)
        .execute(&self.pool)
        .await;

        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }
        Ok(())
    }

    /// Testing helper: reading task runs
    #[cfg(test)]
    async fn get_run(&self, run_id: RunId) -> Result<PgRow, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    // Testing helper: get waits for a run
    #[cfg(test)]
    async fn get_wait_by_run_id(&self, run_id: RunId) -> Result<Option<PgRow>, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.waits WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    // Testing helper: get a run
    #[cfg(test)]
    async fn get_task(&self, task_id: TaskId) -> Result<Option<PgRow>, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.tasks WHERE task_id = $1")
            .bind(task_id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    // Testing helper: get an event
    #[cfg(test)]
    async fn get_event_row(&self, event_name: &str) -> Result<Option<PgRow>, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.events WHERE event_name = $1")
            .bind(event_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }
    /// }}}

    // Run migrations to create or update the database schema.
    // Will create a taskturbine schema and add all tables inside that schema.
    pub async fn update_schema(&self) -> Result<(), MigrateError> {
        sqlx::migrate!("./migrations").run(&self.pool).await
    }

    /// Spawn a task and initialize a run.
    ///
    /// Tasks belong to a namespace. Namespaces allow you to split up your task
    /// workload into different worker pools. This is ideal for spliting up orthoganal
    /// workloads, or to handling various priorities and throughput on the same
    /// taskturbine database.
    pub async fn spawn_task(
        &self,
        namespace: &str,
        task_name: &str,
        params: &[u8],
        options: Option<TaskOptions>,
    ) -> Result<SpawnResult, TaskTurbineError> {
        let options = options.or_else(|| Some(TaskOptions::default())).unwrap();
        let header_json =
            serde_json::to_vec(&options.headers).map_err(TaskTurbineError::EncodeError)?;

        if options.retry_factor < 1.0 {
            return Err(TaskTurbineError::ValidationError(
                "retry_factor must be >= 1.0",
            ));
        }

        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;
        let task_id = Uuid::now_v7();
        let res = sqlx::query(
            "INSERT INTO taskturbine.tasks (
                task_id, usecase, namespace, task_name, params, headers,
                retry_seconds, retry_factor, retry_max_seconds,
                max_attempts, cancellation_max_age, created_at, state
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), $12)",
        )
        .bind(task_id)
        .bind(&self.config.usecase)
        .bind(namespace)
        .bind(task_name)
        .bind(params)
        .bind(header_json)
        .bind(options.retry_seconds)
        .bind(options.retry_factor)
        .bind(options.retry_max_seconds)
        .bind(options.max_attempts)
        .bind(options.cancellation_max_age)
        .bind(TaskState::Pending)
        .execute(&mut *atomic);

        if let Err(e) = res.await {
            return Err(TaskTurbineError::SqlError(e));
        }

        let run_id = Uuid::now_v7();
        let res = sqlx::query(
            "INSERT INTO taskturbine.runs (
                run_id, task_id, attempt, state, available_at
            ) VALUES ($1, $2, 0, $3, NOW())",
        )
        .bind(run_id)
        .bind(task_id)
        .bind(TaskState::Pending)
        .execute(&mut *atomic);

        if let Err(e) = res.await {
            return Err(TaskTurbineError::SqlError(e));
        }
        atomic.commit().await.map_err(TaskTurbineError::SqlError)?;

        Ok(SpawnResult { task_id: TaskId(task_id), run_id: RunId(run_id) })
    }

    /// Claim one or more tasks for processing.
    /// Workers use this method to acquire work to do.
    ///
    /// The `claim_timeout` indicates how long the worker intends to hold the task for.
    /// After this period if the task run is not complete it will be eligible to be
    /// claimed by another worker. That worker will continue processing from the last
    /// checkpoint if any exist.
    pub async fn claim_task(
        &self,
        worker_id: &str,
        claim_timeout: DateTime<Utc>,
        qty: i32,
    ) -> Result<Vec<ClaimedTask>, TaskTurbineError> {
        if qty <= 0 {
            return Err(TaskTurbineError::ValidationError(
                "qty must be greater than zero",
            ));
        }
        let now = Utc::now();
        if claim_timeout < now {
            return Err(TaskTurbineError::ValidationError(
                "claim_timeout must be in the future",
            ));
        }

        // Fetch and update N runs that are pending or sleeping.
        let claimed: Vec<ClaimedTask> = sqlx::query_as(
            "WITH candidates AS (
                SELECT r.task_id, r.run_id
                FROM taskturbine.runs AS r
                INNER JOIN taskturbine.tasks t ON t.task_id = r.task_id
                WHERE r.state IN ('pending', 'sleeping')
                AND t.state IN ('pending', 'sleeping')
                AND r.available_at <= NOW()
                AND t.usecase = $1
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ),
            claim_run AS (
                UPDATE taskturbine.runs
                SET state = 'running',
                    claimed_by = $3,
                    claim_expires_at = $4,
                    started_at = NOW(),
                    attempt = attempt + 1
                WHERE run_id IN (SELECT run_id FROM candidates)
                RETURNING run_id, task_id, attempt
            ),
            claim_task AS (
                UPDATE taskturbine.tasks AS t
                SET state = 'running',
                    first_started_at = COALESCE(t.first_started_at, NOW()),
                    attempts = GREATEST(t.attempts, cr.attempt)
                FROM claim_run AS cr
                WHERE t.task_id = cr.task_id
            )
            SELECT t.task_id, cr.run_id,
            t.task_name, t.params,
            t.retry_seconds, t.retry_factor, t.retry_max_seconds,
            cr.attempt, t.max_attempts
            FROM claim_run AS cr
            INNER JOIN taskturbine.tasks AS t ON cr.task_id = t.task_id
            INNER JOIN taskturbine.runs AS r ON cr.run_id = r.run_id
            ORDER BY r.available_at, r.run_id",
        )
        .bind(&self.config.usecase)
        .bind(qty)
        .bind(worker_id)
        .bind(claim_timeout)
        .fetch_all(&self.pool)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        Ok(claimed)
    }

    /// Extend the claim on a running task.
    /// Can be used by workers to 'heartbeat' and avoid missing their deadlines.
    pub async fn extend_claim(
        &self,
        worker_id: &str,
        run_id: RunId,
        claim_timeout: DateTime<Utc>,
    ) -> Result<(), TaskTurbineError> {
        let now = Utc::now();
        if claim_timeout < now {
            return Err(TaskTurbineError::ValidationError(
                "claim_timeout must be in the future",
            ));
        }

        let res = sqlx::query(
            "UPDATE taskturbine.runs
            SET claim_expires_at = $1
            WHERE run_id = $2
            AND claimed_by = $3
            AND state = 'running'",
        )
        .bind(claim_timeout)
        .bind(run_id)
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        if res.rows_affected() == 0 {
            return Err(TaskTurbineError::NotRunning(run_id.0));
        }

        Ok(())
    }

    /// Release claims on tasks where the claim_timeout_at has passed.
    pub async fn handle_expired_claims(&self) -> Result<i64, TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;
        // Find all runs that have expired claims
        let res = sqlx::query(
            "SELECT run_id, task_id, claimed_by, claim_expires_at
            FROM taskturbine.runs
            WHERE claim_expires_at <= NOW()
            AND state IN ('running', 'pending', 'sleeping')",
        )
        .fetch_all(&mut *atomic)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        // fail all the candidates.
        for run in res.iter() {
            let run_id = run.get::<RunId, _>("run_id");
            let failure_reason = b"{\"reason\":\"claim timeout\"}";
            // TODO error handling?
            let _ = self
                .do_fail_run(&mut atomic, run_id, failure_reason, None)
                .await;
        }

        Ok(res.len() as i64)
    }

    pub async fn handle_cancellation_max_age(&self) -> Result<(), TaskTurbineError> {
        // TODO Build this method, and an upkeep style wrapper.
        // Find all rows that are
        // (t.first_started_at IS NULL OR (
        //  $1 - t.first_started_at < t.cancellation_max_age * INTERVAL '1 SECOND')
        // )
        // These rows have been sleeping or executing for cancellation_max_age
        // seconds and should be cancelled.
        Ok(())
    }

    /// Get a run state in FOR UPDATE mode
    async fn get_locked_run_state(
        &self,
        conn: &mut PgConnection,
        run_id: RunId,
    ) -> Result<PgRow, TaskTurbineError> {
        let res =
            sqlx::query("SELECT task_id, state FROM taskturbine.runs WHERE run_id = $1 FOR UPDATE")
                .bind(run_id)
                .fetch_one(&mut *conn)
                .await;

        if let Err(_) = res {
            return Err(TaskTurbineError::NotFound(run_id.0));
        }

        let row = res.unwrap();
        Ok(row)
    }

    /// Get a task record locked with FOR UPDATE
    async fn get_locked_task(
        &self,
        task_id: TaskId,
        conn: &mut PgConnection,
    ) -> Result<Task, TaskTurbineError> {
        let row: Task = sqlx::query_as(
            "SELECT *
             FROM taskturbine.tasks
             WHERE task_id = $1
             FOR UPDATE",
        )
        .bind(task_id.0)
        .fetch_one(&mut *conn)
        .await
        .map_err(|_| TaskTurbineError::NotFound(task_id.0))?;

        Ok(row)
    }

    /// Mark a run as completed with the provided state.
    /// When a run is completed, the task is also considered complete.
    pub async fn complete_run(
        &self,
        run_id: RunId,
        run_result: &[u8],
    ) -> Result<(), TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;
        let run_row = self.get_locked_run_state(&mut atomic, run_id).await?;
        let task_id: Uuid = run_row.get("task_id");
        let state: TaskState = run_row.get("state");

        if state != TaskState::Running {
            // Need to be running to complete.
            atomic.commit().await.map_err(TaskTurbineError::SqlError)?;
            return Err(TaskTurbineError::NotRunning(run_id.0));
        }
        let res = sqlx::query(
            "UPDATE taskturbine.runs as run
            SET state = $1, completed_at = NOW(), result = $2
            WHERE run_id = $3",
        )
        .bind(TaskState::Completed)
        .bind(run_result)
        .bind(run_id)
        .execute(&mut *atomic)
        .await;
        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }

        let res = sqlx::query(
            "UPDATE taskturbine.tasks
            SET state = $1, last_attempt_run = $2, completed_at = NOW()
            WHERE task_id = $3",
        )
        .bind(TaskState::Completed)
        .bind(run_id)
        .bind(task_id)
        .execute(&mut *atomic)
        .await;
        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }

        self.clear_waits(run_id, &mut atomic).await?;

        atomic.commit().await.map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Clear waits on runs that we are no longer interested in
    /// as the run is complete or cancelled.
    async fn clear_waits(
        &self,
        run_id: RunId,
        conn: &mut PgConnection,
    ) -> Result<(), TaskTurbineError> {
        let _ = sqlx::query("DELETE FROM taskturbine.waits WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut *conn)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Mark a run as failed with the provided reason.
    /// If an retry_at is not provided, the next retry time will be calculated
    /// based on the task's retry_ attributes.
    pub async fn fail_run(
        &self,
        run_id: RunId,
        reason: &[u8],
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;
        let res = self
            .do_fail_run(&mut atomic, run_id, reason, retry_at)
            .await;
        atomic.commit().await.map_err(TaskTurbineError::SqlError)?;

        res
    }

    /// Internal method to fail a run. Can be used within an existing transaction.
    async fn do_fail_run(
        &self,
        conn: &mut PgConnection,
        run_id: RunId,
        reason: &[u8],
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), TaskTurbineError> {
        let run_row = self.get_locked_run_state(&mut *conn, run_id).await?;
        let state: TaskState = run_row.get("state");
        match state {
            TaskState::Running | TaskState::Sleeping => {}
            _ => {
                // If the run is not active/sleeping it cannot be failed.
                return Err(TaskTurbineError::NotRunning(run_id.0));
            }
        }
        let mut task = self
            .get_locked_task(TaskId(run_row.get("task_id")), &mut *conn)
            .await?;
        let res = sqlx::query(
            "UPDATE taskturbine.runs
            SET state = $1, completed_at = NOW(), 
                wake_event = NULL, failure_reason = $2
            WHERE run_id = $3",
        )
        .bind(TaskState::Failed)
        .bind(reason)
        .bind(run_id)
        .execute(&mut *conn)
        .await;

        res.map_err(TaskTurbineError::SqlError)?;

        let next_attempt = task.attempts + 1;
        if next_attempt <= task.max_attempts {
            // Determine the next runtime
            let now = Utc::now();
            let mut next_available_at = if let Some(value) = retry_at {
                value
            } else {
                task.next_retry_at()
            };
            if next_available_at < now {
                next_available_at = now;
            }

            let mut cancel = false;
            // Check if the task has expired due to cancellation age.
            if task.cancellation_max_age > 0 {
                let max_age = chrono::Duration::seconds(task.cancellation_max_age as i64);
                if next_available_at.signed_duration_since(task.created_at) >= max_age {
                    cancel = true;
                }
            }
            // Advance attempt state
            task.attempts = next_attempt;
            task.last_attempt_run = Some(run_id);

            if cancel {
                // Move to cancelled state
                task.state = TaskState::Cancelled;
                task.completed_at = Some(now);
            } else {
                // Not cancelled, advance to next state
                task.completed_at = None;
                task.state = if next_available_at > now {
                    TaskState::Sleeping
                } else {
                    TaskState::Pending
                };

                // Schedule the next run attempt.
                // Create a new run for the next attempt
                let _ = sqlx::query(
                    "INSERT INTO taskturbine.runs (
                        run_id, task_id, attempt, state, available_at
                    ) VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(Uuid::now_v7())
                .bind(task.task_id)
                .bind(next_attempt)
                .bind(task.state)
                .bind(next_available_at)
                .execute(&mut *conn)
                .await
                .map_err(TaskTurbineError::SqlError)?;
            }
        }

        let _ = sqlx::query(
            "UPDATE taskturbine.tasks
            SET state = $1, 
                attempts = $2, 
                last_attempt_run = $3, 
                completed_at = COALESCE(completed_at, $4)
            WHERE task_id = $5",
        )
        .bind(task.state)
        .bind(task.attempts)
        .bind(task.last_attempt_run)
        .bind(task.completed_at)
        .bind(task.task_id)
        .execute(&mut *conn)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        self.clear_waits(run_id, &mut *conn).await?;

        Ok(())
    }

    /// Pause a run and reschedule it to run later.
    /// This is the simplest form of performing a retry
    /// on a run. Scheduling a run this way does not increment
    /// the attempt counter, or count as a fail.
    ///
    /// Runs can go to sleep for reasons like waiting for an event.
    pub async fn schedule_run(
        &self,
        run_id: RunId,
        wake_at: DateTime<Utc>,
    ) -> Result<(), TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;

        let run = self
            .get_locked_run_state(&mut atomic, run_id)
            .await
            .map_err(|_| TaskTurbineError::NotFound(run_id.0))?;
        if run.get::<TaskState, _>("state") != TaskState::Running {
            return Err(TaskTurbineError::NotRunning(run_id.0));
        }
        self.suspend_run(
            &mut atomic,
            &run.get::<TaskId, _>("task_id"),
            &run_id,
            wake_at,
        )
        .await?;

        atomic.commit().await.map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Get the state of a single checkpoint
    pub async fn get_checkpoint(
        &self,
        task_id: TaskId,
        step_name: &str,
    ) -> Result<Option<Checkpoint>, TaskTurbineError> {
        let res: Option<Checkpoint> = sqlx::query_as(
            "SELECT * FROM taskturbine.checkpoints
            WHERE task_id = $1 AND step_name = $2",
        )
        .bind(task_id)
        .bind(step_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    /// Get a list of checkpoints saved for this task.
    /// If there are no checkpoints an empty Vec will be returned.
    pub async fn get_checkpoints(
        &self,
        task_id: TaskId,
    ) -> Result<Vec<Checkpoint>, TaskTurbineError> {
        let res: Vec<Checkpoint> = sqlx::query_as(
            "SELECT * FROM taskturbine.checkpoints
            WHERE task_id = $1 ORDER by updated_at",
        )
        .bind(task_id)
        .fetch_all(&self.pool)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    /// Record a checkpoint for a task and step name.
    /// The worker can extend its claim on the task each time it creates a checkpoint.
    pub async fn set_checkpoint(
        &self,
        task_id: TaskId,
        run_id: RunId,
        step_name: &str,
        state: &[u8],
        extend_claim: Option<Duration>,
    ) -> Result<(), TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;
        self.store_checkpoint(&mut atomic, &task_id, &run_id, step_name, state)
            .await?;
        if let Some(extension) = extend_claim {
            let seconds = extension.as_secs() as f64;
            let _ = sqlx::query(
                "UPDATE taskturbine.runs 
                SET claim_expires_at = COALESCE(claim_expires_at, NOW()) + $1 * INTERVAL '1 second'
                WHERE run_id = $2",
            )
            .bind(seconds)
            .bind(run_id)
            .execute(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;
        }
        atomic.commit().await.map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Await for an external event to be received
    /// or for the timeout to expire.
    /// Events must be recorded with [`Storage::emit_event()`]
    pub async fn await_event(
        &self,
        task_id: TaskId,
        run_id: RunId,
        step_name: &str,
        event_name: &str,
        timeout: Option<u64>,
    ) -> Result<AwaitResult, TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;

        // Ensure the task & run exist and are running.
        let run_row = self.get_locked_run_state(&mut atomic, run_id).await?;
        if run_row.get::<TaskState, _>("state") != TaskState::Running {
            return Err(TaskTurbineError::NotRunning(run_id.0));
        }

        // Fetch the checkpoint if it exists
        let checkpoint_opt = sqlx::query(
            "SELECT state FROM taskturbine.checkpoints
            WHERE task_id = $1 AND step_name = $2",
        )
        .bind(task_id.0)
        .bind(step_name)
        .fetch_optional(&mut *atomic)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        // If we have a checkpoint already, return early.
        if let Some(checkpoint) = checkpoint_opt {
            return Ok(AwaitResult {
                payload: checkpoint.get::<Vec<u8>, _>("state"),
                should_suspend: false,
            });
        }

        // Check for an event that was received while we were sleeping/running.
        let event = self.get_event(&mut atomic, event_name).await?;
        if let Some(payload) = event {
            // There was an event, store a checkpoint and return
            self.store_checkpoint(&mut atomic, &task_id, &run_id, step_name, &payload)
                .await?;

            return Ok(AwaitResult {
                payload,
                should_suspend: false,
            });
        }

        // Store a wait and reschedule this run for when the timeout occurs.
        // If an event is emit before that time, we'll be woken up.
        let timeout_ts = if let Some(timeout) = timeout {
            Utc::now() + Duration::from_secs(timeout)
        } else {
            // TODO use config for default timeout
            Utc::now() + Duration::from_secs(60 * 10)
        };
        // Record the event wait
        self.store_wait(
            &mut atomic,
            &task_id,
            &run_id,
            step_name,
            event_name,
            timeout_ts,
        )
        .await?;

        // Suspend the current run and mark the task as sleeping
        self.suspend_run(&mut atomic, &task_id, &run_id, timeout_ts)
            .await?;

        let _ = atomic.commit().await.map_err(TaskTurbineError::SqlError);

        Ok(AwaitResult {
            should_suspend: true,
            payload: b"".to_vec(),
        })
    }

    /// Store a wait for a task
    /// It is assumed that event_name are globally unique, and on a conflict,
    /// wait record is updated to reflect the provided run information.
    async fn store_wait(
        &self,
        conn: &mut PgConnection,
        task_id: &TaskId,
        run_id: &RunId,
        step_name: &str,
        event_name: &str,
        timeout: DateTime<Utc>,
    ) -> Result<(), TaskTurbineError> {
        let _ = sqlx::query(
            "INSERT INTO taskturbine.waits (task_id, run_id, step_name, event_name, timeout_at, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (event_name)
            DO UPDATE
            SET task_id = EXCLUDED.task_id,
                run_id = EXCLUDED.run_id,
                step_name = EXCLUDED.step_name,
                timeout_at = EXCLUDED.timeout_at,
                created_at = EXCLUDED.created_at"
        )
        .bind(task_id.0)
        .bind(run_id)
        .bind(step_name)
        .bind(event_name)
        .bind(timeout)
        .execute(conn)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Record a checkpoint for a task at a given step.
    /// If the checkpoint already exists, it will be updated with the run_id and state.
    async fn store_checkpoint(
        &self,
        conn: &mut PgConnection,
        task_id: &TaskId,
        run_id: &RunId,
        step_name: &str,
        state: &[u8],
    ) -> Result<(), TaskTurbineError> {
        let _ = sqlx::query(
            "INSERT INTO taskturbine.checkpoints (task_id, owner_run_id, step_name, state, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (task_id, step_name)
            DO UPDATE 
            SET owner_run_id = EXCLUDED.owner_run_id,
                state = EXCLUDED.state,
                updated_at = EXCLUDED.updated_at"
        )
        .bind(task_id.0)
        .bind(run_id)
        .bind(step_name)
        .bind(state)
        .execute(conn)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Read an event's payload by name or None
    async fn get_event(
        &self,
        conn: &mut PgConnection,
        event_name: &str,
    ) -> Result<Option<Vec<u8>>, TaskTurbineError> {
        let event_opt = sqlx::query(
            "SELECT payload FROM taskturbine.events
            WHERE usecase = $1 AND event_name = $2",
        )
        .bind(&self.config.usecase)
        .bind(event_name)
        .fetch_optional(conn)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        if let Some(event) = event_opt {
            let payload: Vec<u8> = event.get("payload");

            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    /// Advance a task and run to sleeping state until available_at
    async fn suspend_run(
        &self,
        conn: &mut PgConnection,
        task_id: &TaskId,
        run_id: &RunId,
        available_at: DateTime<Utc>,
    ) -> Result<(), TaskTurbineError> {
        // TODO combine these queries with a CTE
        let _ = sqlx::query(
            "UPDATE taskturbine.runs
            SET state = $1,
                claimed_by = NULL,
                claim_expires_at = NULL,
                available_at = $2
            WHERE run_id = $3",
        )
        .bind(TaskState::Sleeping)
        .bind(available_at)
        .bind(run_id)
        .execute(&mut *conn)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        let _ = sqlx::query("UPDATE taskturbine.tasks SET state = $1 WHERE task_id = $2")
            .bind(TaskState::Sleeping)
            .bind(task_id.0)
            .execute(&mut *conn)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(())
    }

    /// Record an external event that a task/run is waiting for.
    /// This is ideal for receiving webhooks, or waiting for other tasks
    /// to complete.
    ///
    /// Tasks can wait for events with [`Storage::await_event()`]
    pub async fn emit_event(
        &self,
        event_name: &str,
        payload: &[u8],
    ) -> Result<(), TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;

        let _ = sqlx::query(
            "INSERT INTO taskturbine.events (usecase, event_name, payload, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (usecase, event_name)
            DO UPDATE 
            SET payload = excluded.payload,
                created_at = excluded.created_at",
        )
        .bind(&self.config.usecase)
        .bind(event_name)
        .bind(payload)
        .execute(&mut *atomic)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        // Wake up the task/run.
        // Clear any valid waits, and wake up those runs.
        let _ = sqlx::query(
            "WITH matching_waits AS (
                DELETE FROM taskturbine.waits
                WHERE event_name = $1
                AND (timeout_at IS NULL OR timeout_at >= NOW())
                RETURNING run_id
            ),
            updated_runs AS (
                UPDATE taskturbine.runs
                SET state = $2,
                    available_at = NOW(),
                    claimed_by = NULL,
                    claim_expires_at = NULL
                WHERE run_id IN (SELECT run_id FROM matching_waits)
                RETURNING task_id
            )
            UPDATE taskturbine.tasks
            SET state = $2
            WHERE task_id IN (SELECT task_id FROM updated_runs)
        ",
        )
        .bind(event_name)
        .bind(TaskState::Pending)
        .execute(&mut *atomic)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        let _ = atomic.commit().await;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AwaitResult {
    pub payload: Vec<u8>,
    pub should_suspend: bool,
}

#[cfg(test)]
mod tests {
    use tokio::time;

    use super::*;

    async fn create_storage() -> Storage {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL")
            .expect("Missing required TASKTURBINE_DATABASE_URL env var");
        let config = Config {
            usecase: "test".to_string(),
            database_url: db_url,
            worker_sleep_secs: 2,
            worker_cleanup_cutoff_secs: 500,
            worker_cleanup_probability: 0.1,
            worker_cleanup_limit: 1000,
        };
        let storage = Storage::new(config);

        // Ensure migrations have been applied and that storage is cleared.
        storage.update_schema().await.unwrap();

        storage
    }

    async fn create_task() -> Result<(Storage, SpawnResult), TaskTurbineError> {
        let storage = create_storage().await;
        let namespace = "demo";
        let task_name = "say_hello";
        let payload = b"{\"key\": \"value\"}";

        let result = storage
            .spawn_task(namespace, task_name, payload, None)
            .await;
        assert!(result.is_ok(), "Failed to spawn task {:?}", result.err());
        let spawned = result.unwrap();

        Ok((storage, spawned))
    }

    #[tokio::test]
    async fn spawn_task_invalid_retry_factor() {
        let storage = create_storage().await;
        let namespace = "demo";
        let task_name = "say_hello";
        let payload = b"{\"key\": \"value\"}";

        let result = storage
            .spawn_task(
                namespace,
                task_name,
                payload,
                Some(TaskOptions {
                    retry_factor: 0.0,
                    ..Default::default()
                }),
            )
            .await;
        assert!(result.is_err(), "Should fail");
        let err = result.err().unwrap();
        assert!(matches!(err, TaskTurbineError::ValidationError(..)));
    }

    #[tokio::test]
    async fn spawn_task_get_task_id() {
        let (_, spawned) = create_task().await.unwrap();
        assert!(!spawned.task_id.0.to_string().is_empty());
        assert!(!spawned.run_id.to_string().is_empty());
    }

    #[tokio::test]
    async fn complete_run_not_running() {
        let (storage, spawned) = create_task().await.unwrap();
        let res = storage
            .complete_run(spawned.run_id, b"{\"result\": \"success\"}")
            .await;
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::NotRunning { .. }
        ));
    }

    #[tokio::test]
    async fn complete_run_success() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let res = storage
            .complete_run(spawned.run_id, b"{\"result\": \"success\"}")
            .await;
        assert!(res.is_ok(), "Failed to complete run: {res:?}");
    }

    #[tokio::test]
    async fn complete_run_clears_waits() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce task & run to running state
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        // Register a wait, run will become sleeping
        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "step_name",
                "event_name",
                None,
            )
            .await;
        assert!(res.is_ok());

        // Coerce back to running state
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        // complete the run
        let res = storage
            .complete_run(spawned.run_id, b"{\"result\": \"success\"}")
            .await;

        assert!(res.is_ok());
        let wait_res = storage.get_wait_by_run_id(spawned.run_id).await;

        assert!(wait_res.is_ok());
        assert!(
            wait_res.unwrap().is_none(),
            "wait should be deleted on run completion"
        );
    }

    #[tokio::test]
    async fn fail_run_missing() {
        let storage = create_storage().await;
        let id = RunId(Uuid::now_v7());
        let res = storage.fail_run(id, b"", None).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, TaskTurbineError::NotFound { .. }));
    }

    #[tokio::test]
    async fn fail_run_ok_no_retry_at() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let res = storage
            .fail_run(
                spawned.run_id,
                b"{\"error\": \"something went wrong\"}",
                None,
            )
            .await;
        assert!(res.is_ok(), "Failed to fail run: {res:?}");
    }

    #[tokio::test]
    async fn fail_run_can_fail_task() {
        let storage = create_storage().await;
        let options = TaskOptions {
            max_attempts: 0,
            ..TaskOptions::default()
        };
        let spawned = storage
            .spawn_task("ns", "task-1", b"", Some(options))
            .await
            .unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let res = storage
            .fail_run(
                spawned.run_id,
                b"{\"error\": \"something went wrong\"}",
                None,
            )
            .await;
        assert!(res.is_ok(), "Failed to fail run: {res:?}");
        let run = storage.get_run(spawned.run_id).await.unwrap();
        assert!(matches!(
            run.get::<TaskState, _>("state"),
            TaskState::Failed
        ));
    }

    #[tokio::test]
    async fn fail_run_ok_with_retry_at() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let retry_at = Utc::now() + Duration::from_secs(120);
        let res = storage
            .fail_run(
                spawned.run_id,
                b"{\"error\": \"something went wrong\"}",
                Some(retry_at),
            )
            .await;
        assert!(res.is_ok(), "Failed to fail run: {res:?}");
    }

    #[tokio::test]
    async fn fail_run_remove_wait() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce task & run to running state
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        // Register a wait
        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "step_name",
                "event_name",
                None,
            )
            .await;
        assert!(res.is_ok());

        // Fail the run
        let res = storage
            .fail_run(
                spawned.run_id,
                b"{\"error\": \"something went wrong\"}",
                None,
            )
            .await;
        dbg!(&res);
        assert!(res.is_ok());
        let wait_res = storage.get_wait_by_run_id(spawned.run_id).await;
        assert!(wait_res.is_ok());
        let wait = wait_res.unwrap();
        assert!(wait.is_none(), "wait should be deleted on fail");
    }

    #[tokio::test]
    async fn await_event_missing_run() {
        let storage = create_storage().await;
        let task_id = TaskId(Uuid::now_v7());
        let run_id = RunId(Uuid::now_v7());
        let res = storage
            .await_event(task_id, run_id, "step_name", "event_name", None)
            .await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, TaskTurbineError::NotFound(_)));
    }

    #[tokio::test]
    async fn await_event_not_running() {
        let (storage, spawned) = create_task().await.unwrap();

        // Fails because the run is not running.
        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "step_name",
                "event_name",
                None,
            )
            .await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        assert!(matches!(err, TaskTurbineError::NotRunning(_)));
    }

    #[tokio::test]
    async fn await_event_reads_from_existing_checkpoint() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce to running and set a checkpoint
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;
        let _ = storage
            .set_checkpoint(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                b"results",
                None,
            )
            .await;

        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                "event_name",
                None,
            )
            .await;
        assert!(res.is_ok());
        let await_result = res.unwrap();

        assert!(!await_result.should_suspend);
        assert_eq!(await_result.payload, b"results");

        let run = storage.get_run(spawned.run_id).await.unwrap();
        assert_eq!(run.get::<String, _>("state"), "running");
    }

    #[tokio::test]
    async fn await_event_record_wait_advance_to_sleeping() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce to running and store a wait
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                "event_name",
                None,
            )
            .await;
        assert!(res.is_ok());
        let await_result = res.unwrap();
        assert!(await_result.should_suspend);
        assert_eq!(await_result.payload, b"");

        let run = storage.get_run(spawned.run_id).await.unwrap();
        assert_eq!(run.get::<String, _>("state"), "sleeping");
    }

    #[tokio::test]
    async fn await_event_has_event() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce to running and set a checkpoint
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let task_id = spawned.task_id;
        let event_name = format!("event-{task_id}");
        let _ = storage.emit_event(&event_name, b"event-payload").await;

        // Should get the event payload back
        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                &event_name,
                None,
            )
            .await;
        assert!(res.is_ok());
        let await_result = res.unwrap();
        assert_eq!(await_result.payload, b"event-payload");
        assert!(!await_result.should_suspend);

        let run = storage.get_run(spawned.run_id).await.unwrap();
        assert_eq!(run.get::<String, _>("state"), "running");
    }

    #[tokio::test]
    async fn set_checkpoint_extend_claim() {
        let (storage, spawned) = create_task().await.unwrap();

        let now = Utc::now();
        let res = storage
            .set_checkpoint(
                spawned.task_id,
                spawned.run_id,
                "step-1",
                b"event-payload",
                Some(Duration::from_secs(5 * 60)),
            )
            .await;
        assert!(res.is_ok());

        let run = storage.get_run(spawned.run_id).await.unwrap();
        let claim_expires = run.get::<DateTime<Utc>, _>("claim_expires_at");
        let delta = claim_expires - now;
        assert!(
            delta.num_seconds() >= 300,
            "claim should expire at least 290s in the future "
        );

        // Ensure the checkpoint stores state as well.
        let checkpoint_opt = storage
            .get_checkpoint(spawned.task_id, "step-1")
            .await
            .unwrap();
        assert!(checkpoint_opt.is_some());
        let checkpoint = checkpoint_opt.unwrap();
        assert_eq!(b"event-payload".to_vec(), checkpoint.state);
    }

    #[tokio::test]
    async fn emit_event_records() {
        let storage = create_storage().await;
        let uuid = Uuid::now_v7();
        let event_id = format!("event-{uuid}");
        let res = storage.emit_event(&event_id, b"payload data").await;
        assert!(res.is_ok());

        let res = storage.get_event_row(&event_id).await;
        assert!(res.is_ok());
        let opt = res.unwrap();
        assert!(opt.is_some());
        let event = opt.unwrap();
        assert_eq!(b"payload data".to_vec(), event.get::<Vec<u8>, _>("payload"));
    }

    #[tokio::test]
    async fn emit_event_clears_task_waits() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;
        let uuid = Uuid::now_v7();
        let event_id = format!("event-{uuid}");

        let res = storage
            .await_event(spawned.task_id, spawned.run_id, "step-1", &event_id, None)
            .await;
        assert!(res.is_ok());

        let res = storage.get_wait_by_run_id(spawned.run_id).await;
        let opt = res.unwrap();
        assert!(opt.is_some(), "a wait should be saved");

        // Capture an event which should wait up the task
        let res = storage.emit_event(&event_id, b"payload data").await;
        assert!(res.is_ok());

        let res = storage.get_wait_by_run_id(spawned.run_id).await;
        let opt = res.unwrap();
        assert!(opt.is_none(), "no wait should remain");

        let run = storage.get_run(spawned.run_id).await.unwrap();
        assert_eq!(run.get::<TaskState, _>("state"), TaskState::Pending);

        let task = storage.get_task(spawned.task_id).await.unwrap().unwrap();
        assert_eq!(task.get::<TaskState, _>("state"), TaskState::Pending);
    }

    #[tokio::test]
    async fn test_get_checkpoint_and_set() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce to running and set a checkpoint
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;
        let _ = storage
            .set_checkpoint(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                b"results",
                None,
            )
            .await;
        let res = storage.get_checkpoint(spawned.task_id, "first-step").await;
        let maybe_checkpoint = res.unwrap();
        let checkpoint = maybe_checkpoint.unwrap();
        assert_eq!(b"results".to_vec(), checkpoint.state);
    }

    #[tokio::test]
    async fn test_get_checkpoints() {
        let (storage, spawned) = create_task().await.unwrap();

        // Coerce to running and set a checkpoint
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;
        let _ = storage
            .set_checkpoint(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                b"results",
                None,
            )
            .await;
        let _ = storage
            .set_checkpoint(
                spawned.task_id,
                spawned.run_id,
                "second-step",
                b"second result",
                None,
            )
            .await;

        let res = storage.get_checkpoints(spawned.task_id).await;
        let rows = res.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(b"results".to_vec(), rows[0].state);
        assert_eq!(b"second result".to_vec(), rows[1].state);
    }

    #[tokio::test]
    async fn test_schedule_run_fail_not_running() {
        let (storage, spawned) = create_task().await.unwrap();

        let later = Utc::now() + Duration::from_secs(5 * 60);
        let res = storage.schedule_run(spawned.run_id, later).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::NotRunning(_)
        ));
    }

    #[tokio::test]
    async fn test_schedule_run_running() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let later = Utc::now() + Duration::from_secs(5 * 60);
        let res = storage.schedule_run(spawned.run_id, later).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn cleanup_events_with_limit() {
        let storage = create_storage().await;
        let _ = storage.emit_event("event-1", b"hi").await;
        let _ = storage.emit_event("event-2", b"hi").await;
        let _ = storage.emit_event("event-3", b"hi").await;

        // Use future time as event times are not mockable/mutatable
        let cutoff = Utc::now() + Duration::from_secs(60);
        let res = storage.cleanup_events(cutoff, 2).await;
        assert!(res.is_ok());
        assert_eq!(2, res.unwrap());
    }

    #[tokio::test]
    async fn cleanup_tasks_with_limit() {
        let storage = create_storage().await;
        let _ = storage.clear_storage().await;

        let completed = storage
            .spawn_task("ns", "task1", b"{}", None)
            .await
            .unwrap();
        let _ = storage
            .set_run_state(completed.task_id, TaskState::Running)
            .await;
        let _ = storage.complete_run(completed.run_id, b"").await;

        // Skip any retries.
        let options = TaskOptions {
            max_attempts: 0,
            ..TaskOptions::default()
        };
        let failed = storage
            .spawn_task("ns", "task1", b"{}", Some(options))
            .await
            .unwrap();
        let _ = storage
            .set_run_state(failed.task_id, TaskState::Running)
            .await;
        let _ = storage.fail_run(failed.run_id, b"", None).await;

        let pending = storage
            .spawn_task("ns", "task1", b"{}", None)
            .await
            .unwrap();

        // Use a time in the future as I've not built methods
        // to manipulate time of tasks.
        let cutoff = Utc::now() + Duration::from_secs(60 * 5);
        let res = storage.cleanup_tasks(cutoff, 2).await;
        assert!(res.is_ok());
        assert_eq!(1, res.unwrap());

        let task = storage.get_task(pending.task_id).await.unwrap().unwrap();
        assert_eq!(task.get::<Option<DateTime<Utc>>, _>("completed_at"), None);
    }

    #[tokio::test]
    async fn claim_task_zero_qty() {
        let storage = create_storage().await;
        let timeout = Utc::now() + Duration::from_secs(60 * 5);
        let res = storage.claim_task("worker-1", timeout, 0).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::ValidationError(_)
        ));
    }

    #[tokio::test]
    async fn claim_task_past_expiration() {
        let storage = create_storage().await;
        let timeout = Utc::now() - Duration::from_secs(1);
        let res = storage.claim_task("worker-1", timeout, 0).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::ValidationError(_)
        ));
    }

    #[tokio::test]
    async fn claim_task_success() {
        let storage = create_storage().await;
        let _ = storage.clear_storage().await;
        let timeout = Utc::now() + Duration::from_secs(30);

        let _ = storage.spawn_task("test", "hello-world", b"", None).await;
        let _ = storage.spawn_task("test", "hello-world", b"", None).await;

        let res = storage.claim_task("worker-1", timeout, 1).await;
        assert!(res.is_ok());

        let claimed = res.unwrap();
        assert_eq!(claimed.len(), 1);
        let first_claim = &claimed[0];
        assert_eq!(first_claim.task_name, "hello-world");

        let res = storage.claim_task("worker-1", timeout, 100).await;
        assert!(res.is_ok());
        assert!(claimed.len() < 100, "");
    }

    #[tokio::test]
    async fn claim_task_complete_run_workflow() {
        let storage = create_storage().await;
        let _ = storage.clear_storage().await;
        let timeout = Utc::now() + Duration::from_secs(30);

        let _ = storage.spawn_task("test", "hello-world", b"", None).await;

        let res = storage.claim_task("worker-1", timeout, 1).await;
        assert!(res.is_ok());
        let claimed = res.unwrap();
        assert!(!claimed.is_empty());

        let res = storage.complete_run(claimed[0].run_id, b"").await;
        assert!(res.is_ok());

        let task = storage.get_task(claimed[0].task_id).await.unwrap().unwrap();
        assert_eq!(task.get::<String, _>("task_name"), "hello-world");
        assert_eq!(task.get::<String, _>("state"), "completed");
    }

    #[tokio::test]
    async fn handle_expired_claims() {
        let storage = create_storage().await;
        let _ = storage.clear_storage().await;
        let timeout = Utc::now() + Duration::from_secs(1);

        let _ = storage.spawn_task("test", "hello-world", b"", None).await;
        let res = storage.claim_task("worker-1", timeout, 1).await;
        assert!(res.is_ok());

        time::sleep(Duration::from_secs(2)).await;

        let res = storage.handle_expired_claims().await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), 1);
    }

    #[tokio::test]
    async fn extend_claim_on_run_not_running() {
        let (storage, spawned) = create_task().await.unwrap();
        let timeout = Utc::now() + Duration::from_secs(1);

        let res = storage
            .extend_claim("worker-1", spawned.run_id, timeout)
            .await;
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::NotRunning(_)
        ));
    }

    #[tokio::test]
    async fn extend_claim_on_run_running() {
        let (storage, _) = create_task().await.unwrap();
        let timeout = Utc::now() + Duration::from_secs(1);

        let res = storage.claim_task("worker-1", timeout, 1).await;
        assert!(res.is_ok());
        let claimed = &res.unwrap()[0];

        let extended_timeout = Utc::now() + Duration::from_secs(60);
        let res = storage
            .extend_claim("worker-1", claimed.run_id, extended_timeout)
            .await;
        assert!(res.is_ok());

        let run = storage.get_run(claimed.run_id).await.unwrap();
        assert!(
            run.get::<DateTime<Utc>, _>("claim_expires_at") >= timeout + Duration::from_secs(30),
            "Should be after the original timeout."
        );
    }

    #[tokio::test]
    async fn extend_claim_on_other_worker() {
        let (storage, _) = create_task().await.unwrap();
        let timeout = Utc::now() + Duration::from_secs(1);

        let res = storage.claim_task("worker-1", timeout, 1).await;
        assert!(res.is_ok());
        let claimed = &res.unwrap()[0];

        let extended_timeout = Utc::now() + Duration::from_secs(60);
        let res = storage
            .extend_claim("worker-2", claimed.run_id, extended_timeout)
            .await;
        assert!(res.is_err());
        assert!(matches!(
            res.err().unwrap(),
            TaskTurbineError::NotRunning(_)
        ));
    }
}
