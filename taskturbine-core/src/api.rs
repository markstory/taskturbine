use std::collections::HashMap;

use crate::config::Config;
use chrono::{DateTime, Duration, Utc};
use sqlx::{
    PgConnection, PgPool, Postgres, Row, Transaction, migrate::MigrateError, postgres::PgRow,
    query::Query,
};
use uuid::Uuid;

#[derive(Debug)]
pub enum TaskTurbineError {
    EncodeError(serde_json::Error),
    SqlError(sqlx::Error),
    NotFound(Uuid),
    NotRunning(Uuid),
    ValidationError(&'static str),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum TaskState {
    Pending,
    Running,
    Sleeping,
    Completed,
    Failed,
    Cancelled,
}

/// Result of spawning a task.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpawnResult {
    pub task_id: Uuid,
    pub run_id: Uuid,
}

/// Entity structure for a task
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct Task {
    pub task_id: Uuid,
    pub namespace: String,
    pub task_name: String,
    pub params: Vec<u8>,
    pub headers: Vec<u8>,
    pub retry_seconds: i32,
    pub retry_factor: f64,
    pub retry_max_seconds: i32,
    pub attempts: i32,
    pub max_attempts: i32,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub cancellation_max_age: i32,
    pub enqueue_at: DateTime<Utc>,
    pub state: TaskState,
    pub last_attempt_run: Option<Uuid>,
}
impl Task {
    /// Calculate the next retry based on retry attributes.
    pub fn next_retry_at(&self) -> DateTime<Utc> {
        let now = Utc::now();
        let total_delay = self.retry_seconds as f64 * self.retry_factor.powi(self.attempts);
        let capped = total_delay.min(self.retry_max_seconds as f64);
        now + chrono::Duration::seconds(capped as i64)
    }
}

/// Entity structure for a task checkpoint
#[derive(sqlx::FromRow, Debug, PartialEq)]
pub struct Checkpoint {
    task_id: Uuid,
    step_name: String,
    state: Vec<u8>,
    owner_run_id: Uuid,
    updated_at: DateTime<Utc>,
}

/// Options for spawning a task.
/// Default values are drawn from the TaskRuntime and TaskOptions defaults.
pub struct TaskOptions {
    /// Map of headers to include with the task activation
    headers: HashMap<String, String>,

    /// The maximum number of attempts to make on this task
    max_attempts: i32,

    /// The minimum number of seconds to wait between retries.
    retry_seconds: i32,

    /// The multipier to apply to retry delays between attempts.
    /// Use > 1.0 to create exponential backoff.
    retry_factor: f64,

    /// The maximum number of seconds to wait between retries.
    retry_max_seconds: i32,

    /// The maximum age of a task before it should not be run.
    /// Measured in seconds from when the task was first stored.
    cancellation_max_age: i32,
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
        Self { config, pool }
    }

    /// {{{ Testing helpers
    /// Testing helper: Delete all data from the storage tables.
    #[cfg(test)]
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

    /// Testing Helper: setting run + task to a specific state.
    #[cfg(test)]
    async fn set_run_state(&self, task_id: Uuid, state: TaskState) -> Result<(), TaskTurbineError> {
        let res = sqlx::query(
            "UPDATE taskturbine.runs
            SET state = $1
            WHERE task_id = $2",
        )
        .bind(state)
        .bind(task_id)
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
        .bind(task_id)
        .execute(&self.pool)
        .await;

        if let Err(e) = res {
            return Err(TaskTurbineError::SqlError(e));
        }
        Ok(())
    }

    /// Testing helper: reading task runs
    #[cfg(test)]
    async fn get_run(&self, run_id: Uuid) -> Result<PgRow, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.runs WHERE run_id = $1")
            .bind(run_id)
            .fetch_one(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    // Testing helper: get waits for a run
    #[cfg(test)]
    async fn get_wait_by_run_id(&self, run_id: Uuid) -> Result<Option<PgRow>, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.waits WHERE run_id = $1")
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(TaskTurbineError::SqlError)?;

        Ok(res)
    }

    // Testing helper: get a run
    #[cfg(test)]
    async fn get_task(&self, task_id: Uuid) -> Result<Option<PgRow>, TaskTurbineError> {
        let res = sqlx::query("SELECT * FROM taskturbine.tasks WHERE task_id = $1")
            .bind(task_id)
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
        payload: &[u8],
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
                task_id, namespace, task_name, params, headers,
                retry_seconds, retry_factor, retry_max_seconds,
                max_attempts, cancellation_max_age, enqueue_at, state
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11)",
        )
        .bind(task_id)
        .bind(namespace)
        .bind(task_name)
        .bind(payload)
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

        Ok(SpawnResult { task_id, run_id })
    }

    /// Get a run state in FOR UPDATE mode
    async fn get_locked_run_state(
        &self,
        conn: &mut PgConnection,
        run_id: Uuid,
    ) -> Result<PgRow, TaskTurbineError> {
        let res =
            sqlx::query("SELECT task_id, state FROM taskturbine.runs WHERE run_id = $1 FOR UPDATE")
                .bind(run_id)
                .fetch_one(&mut *conn)
                .await;

        if let Err(_) = res {
            return Err(TaskTurbineError::NotFound(run_id));
        }

        let row = res.unwrap();
        Ok(row)
    }

    /// Get a task record locked with FOR UPDATE
    async fn get_locked_task(
        &self,
        task_id: Uuid,
        conn: &mut PgConnection,
    ) -> Result<Task, TaskTurbineError> {
        let row: Task = sqlx::query_as(
            "SELECT *
             FROM taskturbine.tasks
             WHERE task_id = $1
             FOR UPDATE",
        )
        .bind(task_id)
        .fetch_one(&mut *conn)
        .await
        .map_err(|_| TaskTurbineError::NotFound(task_id))?;

        Ok(row)
    }

    /// Mark a run as completed with the provided state.
    /// When a run is completed, the task is also considered complete.
    pub async fn complete_run(
        &self,
        run_id: Uuid,
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
            return Err(TaskTurbineError::NotRunning(run_id));
        }
        let res = sqlx::query(
            "UPDATE taskturbine.runs
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
            SET state = $1, last_attempt_run = $2 WHERE task_id = $3",
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
        run_id: Uuid,
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
        run_id: Uuid,
        reason: &[u8],
        retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;

        let run_row = self.get_locked_run_state(&mut atomic, run_id).await?;
        let state: TaskState = run_row.get("state");
        match state {
            TaskState::Running | TaskState::Sleeping => {}
            _ => {
                // If the run is not active/sleeping it cannot be failed.
                atomic.commit().await.map_err(TaskTurbineError::SqlError)?;
                return Err(TaskTurbineError::NotRunning(run_id));
            }
        }
        let mut task = self
            .get_locked_task(run_row.get("task_id"), &mut atomic)
            .await?;
        let res = sqlx::query(
            "UPDATE taskturbine.runs
            SET state = $1, failed_at = NOW(), 
                wake_event = NULL, failure_reason = $2
            WHERE run_id = $3"
        )
        .bind(TaskState::Failed)
        .bind(reason)
        .bind(run_id)
        .execute(&mut *atomic)
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
                if next_available_at.signed_duration_since(task.enqueue_at) >= max_age {
                    cancel = true;
                }
            }
            // Advance attempt state
            task.attempts = next_attempt;
            task.last_attempt_run = Some(run_id);

            if cancel {
                // Move to cancelled state
                task.state = TaskState::Cancelled;
                task.cancelled_at = Some(now);
            } else {
                // Not cancelled, advance to next state
                task.cancelled_at = None;
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
                .execute(&mut *atomic)
                .await
                .map_err(TaskTurbineError::SqlError)?;
            }
        }

        let _ = sqlx::query(
            "UPDATE taskturbine.tasks
            SET state = $1, 
                attempts = $2, 
                last_attempt_run = $3, 
                cancelled_at = COALESCE(cancelled_at, $4)
            WHERE task_id = $5",
        )
        .bind(task.state)
        .bind(task.attempts)
        .bind(task.last_attempt_run)
        .bind(task.cancelled_at)
        .bind(task.task_id)
        .execute(&mut *atomic)
        .await
        .map_err(TaskTurbineError::SqlError)?;

        self.clear_waits(run_id, &mut atomic).await?;

        atomic.commit().await.map_err(TaskTurbineError::SqlError)?;
        Ok(())
    }

    /// Get the state of a single checkpoint
    pub async fn get_checkpoint(&self, task_id: Uuid, step_name: &str) -> Result<Option<Checkpoint>, TaskTurbineError> {
        let res: Option<Checkpoint> = sqlx::query_as(
            "SELECT * FROM taskturbine.checkpoints
            WHERE task_id = $1 AND step_name = $2"
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
    pub async fn get_checkpoints(&self, task_id: Uuid) -> Result<Vec<Checkpoint>, TaskTurbineError> {
        let res: Vec<Checkpoint> = sqlx::query_as(
            "SELECT * FROM taskturbine.checkpoints WHERE task_id = $1 ORDER by updated_at"
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
        task_id: Uuid,
        run_id: Uuid,
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
            let seconds = extension.num_seconds();
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
        task_id: Uuid,
        run_id: Uuid,
        step_name: &str,
        event_name: &str,
        timeout: Option<i32>,
    ) -> Result<AwaitResult, TaskTurbineError> {
        let mut atomic = self
            .pool
            .begin()
            .await
            .map_err(TaskTurbineError::SqlError)?;

        // Ensure the task & run exist and are running.
        let run_row = self.get_locked_run_state(&mut atomic, run_id).await?;
        if run_row.get::<TaskState, _>("state") != TaskState::Running {
            return Err(TaskTurbineError::NotRunning(run_id));
        }

        // Fetch the checkpoint if it exists
        let checkpoint_opt = sqlx::query(
            "SELECT state FROM taskturbine.checkpoints
            WHERE task_id = $1 AND step_name = $2",
        )
        .bind(task_id)
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
            Utc::now() + Duration::seconds(timeout as i64)
        } else {
            // TODO use config for default timeout
            Utc::now() + Duration::seconds(60 * 10)
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
        task_id: &Uuid,
        run_id: &Uuid,
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
        .bind(task_id)
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
        task_id: &Uuid,
        run_id: &Uuid,
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
        .bind(task_id)
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
            WHERE event_name = $1",
        )
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
        task_id: &Uuid,
        run_id: &Uuid,
        available_at: DateTime<Utc>,
    ) -> Result<(), TaskTurbineError> {
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
            .bind(task_id)
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
            "INSERT INTO taskturbine.events (event_name, payload, created_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (event_name)
            DO UPDATE 
            SET payload = excluded.payload,
                created_at = excluded.created_at"
        )
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
        ")
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
    use super::*;

    async fn create_storage() -> Storage {
        let db_url = std::env::var("TASKTURBINE_DATABASE_URL").unwrap();
        let config = Config {
            database_url: db_url,
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
        assert!(!spawned.task_id.to_string().is_empty());
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
        let id = Uuid::now_v7();
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
    async fn fail_run_ok_with_retry_at() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage
            .set_run_state(spawned.task_id, TaskState::Running)
            .await;

        let retry_at = Utc::now() + chrono::Duration::seconds(120);
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
        assert!(res.is_ok());
        let wait_res = storage.get_wait_by_run_id(spawned.run_id).await;
        assert!(wait_res.is_ok());
        let wait = wait_res.unwrap();
        assert!(wait.is_none(), "wait should be deleted on fail");
    }

    #[tokio::test]
    async fn await_event_missing_run() {
        let storage = create_storage().await;
        let task_id = Uuid::now_v7();
        let run_id = Uuid::now_v7();
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
        let event_id = format!("event-{task_id}");
        let _ = storage.emit_event(&event_id, b"event-payload").await;

        // Should get the event payload back
        let res = storage
            .await_event(
                spawned.task_id,
                spawned.run_id,
                "first-step",
                &event_id,
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
                Some(Duration::minutes(5)),
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
        let checkpoint_opt = storage.get_checkpoint(spawned.task_id, "step-1").await.unwrap();
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
        assert_eq!(
            b"payload data".to_vec(),
            event.get::<Vec<u8>, _>("payload")
        );
    }

    #[tokio::test]
    async fn emit_event_clears_task_waits() {
        let (storage, spawned) = create_task().await.unwrap();
        let _ = storage.set_run_state(spawned.task_id, TaskState::Running).await;
        let uuid = Uuid::now_v7();
        let event_id = format!("event-{uuid}");

        let res = storage.await_event(
            spawned.task_id,
            spawned.run_id,
            "step-1",
            &event_id,
            None
        ).await;
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
}
