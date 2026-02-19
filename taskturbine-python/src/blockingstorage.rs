use std::time::Duration;

use taskturbine_core::models::{RunId, TaskId};

/// Internal blocking storage adapter.
///
/// Bridges between the tokio based runtime of the rust library
/// with sync python. This is usually put into an Arc and shared
/// with multiple python classes.
pub struct BlockingStorage {
    /// The Storage interface. This struct generally needs to be run
    /// in a tokio runtime.
    inner: taskturbine_core::storage::Storage,

    /// The tokio runtime for interacting with taskturbine_core
    /// which is tokio based.
    rt: tokio::runtime::Runtime,
}

impl BlockingStorage {
    /// Create a new BlockingStorage instance
    pub fn new(config: taskturbine_core::config::Config) -> Self {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let inner = rt.block_on(taskturbine_core::storage::Storage::new_fut(config));

        Self { inner, rt }
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.spawn_task()`]
    pub fn spawn_task(
        &self,
        channel: &str,
        task_name: &str,
        params: &[u8],
        options: Option<taskturbine_core::storage::TaskOptions>,
    ) -> Result<taskturbine_core::models::SpawnResult, taskturbine_core::storage::TaskTurbineError>
    {
        self.rt
            .block_on(self.inner.spawn_task(channel, task_name, params, options))
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.spawn_task()`]
    pub fn emit_event(
        &self,
        event_name: &str,
        payload: &[u8],
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt.block_on(self.inner.emit_event(event_name, payload))
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.spawn_task()`]
    pub fn await_event(
        &self,
        task_id: TaskId,
        run_id: RunId,
        step_name: &str,
        event_name: &str,
        timeout: Duration,
    ) -> Result<taskturbine_core::storage::AwaitResult, taskturbine_core::storage::TaskTurbineError>
    {
        self.rt.block_on(self.inner.await_event(
            task_id,
            run_id,
            step_name,
            event_name,
            Some(timeout),
        ))
    }

    /// Make a blocking call to [`taskturbine_core::storage::Storage.claim_task()`]
    pub fn claim_task(
        &self,
        channels: Vec<&str>,
        worker_id: &str,
        claim_timeout: Duration,
        qty: i32,
    ) -> Result<
        Vec<taskturbine_core::models::ClaimedTask>,
        taskturbine_core::storage::TaskTurbineError,
    > {
        self.rt.block_on(
            self.inner
                .claim_task(channels, worker_id, claim_timeout, qty),
        )
    }

    pub fn get_checkpoint(
        &self,
        task_id: TaskId,
        step_name: &str,
    ) -> Result<
        Option<taskturbine_core::models::Checkpoint>,
        taskturbine_core::storage::TaskTurbineError,
    > {
        self.rt
            .block_on(self.inner.get_checkpoint(task_id, step_name))
    }

    pub fn set_checkpoint(
        &self,
        task_id: TaskId,
        run_id: RunId,
        step_name: &str,
        state: &[u8],
        extend_claim: Option<Duration>,
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt.block_on(
            self.inner
                .set_checkpoint(task_id, run_id, step_name, state, extend_claim),
        )
    }

    pub fn fail_run(
        &self,
        run_id: RunId,
        reason: &[u8],
        retry_at: Option<Duration>,
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt
            .block_on(self.inner.fail_run(run_id, reason, retry_at))
    }

    pub fn complete_run(
        &self,
        run_id: RunId,
        run_result: &[u8],
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt
            .block_on(self.inner.complete_run(run_id, run_result))
    }

    pub fn schedule_run(
        &self,
        run_id: RunId,
        wait_for: Duration,
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt.block_on(self.inner.schedule_run(run_id, wait_for))
    }

    pub fn run_cleanup(
        &self,
        older_than: Duration,
    ) -> Result<(), taskturbine_core::storage::TaskTurbineError> {
        self.rt.block_on(self.inner.run_cleanup(older_than))
    }

    /// Get the config of the application
    pub fn get_config(&self) -> taskturbine_core::config::Config {
        self.inner.get_config()
    }
}
