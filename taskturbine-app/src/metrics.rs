use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

/// Import and call during application startup to register all metrics if required.
pub fn setup_metrics() {
    describe_counter!("app.spawn_task", Unit::Count, "Number of tasks spawned");
    describe_counter!("app.emit_event", Unit::Count, "Number of events emit");
    describe_counter!(
        "worker.claim_tasks.claimed",
        Unit::Count,
        "Counter of tasks claimed each claim cycle."
    );
    describe_counter!(
        "worker.execute_task",
        Unit::Count,
        "Counter of tasks executed."
    );
    describe_counter!(
        "worker.execute_task.not_found",
        Unit::Count,
        "Counter of tasks that were not found in the task registry."
    );
    describe_counter!(
        "worker.execute_task.outcome",
        Unit::Count,
        "Counter of tasks by labeled by outcome."
    );
    describe_counter!(
        "worker.empty_sleep",
        Unit::Count,
        "Counter of claim attempts that didn't obtain any tasks."
    );
    describe_counter!(
        "worker.claim.work_send.full",
        Unit::Count,
        "Counter of claim attempts that failed because the worker thread queue was full."
    );
    describe_counter!(
        "context.await_event",
        Unit::Count,
        "Counter of task executions that called await_event"
    );

    describe_histogram!(
        "worker.execute_task.call.duration",
        Unit::Seconds,
        "Duration of execute_task calls"
    );
    describe_histogram!(
        "run_upkeep.duration",
        Unit::Seconds,
        "Duration of upkeep operations"
    );
    describe_histogram!(
        "worker.claim_tasks.duration",
        Unit::Seconds,
        "Duration of worker claim_task operations"
    );

    describe_gauge!(
        "worker.claim_tasks.idle_count",
        Unit::Count,
        "The number of claim_task that did not fetch any rows when idle shutdown is enabled"
    );
    describe_gauge!(
        "run_upkeep.total_count",
        Unit::Count,
        "The total number of tasks in the usecase. Collected during each upkeep operation"
    );
    describe_gauge!(
        "run_upkeep.pending_count",
        Unit::Count,
        "The number of tasks where state=pending in the usecase. Collected during each upkeep operation"
    );
    describe_gauge!(
        "run_upkeep.running_count",
        Unit::Count,
        "The number of tasks where state=running in the usecase. Collected during each upkeep operation"
    );
    describe_gauge!(
        "run_upkeep.sleeping_count",
        Unit::Count,
        "The number of tasks where state=sleeping in the usecase. Collected during each upkeep operation"
    );
}
