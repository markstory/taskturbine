use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use taskturbine_core::api::Storage;
use taskturbine_core::context::{FlowControl, TaskContext};
use taskturbine_core::models::ClaimedTask;

use crate::CliError;

pub async fn demo(storage: Storage) -> Result<(), CliError> {
    let timeout = Utc::now() + Duration::from_secs(60);
    let res = storage.claim_task("demo-1", timeout, 1).await;
    let claimed = if let Err(err) = res {
        return Err(CliError::Message(format!("{err:?}")));
    } else {
        res.unwrap()
    };
    let storage = Arc::new(storage);
    for task in claimed.iter() {
        let task_id = &task.task_id;

        println!("Attemting to execute {task_id}");
        execute_task(task, storage.clone()).await;
    }

    Ok(())
}

// Worker isolate. Ideally failures here don't spiral out.
async fn execute_task(task: &ClaimedTask, storage: Arc<Storage>) {
    let context = TaskContext::build(task.clone(), storage.clone());
    let taskname = &task.task_name;

    // TODO have a task registry, and do dynamic lookups and calls.
    if taskname == "hello_world" {
        // TODO parse args
        let res = hello_world(context).await;

        // Handle task execution results.
        match res {
            Err(FlowControl::InvalidValue(msg)) => println!("Invalid value {msg}"),
            Err(FlowControl::Failure(msg)) => {
                println!("Task run failure: {msg}");
                let retry_at = task.next_retry_at();
                let res = storage.fail_run(task.run_id, b"", Some(retry_at)).await;
                if let Err(schedule_err) = res {
                    println!("Failed to fail run {schedule_err:?}");
                }
            }
            Err(FlowControl::Suspend(wait_for)) => {
                let wake_at = Utc::now() + wait_for;

                let res = storage.schedule_run(task.run_id, wake_at).await;
                if let Err(schedule_err) = res {
                    println!("Failed to schedule run {schedule_err:?}");
                }
            }
            Ok(_) => {
                println!("Completed task {taskname}");
                let res = storage.complete_run(task.run_id, b"").await;
                if let Err(msg) = res {
                    println!("Failed to complete run {msg:?}");
                }
            }
        }
    }
}

// Userland code
async fn hello_world(mut ctx: TaskContext) -> Result<(), FlowControl> {
    println!("Ran 'userland' task function - hello_world");

    // let _ = ctx.sleep_for("sleepy-time", Duration::from_secs(20)).await?;
    // println!("Sleep completed");

    // Run synchronous steps
    fn step_one() -> Result<Vec<u8>, CliError> {
        println!("Ran step_one");
        Ok(b"a result value".to_vec())
    }
    let step1 = ctx.step("step-1-echo", step_one).await;
    println!("Step 1 result {step1:?}");

    // Run asynchronous steps
    async fn step_two() -> Result<Vec<u8>, CliError> {
        // println!("Ran step_two - fails");
        // Err(CliError::Message("step two failed".to_string()))

        println!("Ran step_two - ok");
        Ok(b"two results".to_vec())
    }
    let step2 = ctx.async_step("step-2-echo", step_two).await?;
    println!("Step 2 result {step2:?}");

    Ok(())
}
