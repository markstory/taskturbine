use std::time::Duration;

use chrono::Utc;
use taskturbine_core::api::{ClaimedTask, Storage};

use crate::CliError;


pub async fn demo(storage: Storage) -> Result<(), CliError> {
    let timeout = Utc::now() + Duration::from_secs(60);
    let res = storage.claim_task("demo-1", timeout, 1).await;
    let claimed = if let Err(err) = res {
        return Err(CliError::Message(format!("{err:?}")))
    } else {
        res.unwrap()
    };
    for task in claimed.iter() {
        let task_id = &task.task_id;
        println!("Attemting to execute {task_id}");

        match execute_task(task).await {
            Ok(_) => { 
                let _ = storage.complete_run(task.run_id, b"").await;
                println!("Task excecution complete");
            },
            Err(err) => {
                let retry_at = task.next_retry_at();
                let _ = storage.fail_run(task.run_id, b"", Some(retry_at)).await;
                println!("Task execution failed: {err:?}");
            }
        }
    }

    Ok(())
}

async fn execute_task(task: &ClaimedTask) -> Result<(), CliError> {
    let taskname = &task.task_name;

    // TODO have a task registry, and do dynamic lookups and calls.
    if taskname == "hello_world" {
        // TODO parse args, make context
        hello_world();
    }

    Ok(())
}

fn hello_world() {
    println!("Ran 'userland' task function - hello_world");
}
