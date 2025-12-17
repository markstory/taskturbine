use simple_logger::SimpleLogger;
use taskturbine_core::app::{TaskturbineApp, run_worker};
use taskturbine_core::context::{FlowControl, TaskContext};
use taskturbine_core::storage::Storage;

use crate::CliError;

// Demo application setup
pub async fn demo(storage: Storage) -> Result<(), CliError> {
    // Get the configuration from storage.
    // In userland code they would define the config, and apply to the App.
    let config = storage.get_config();

    // Setup basic logger
    SimpleLogger::new().init().unwrap();

    // Create an application instance
    let mut app = TaskturbineApp::new(config.clone());
    app = app
        .add_channel("ingest")
        .add_channel("reports")
        .register_task("hello_world", hello_world)
        .register_task("explode", explode)
        .register_task("sailboat", sailboat);

    let worker = app.create_worker("demo-worker-1", vec!["default".to_string(), "reports".to_string()]);
    run_worker(worker).await;
    Ok(())
}

// Userland task code
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

    let event = ctx.await_event("step-3-echo", None).await?;
    println!("Step 3 event {event:?}");

    Ok(())
}

// Userland task code
async fn sailboat(mut _ctx: TaskContext) -> Result<(), FlowControl> {
    println!("Ahoy! Setting sail in the sailboat task.");
    Ok(())
}

// Userland task code
async fn explode(mut _ctx: TaskContext) -> Result<(), FlowControl> {
    panic!("Oh no!");
}
