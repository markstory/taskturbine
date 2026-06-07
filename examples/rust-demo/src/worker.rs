use simple_logger::SimpleLogger;
use taskturbine::app::run_worker;

mod db;
mod tasks;

use tasks::make_task_app;

#[tokio::main]
async fn main() {
    SimpleLogger::new().init().unwrap();

    log::info!("Starting worker");
    let app = make_task_app();

    let worker = app.create_worker("worker-1", vec![]);
    run_worker(worker).await;
}
