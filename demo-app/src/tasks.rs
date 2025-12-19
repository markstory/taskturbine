use taskturbine_core::{app::TaskturbineApp, context::{FlowControl, TaskContext}, config::Config};

pub fn make_task_app() -> TaskturbineApp {
    let task_config = Config {
        database_url: "postgresql://apps:password@localhost/test_taskturbine".into(),
        ..Config::default()
    };

    let app = TaskturbineApp::new(task_config)
        .add_channel("mail")
        .register_task("register-user", register_user);

    app
}

pub async fn register_user(_ctx: TaskContext) -> Result<(), FlowControl> {
    log::info!("starting register task");

    Ok(())
}
