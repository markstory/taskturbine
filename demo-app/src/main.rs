use axum::{extract::State, routing::{get, post}, Form, Router};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use tasks::make_task_app;
use taskturbine_core::app::TaskturbineApp;
use std::sync::Arc;

mod tasks;

struct AppState {
    tasks: TaskturbineApp,
}

#[tokio::main]
async fn main() {
    SimpleLogger::new().init().unwrap();

    let task_app = make_task_app();

    let state = Arc::new(AppState { tasks: task_app });

    let app = Router::new()
        .route("/", get(home))
        .route("/register", get(register))
        .route("/register", post(process_register))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    log::info!("Listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}

async fn home() -> &'static str {
    "Welcome home"
}

#[derive(Deserialize, Serialize)]
struct Signup {
    email: String,
}

async fn register() -> &'static str {
    "Register for our site"
}

async fn process_register(
    State(state): State<Arc<AppState>>,
    Form(sign_up): Form<Signup>,
) -> &'static str {
    // create a json payload for the task and queue a task
    let params = serde_json::to_string(&sign_up).unwrap();
    state.tasks.spawn_task("register-user", params.as_bytes(), None).await.unwrap();

    "all done"
}
