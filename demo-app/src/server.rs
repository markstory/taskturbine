use axum::{extract::State, response::Html, routing::{get, post}, Form, Router};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use tasks::make_task_app;
use taskturbine_core::app::TaskturbineApp;
use std::sync::Arc;
use minijinja::{Environment, context, path_loader};

mod tasks;

struct AppState<'a> {
    tasks: TaskturbineApp,
    templates: Environment<'a>,
}

fn create_template_env() -> Environment<'static> {
    let mut env = Environment::new();
    // env.add_template("register", "register html goes here {{ name }}").unwrap();
    // env.add_template("process-register", "{{ name }}. Your registration is processing.").unwrap();
    env.set_loader(path_loader("templates"));

    env
}

#[tokio::main]
async fn main() {
    SimpleLogger::new().init().unwrap();

    let state = Arc::new(AppState { 
        tasks: make_task_app(),
        templates: create_template_env(),
    });

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
    name: String,
    email: String,
}

async fn register<'a>(State(state): State<Arc<AppState<'a>>>) -> Html<String> {
    let tmpl = state.templates.get_template("register.html").unwrap();
    let html = tmpl.render(context!(name => "test")).unwrap();

    Html(html)
}

async fn process_register<'a>(
    State(state): State<Arc<AppState<'a>>>,
    Form(sign_up): Form<Signup>,
) -> Html<String> {
    // create a json payload for the task and queue a task
    let params = serde_json::to_string(&sign_up).unwrap();
    state.tasks.spawn_task("register-user", params.as_bytes(), None).await.unwrap();

    let tmpl = state.templates.get_template("process-register.html").unwrap();
    let html = tmpl.render(context!(name => "test")).unwrap();

    Html(html)
}
