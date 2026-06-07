use axum::{
    Form, Router,
    extract::{Path, State},
    response::Html,
    routing::{get, post},
};
use db::{SALT, create_db};
use hmac::Mac;
use minijinja::{Environment, context, path_loader};
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use sqlx::{Pool, Postgres, Row};
use std::sync::Arc;
use tasks::{HmacSha256, make_task_app};
use taskturbine::app::TaskturbineApp;

mod db;
mod tasks;

struct AppState<'a> {
    db: Pool<Postgres>,
    tasks: TaskturbineApp,
    templates: Environment<'a>,
}

fn create_template_env() -> Environment<'static> {
    let mut env = Environment::new();

    env.set_loader(path_loader("templates"));

    env
}

#[tokio::main]
async fn main() {
    SimpleLogger::new().init().unwrap();

    let state = Arc::new(AppState {
        db: create_db().await,
        tasks: make_task_app(),
        templates: create_template_env(),
    });

    let app = Router::new()
        .route("/", get(home))
        .route("/register", get(register))
        .route("/register", post(process_register))
        .route("/verify/{user_id}/{token}", get(verify_user))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    log::info!("Listening on {}", listener.local_addr().unwrap());

    axum::serve(listener, app).await.unwrap();
}

async fn home<'a>(State(state): State<Arc<AppState<'a>>>) -> Html<String> {
    let tmpl = state.templates.get_template("home.html").unwrap();
    let html = tmpl.render(context!()).unwrap();

    Html(html)
}

#[derive(Deserialize, Serialize)]
struct Signup {
    name: String,
    email: String,
    org_name: String,
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
    state
        .tasks
        .spawn_task("register-user", params.as_bytes(), None)
        .await
        .unwrap();
    log::info!("Spawned registration task for {}", sign_up.email);

    let tmpl = state
        .templates
        .get_template("process-register.html")
        .unwrap();
    let html = tmpl.render(context!(name => "test")).unwrap();

    Html(html)
}

async fn verify_user<'a>(
    Path((user_id, token)): Path<(String, String)>,
    State(state): State<Arc<AppState<'a>>>,
) -> Html<String> {
    let Ok(user_id) = user_id.parse::<i64>() else {
        log::info!("Failed to parse user id");
        let tmpl = state.templates.get_template("verify-failed.html").unwrap();
        let html = tmpl.render(context!()).unwrap();
        return Html(html);
    };

    log::info!("Verifying user {user_id} with token {token}");
    // Note: this should have rate limiting and attempt tracking to avoid brute force attacks.
    let res = sqlx::query("SELECT * FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(&state.db)
        .await;

    let Ok(Some(row)) = res else {
        log::info!("Failed to load user");
        let tmpl = state.templates.get_template("verify-failed.html").unwrap();
        let html = tmpl.render(context!()).unwrap();
        return Html(html);
    };

    let mut mac = HmacSha256::new_from_slice(SALT.as_bytes()).unwrap();
    mac.update(row.get::<String, _>("email").as_bytes());

    let Ok(token_bytes) = hex::decode(token) else {
        log::info!("Failed to decode token");
        let tmpl = state.templates.get_template("verify-failed.html").unwrap();
        let html = tmpl.render(context!()).unwrap();
        return Html(html);
    };
    let Ok(_) = mac.verify_slice(token_bytes.as_slice()) else {
        log::info!("Failed to verify token hmac");
        let tmpl = state.templates.get_template("verify-failed.html").unwrap();
        let html = tmpl.render(context!()).unwrap();
        return Html(html);
    };

    // Save an event to continue the task workflow.
    let event_name = format!("email-verify-{}", row.get::<String, _>("email"));
    let res = state.tasks.emit_event(&event_name, b"").await;

    let Ok(_) = res else {
        log::info!("Failed to emit even");
        let tmpl = state.templates.get_template("verify-failed.html").unwrap();
        let html = tmpl.render(context!()).unwrap();
        return Html(html);
    };

    let tmpl = state.templates.get_template("verify-success.html").unwrap();
    let html = tmpl.render(context!()).unwrap();
    Html(html)
}
