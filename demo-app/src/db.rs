use std::env;

use sqlx::{migrate, postgres::PgConnectOptions, ConnectOptions, PgPool, Pool, Postgres};


pub async fn create_db() -> Pool<Postgres> {
    let database_url = env::var("DEMO_DATABASE_URL").expect("Missing DEMO_DATABASE_URL in env");
    let database_log_queries = env::var("DEMO_DATABASE_LOG_QUERIES").unwrap_or("false".into());
    let pool = PgPool::connect_lazy(&database_url)
        .expect("Failed to create database connection pool");

    let options: Result<PgConnectOptions, _> = database_url.parse();
    if let Ok(mut opts) = options {
        if database_log_queries == "true" {
            opts = opts.log_statements(log::LevelFilter::Debug);
        } else {
            opts = opts.disable_statement_logging();
        }
        pool.set_connect_options(opts);
    }
    migrate!("./migrations").run(&pool).await.expect("Migrations failed to run! Abort");

    pool
}
