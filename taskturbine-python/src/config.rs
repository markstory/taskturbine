use pyo3::prelude::*;

/// See taskturbine.pyi for docstrings
#[pyclass]
#[derive(Debug, Clone)]
pub struct Config {
    #[pyo3(get, set)]
    pub app_module: String,

    #[pyo3(get, set)]
    pub database_url: String,

    #[pyo3(get, set)]
    pub database_log_queries: bool,

    #[pyo3(get, set)]
    pub usecase: String,

    #[pyo3(get, set)]
    pub default_channel: String,

    #[pyo3(get, set)]
    pub worker_concurrency: i32,

    #[pyo3(get, set)]
    pub worker_sleep_secs: i32,

    #[pyo3(get, set)]
    pub worker_cleanup_limit: i32,

    #[pyo3(get, set)]
    pub worker_cleanup_cutoff_secs: i32,

    #[pyo3(get, set)]
    pub worker_cleanup_interval_secs: i32,

    #[pyo3(get, set)]
    pub worker_cleanup_inline: bool,

    #[pyo3(get, set)]
    pub await_event_default_timeout_secs: i32,

    #[pyo3(get, set)]
    pub worker_claim_timeout_secs: i32,

    #[pyo3(get, set)]
    pub worker_max_tasks_per_child: i32,
}

/// Convert from the python module to the core struct.
impl From<Config> for taskturbine_core::config::Config {
    fn from(value: Config) -> Self {
        taskturbine_core::config::Config {
            database_url: value.database_url,
            database_log_queries: value.database_log_queries,
            usecase: value.usecase,
            default_channel: value.default_channel,
            worker_claim_timeout_secs: value.worker_claim_timeout_secs,
            worker_cleanup_cutoff_secs: value.worker_cleanup_cutoff_secs,
            worker_cleanup_inline: value.worker_cleanup_inline,
            worker_cleanup_interval_secs: value.worker_cleanup_interval_secs,
            worker_cleanup_limit: value.worker_cleanup_limit,
            worker_concurrency: value.worker_concurrency,
            worker_sleep_secs: value.worker_sleep_secs,
            await_event_default_timeout_secs: value.await_event_default_timeout_secs,
        }
    }
}

#[pymethods]
impl Config {
    #[new]
    #[pyo3(signature = (
        app_module,
        database_url,
        *,
        database_log_queries=false,
        usecase="default",
        default_channel="default",
        worker_claim_timeout_secs=600,
        worker_cleanup_cutoff_secs=600,
        worker_cleanup_inline=true,
        worker_cleanup_interval_secs=30,
        worker_cleanup_limit=1000,
        worker_concurrency=3,
        worker_sleep_secs=2,
        worker_max_tasks_per_child=1000,
        await_event_default_timeout_secs=120,
    ))]
    fn __new__(
        app_module: &str,
        database_url: &str,
        database_log_queries: bool,
        usecase: &str,
        default_channel: &str,
        worker_claim_timeout_secs: i32,
        worker_cleanup_cutoff_secs: i32,
        worker_cleanup_inline: bool,
        worker_cleanup_interval_secs: i32,
        worker_cleanup_limit: i32,
        worker_concurrency: i32,
        worker_sleep_secs: i32,
        worker_max_tasks_per_child: i32,
        await_event_default_timeout_secs: i32,
    ) -> Self {
        Config {
            app_module: app_module.to_string(),
            database_url: database_url.to_string(),
            database_log_queries,
            usecase: usecase.to_string(),
            default_channel: default_channel.to_string(),
            worker_concurrency,
            worker_sleep_secs,
            worker_cleanup_limit,
            worker_cleanup_interval_secs,
            worker_cleanup_inline,
            worker_cleanup_cutoff_secs,
            worker_claim_timeout_secs,
            worker_max_tasks_per_child,
            await_event_default_timeout_secs,
        }
    }
}
