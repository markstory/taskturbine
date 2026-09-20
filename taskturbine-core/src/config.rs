/// Configuration options for Taskturbine core APIs
///
/// Each language application framework must provide a way to convert
/// application config into this struct to configure Storage.
#[derive(Debug, Clone)]
pub struct Config {
    /// The URI of the database your are connecting to.
    /// Example: postgresql://app:password@localhost/taskturbine
    pub database_url: String,

    /// Enable database logging at DEBUG level
    pub database_log_queries: bool,

    /// The application or client that is connecting.
    /// Workers are bound to a specific usecase and can conditionally
    /// consume from one or more channel (aka. queue/topic)
    pub usecase: String,

    /// The default number of seconds that events are waited on for.
    pub await_event_default_timeout_secs: i32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            database_url: "".to_string(),
            database_log_queries: false,
            usecase: "default".to_string(),
            await_event_default_timeout_secs: 120,
        }
    }
}
