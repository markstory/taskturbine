#[derive(Debug, Clone)]
pub struct Config {
    /// The URI of the database your are connecting to.
    /// Example: postgresql://app:password@localhost/taskturbine
    pub database_url: String,

    /// The application or client that is connecting.
    /// Workers are bound to a specific usecase and can conditionally
    /// consume from one or more namespaces (aka. queues/topics/channels) 
    pub usecase: String,

    /// The number of seconds a worker should sleep when no tasks are available.
    pub worker_sleep_secs: i32,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            database_url: "".to_string(),
            usecase: "default".to_string(),
            worker_sleep_secs: 2,
        }
    }
}
