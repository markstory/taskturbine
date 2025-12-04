#[derive(Debug, Clone)]
pub struct Config {
    /// The URI of the database your are connecting to.
    /// Example: postgresql://app:password@localhost/taskturbine
    pub database_url: String,

    /// The application or client that is connecting.
    /// Workers are bound to a specific usecase and can conditionally
    /// consume from one or more namespaces (aka. queues/topics/channels) 
    pub usecase: String,
}
