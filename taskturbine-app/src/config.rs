use figment::{Provider, Error, Metadata, Profile, value::{Map, Dict}};
use serde::{Serialize, Deserialize};

use taskturbine_core::config::Config as CoreConfig;

/// Configuration options for Taskturbine rust applications.
///
/// This struct duplicates several options from taskturbine_core::config::Config
/// for ergonomics.
///
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct Config {
    // Attributes duplicated from taskturbine_core::config::Config
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

    // Attributes for taskturbine-app
    /// The default channel that tasks are spawned into.
    /// This channel will automatically be registered into the application
    /// using a config instance.
    pub default_channel: String,

    /// The number of task execution slots to start.
    /// More slots will enable more tasks to run concurrently.
    pub worker_concurrency: i32,

    /// The number of milliseconds a worker should sleep when
    //  one of the following happens:
    //
    //  - A worker attempts to claim tasks but none are found.
    //  - A worker claims tasks and can't send them to a worker queue.
    pub worker_sleep_ms: i32,

    /// The maximum number of completed tasks and events
    /// a worker will delete in a single cleanup operation.
    pub worker_cleanup_limit: i32,

    /// The age of completed tasks and events in seconds
    /// after now() that are safe to delete.
    pub worker_cleanup_cutoff_secs: i32,

    /// The minimum number of seconds between each cleanup operation.
    /// It is recommended you do this periodically to ensure that your
    /// database doesn't grow indefinitely.
    pub worker_upkeep_interval_secs: i32,

    /// Whether or not workers should run cleanup operations inline.
    /// Set to false if you are going to run cleanup workers separately.
    pub worker_upkeep_inline: bool,

    /// The number of seconds that workers will claim tasks for.
    /// Workers are expected to complete tasks within their claim timeout.
    /// After a claim timeout is exceeded, the task will be made pending again.
    /// Default value is 600 (10m)
    pub worker_claim_timeout_secs: i32,

    /// Whether or not the worker should shutdown on when it is idle.
    pub worker_shutdown_on_idle: bool,

    /// The number empty claim attempts to make before a worker considers
    /// itself idle. If `[worker_shutdown_on_idle]` is set, the worker
    /// will complete its run loop. This is used for integration testing.
    pub worker_shutdown_idle_max: i32,
}

impl Default for Config {
    fn default() -> Self {
        let core = CoreConfig::default();
        Config {
            database_url: core.database_url,
            database_log_queries: core.database_log_queries,
            usecase: core.usecase,
            await_event_default_timeout_secs: core.await_event_default_timeout_secs,

            default_channel: "default".to_string(),
            worker_concurrency: 3,
            worker_sleep_ms: 100,
            worker_upkeep_inline: true,
            worker_upkeep_interval_secs: 10,
            worker_cleanup_cutoff_secs: 60 * 10,
            worker_cleanup_limit: 1000,
            worker_claim_timeout_secs: 60 * 10,
            worker_shutdown_on_idle: false,
            worker_shutdown_idle_max: 5,
        }
    }
}

impl Provider for Config {
    /// Describe which provider configuration is coming from.
    fn metadata(&self) -> Metadata {
        Metadata::named("Taskturbine Config")
    }

    /// Get configuration data out.
    fn data(&self) ->  Result<Map<Profile, Dict>, Error> {
        figment::providers::Serialized::defaults(Config::default()).data()
    }
}

impl From<Config> for CoreConfig {
    // Create a taskturbine-core::config::Config from the application config
    // so that configuration can be passed down.
    fn from(val: Config) -> Self {
        CoreConfig {
            database_url: val.database_url,
            database_log_queries: val.database_log_queries,
            usecase: val.usecase,
            await_event_default_timeout_secs: val.await_event_default_timeout_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use figment::{Figment, providers::Env};

    #[test]
    fn config_uses_env_vars() {
        figment::Jail::expect_with(|jail| {
            jail.set_env("TASKTURBINE_DATABASE_URL", "postgresql://user:password@localhost/test");
            let builder = Figment::from(Config::default())
                .merge(Env::prefixed("TASKTURBINE_"));

            let config: Config = builder.extract().expect("Config should parse");
            assert_eq!("postgresql://user:password@localhost/test", config.database_url, "env var is included");
            assert_eq!("default", config.usecase, "defaults work too");

            Ok(())
        });
    }
}
