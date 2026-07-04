# Taskturbine Core

Taskturbine core provides the core storage models and API for interacting with taskturbine application state in Postgres.

Using postgres as both a queue and state storage allows taskturbine to be operationally simple yet provide powerful features like retries, scheduling, workload separation, external synchronization and more.

## Application Frameworks

Generally this crate is consumed by using an application framework:

- Python - https://pypi.org/project/taskturbine/
- Rust - https://crates.io/crates/taskturbine/
