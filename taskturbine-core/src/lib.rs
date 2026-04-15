//! A durable task framework for Rust and Python.
//!
//! # Overview
//!
//! Taskturbine provides a durable task framework backed by postgres.
//! The schema and basic storage API are inspired by [Absurd](https://github.com/earendil-works/absurd).
//!
//! Using postgres as both a queue and state storage allows taskturbine
//! to be operationally simple yet provide powerful features like
//! retries, scheduling, workload separation, external synchronization and more.
//!
//! # Durable execution?
//!
//! Durable execution enables you to build asynchronous, stateful, fault-tolerant,
//! applications. Durable execution provides a fault-tolerant approach to
//! running code, that is designed for failure and interuptions through retries
//! and persistence. By handling scheduling, state management and error handling
//! your applications can focus on solving the problem at hand.
//!
//! While durable execution systems cannot prevent your application from failing, but
//! it can greatly reduce the impact of those failures.
//!
//! # Tasks and Steps
//!
//! Application logic is defined as _tasks_. Tasks are functions that are composed of multiple
//! operations or _steps_. Tasks execute their steps in the order they are defined, and the result
//! of each step is stored. Should a task or step fail for any reason, a subsequent _run_ can be
//! scheduled. When the subsequent run is started, the application state is reconstructed from
//! stored _checkpoints_, and any completed steps are skipped. Tasks can also _sleep_ for a period
//! of time, or wait for an _event_ to be emit elsewhere in your application logic. Event payloads
//! are also stored allowing you to build race free synchronization logic and workflows.
//!
//! As an application grows, you'll likely want to isolate different workloads from each
//! other. To facilitate this, tasks can be _spawned_ into a named _channel_. Workers
//! claim and execute tasks from one or more channels. This enables you to have different
//! groups of workers for different workloads or customers.
//!
//! # Terminology
//!
//! - `usecase` The client application that a task belongs to. A single taskturbine
//!   database can be shared by multiple applications if required.
//! - `channel` Channels enable you to separate workloads within a `usecase`. For example, you may
//!   want many workers processing high-priority tasks, and fewer processing lower priority work.
//! - `task` A workflow or task that should be executed durably.
//! - `step` An incremental operation or side-effect that can succeed or fail. Steps
//!   act as error and persistence boundaries for your tasks. Steps that complete are not
//!   retried or run multiple times.
//! - `checkpoint` As steps are completed, checkpoints are created.
//! - `run` An attempt to execute a task. Each run can read checkpoints from previous
//!   runs, allowing tasks to resume where they left off.
//! - `event` Tasks can be suspended until named events are emit by the application. Events are
//!   ideal for waiting on webhooks, or other tasks to complete.
//! - `wait` When a task is waiting for an event, it records a `wait`.
//!
//! # Defining Tasks
//!
//! Tasks are defined as async functions with a signature like:
//!
//! ```rust
//! use taskturbine_core::context::{TaskContext, FlowControl};
//!
//! pub async fn do_some_task(mut ctx: TaskContext) -> Result<(), FlowControl> {
//!   todo!();
//! }
//! ```
//!
//! The [TaskContext](context/struct.TaskContext.html) exposes methods to define task steps,
//! wait for events, spawn tasks and work with the tasks' parameters.
//!
//! The [FlowControl](context/enum.FlowControl.html) error is used by taskturbine to represent
//! scenarios where task steps have failed due to application logic, or are waiting for events, or
//! for a sleep to expire. Task steps, events, and sleep operations will return `FlowControl` to
//! direct taskturbine on how to advance the statemachine for a task when the task is not yet
//! complete.
//!
//! ## Steps
//!
//! Tasks execute their steps in the order they are defined, and the result
//! of each step is stored. Should a task or step fail for any reason, a subsequent run can be
//! scheduled. When the task run is resumed, it will have access to any previously completed
//! step results, and completed steps will not be run again.
//!
//! Step can either being synchronous or asynchronous. Task steps are defined using the
//! [step()](context/struct.TaskContext.html#method.step) and
//! [async_step()](context/struct.TaskContext.html#method.async_step).
//!
//! ```rust
//! use taskturbine_core::app::ResultData;
//! use taskturbine_core::context::{FlowControl, TaskContext};
//!
//! #[derive(Debug)]
//! struct Error(String);
//!
//! pub async fn do_some_task(mut ctx: TaskContext) -> Result<Option<ResultData>, FlowControl> {
//!     // Define a sync step. `step_result` will contain the bytes returned by the step fn.
//!     let prepared_bytes = ctx.step(
//!         "prepare-data",
//!         |ctx: TaskContext| -> Result<ResultData, Error> {
//!             todo!();
//!         }
//!     ).await?;
//!
//!     // Define an async step
//!     let email_results = ctx.async_step(
//!         "send-results",
//!         async |ctx: TaskContext| -> Result<ResultData, Error> {
//!             todo!();
//!         }
//!     ).await?;
//!
//!     Ok(None)
//! }
//! ```
//!
//! ## Spawning Tasks
//!
//! Tasks can be spawned using either
//! [TaskturbineApp.spawn_task()](app/struct.TaskturbineApp.html#method.spawn_task) or
//! [TaskContext.spawn_task()](context/struct.TaskContext.html#method.spawn_task). Tasks
//! take their parameters as a bytestring, and encoding/decoding parameter payloads is
//! an application concern.
//!
//! ### Making tasks idempotent
//!
//! In scenarios where you want to prevent duplicate tasks from being spawned you can
//! use `TaskOptions.idempotency_key` to provide a unique value that is combined with the task
//! name to prevent duplicate tasks being spawned. When a duplicate task creation is attempted
//! the result will be a `StorageError::DuplicateSpawn` containing the id of the previously created
//! task.
//!
//! # Events
//!
//! Tasks can wait for an event to happen outside of a task. Your application logic can _emit
//! events_ as they happen. When an event is emit, any task/run that has a `wait` registered, will
//! be woken up and made pending for execution again. This provides a simple sychronization
//! tool that lets you have tasks wait for events like webhooks, or other tasks to complete.
//!
//! Use [TaskContext.await_event](context/struct.TaskContext.html#method.await_event) to await events,
//! and [TaskturbineApp.emit_event](app/struct.TaskturbineApp.html#method.emit_event) or
//! [TaskContext.emit_event](context/struct.TaskContext.html#method.emit_event) to emit events.
//!
//! # Running workers
//!
//! Workers claim and execute tasks from one or more channels. While taskturbine gives you
//! the building blocks for a worker, you do need to put them together in your application
//! to create a worker binary. A simple worker can look like
//!
//! ```rust
//! use std::env;
//! use taskturbine_core::app::{TaskturbineApp, ResultData, run_worker};
//! use taskturbine_core::config::Config;
//! use taskturbine_core::context::{FlowControl, TaskContext};
//!
//! async fn send_mail(ctx: TaskContext) -> Result<Option<ResultData>, FlowControl> {
//!     Ok(None)
//! }
//!
//! async fn register_user(ctx: TaskContext) -> Result<Option<ResultData>, FlowControl> {
//!     Ok(None)
//! }
//!
//! // Create a Task application.
//! // Having a factory method for the task application will make
//! // it easier to spawn tasks and emit events from other parts of your application.
//! pub fn make_task_app() -> TaskturbineApp {
//!     let database_url = env::var("DATABASE_URL").expect("Missing DATABASE_URL in env");
//!     let task_config = Config {
//!         database_url,
//!         ..Config::default()
//!     };
//!
//!     TaskturbineApp::new(task_config)
//!         .add_channel("email")
//!         // Task functions can be imported from modules.
//!         .register_task("send_mail", send_mail)
//!         .register_task("register-user", register_user)
//! }
//!
//! // Entry point for the worker.
//! async fn worker_main() {
//!     log::info!("Starting worker");
//!     let app = make_task_app();
//!
//!     // Each worker instance should have a different worker_id
//!     let worker = app.create_worker("worker-1", vec![]);
//!     run_worker(worker).await;
//! }
//! ```
//!
//! # Performing upkeep
//!
//! Workers can timeout, get OOMkilled and be restarted mid task. When this happens
//! the tasks they previously claimed need to be released. By periodically running
//! upkeep operations, expired claims are released, and tasks that are past their
//! `cancellation_max_age` can be cancelled. Upkeep operations are done
//! within processing workers by default.
//!
//! You can tune how often upkeep operations are done by workers using
//!
//! - [Config.worker_upkeep_interval_secs](config/struct.Config.html#structfield.worker_upkeep_interval_secs)
//! - [Config.worker_upkeep_inline](config/struct.Config.html#structfield.worker_upkeep_inline)
//!
//! # Upkeep workers
//!
//! If you have larger numbers of workers, the upkeep operations of those workers can create
//! contention and consume cycles from executing tasks. In larger deployments it can be more efficient
//! to have dedicated upkeep worker to reduce contention:
//!
//! ```rust
//! use taskturbine_core::app::{TaskturbineApp, run_upkeep_worker};
//! use taskturbine_core::config::Config;
//!
//! async fn worker_main() {
//!     let config = Config::default();
//!     let app = TaskturbineApp::new(config);
//!
//!     let worker = app.create_worker("upkeep-worker-1", vec![]);
//!     run_upkeep_worker(worker).await;
//! }
//! ```
//!
//! When running a dedicated worker you may need to tune your configuration if you were previously
//! running inline upkeep operations on many workers.
//!
pub mod app;
pub mod config;
pub mod context;
pub mod models;
pub mod storage;
