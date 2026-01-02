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
//!    database can be shared by multiple applications if required.
//! - `channel` Channels enable you to separate workloads within a `usecase`. For example, you may
//!    want many workers processing high-priority tasks, and fewer processing lower priority work.
//! - `task` A workflow or task that should be executed durably.
//! - `step` An incremental operation or side-effect that can succeed or fail. Steps
//!    act as error and persistence boundaries for your tasks. Steps that complete are not
//!    retried or run multiple times.
//! - `checkpoint` As steps are completed, checkpoints are created.
//! - `run` An attempt to execute a task. Each run can read checkpoints from previous
//!    runs, allowing tasks to resume where they left off.
//! - `event` Tasks can be suspended until named events are emit by the application. Events are
//!    ideal for waiting on webhooks, or other tasks to complete.
//! - `wait` When a task is waiting for an event, it records a `wait`.
//!
//! # Defining a Task
//!
//! # Events
//!
//! Tasks can wait for an event to happen outside of a task. Your application logic can _emit
//! events_ as they happen. When an event is emit, any event that has a `wait` registered
//! Events allow you to have tasks wait for events like webhooks, or other tasks.
//!
//! Use [TaskContext.await_event](context/struct.TaskContext.html#method.await_event) to await events,
//! and [TaskturbineApp.emit_event](app/struct.TaskturbineApp.html#method.emit_event) or
//! [TaskContext.emit_event](context/struct.TaskContext.html#method.emit_event) to emit events.
//!
//! # Running workers
//!
//! # Performing cleanup
//!
pub mod app;
pub mod config;
pub mod context;
pub mod models;
pub mod storage;
