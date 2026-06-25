# Taskturbine

This packages contains the rust client for taskturbine, a cross-platform durable task
framework.

## What are durable tasks?

Durable tasks are operations that are resilient to failure and interruptions. Instead of having
to manually manage retries, state and scheduling, you express your logic as a workflow of 
operations or functions. Each 'step' in a durable task will store its result, and retries will
resume from the last completed step.

See the [project homepage](https://github.com/markstory/taskturbine) for more background documentation.

## Installation

```
cargo add taskturbine
```
## Defining Tasks

- Setup minimal config : db url
- Setup app, and register first task.
- Show multi step task

## Spawning tasks

- Use app to spawn tasks.
- Use context to spawn tasks

## Running tasks

- Running a worker
- Running an upkeep worker
