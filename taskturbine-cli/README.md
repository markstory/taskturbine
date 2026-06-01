# Taskturbine CLI

This crate provides CLI tools for interacting with a taskturbine application's database. With the CLI tools you can:

- Spawn tasks.
- Cancel tasks.
- Emit events.
- List tasks and runs
- View individual tasks and runs.
- Run a upkeep worker.
- Clear all stored tasks + events.
- Create schema and run schema migrations.
- Run tasks on a schedule

## Scheduler

Using `taskturbine-cli scheduler` will let you periodically spawn tasks based on
schedules defined in a configuration file. Think of it like crontab for your
application's tasks:

### Scheduler Configuration File

```
[schedules]

[schedules.send-digests]
taskname = "myapp-notification-send-digests"
channel = "notifications"
schedule = {cron = "*/5 * * * *" }
params = '{"enable_experiement": true}'

[schedules.process-commits]
taskname = "myapp-commits-process"
channel = "commits"
schedule =  { timedelta = { minutes = 5 } }
```

The `schedules` table contains a list of schedule keys, the task, channel and
schedule to use. Each schedule can use either a crontab or timedelta expression
schedule. Fixed parameter payloads can be provided to tasks if required. By
default tasks are spawned without any parameters or options.
