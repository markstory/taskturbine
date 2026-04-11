use taskturbine_core::models::{Run, Task};

pub const INVALID_DATA: &str = "<non-utf8 data>";

pub fn dump_run(run: &Run, show_results: bool) {
    println!("Run Id: {}", run.run_id);
    println!(" attempt: {}", run.attempt);
    println!(" state: {}", run.state);
    println!(" claimed_by: {}", run.claimed_by);
    println!(" created at: {}", run.created_at);
    if show_results && let Some(result) = &run.result {
        println!(
            " payload: {}",
            str::from_utf8(result.as_slice()).unwrap_or(INVALID_DATA)
        );
    }
}

pub fn dump_task(task: &Task) {
    println!("Task Id: {}", task.task_id);
    println!("  channel:    {}", task.channel);
    println!("  task_name:  {}", task.task_name);
    println!("  state:      {}", task.state);
    println!(
        "  headers:    {}",
        str::from_utf8(&task.headers).unwrap_or(INVALID_DATA)
    );
    println!(
        "  parameters: {}",
        str::from_utf8(&task.params).unwrap_or(INVALID_DATA)
    );
    println!(" Retry:");
    println!("  seconds:      {}", &task.retry_seconds);
    println!("  factor:       {}", &task.retry_factor);
    println!("  max_seconds:  {}", &task.retry_max_seconds);
    println!("  attempts:     {}", &task.attempts);
    println!("  max_attempts: {}", &task.max_attempts);
    println!(" cancellation_max_age:  {}", &task.cancellation_max_age);
}
