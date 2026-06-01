CREATE TABLE taskturbine.scheduler_state (
    usecase text NOT NULL,
    schedule_id VARCHAR NOT NULL,
    last_run TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (usecase, schedule_id)
);
