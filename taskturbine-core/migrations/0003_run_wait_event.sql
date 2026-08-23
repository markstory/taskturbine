ALTER TABLE taskturbine.runs ADD COLUMN wait_event_name TEXT;
ALTER TABLE taskturbine.runs ADD COLUMN wait_timeout TIMESTAMPTZ;
CREATE INDEX runs_wait_event ON taskturbine.runs (wait_event_name);
