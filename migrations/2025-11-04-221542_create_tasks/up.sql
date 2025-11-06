CREATE TABLE tasks (
  id SERIAL NOT NULL PRIMARY KEY,
  -- We could and preferably would use a custom enum here but this is much simpler with diesel.
  task_type INTEGER NOT NULL,
  state INTEGER NOT NULL DEFAULT 1, -- Default of "scheduled"
  scheduled_at TIMESTAMPTZ NOT NULL,
  shard SMALLINT NOT NULL DEFAULT floor(random() * 256) CHECK (shard >= 0 AND shard < 256)
);

CREATE INDEX sharding on tasks (shard);
