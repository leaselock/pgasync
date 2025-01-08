/* Install me first!! first: Client library for async libary */

\if :bootstrap

CREATE EXTENSION IF NOT EXISTS dblink;
CREATE SCHEMA IF NOT EXISTS async;

CREATE TABLE async.client_control
(
  client_only BOOL DEFAULT TRUE,
  connection_string TEXT DEFAULT ''
);

CREATE UNIQUE INDEX ON async.client_control((1));
INSERT INTO async.client_control DEFAULT VALUES;

DO
$$
BEGIN
  PERFORM 1 FROM async.control;
EXCEPTION WHEN undefined_table THEN
  UPDATE async.client_control SET client_only = true;
END;
$$;

CREATE TYPE async.finish_status_t AS ENUM(
  'FINISHED',  /* all systems go! */
  'FAILED', /* task returned with an error */
  'CANCELED', /* task canceled by something outside of normal processing */
  'PAUSED', /* will cancel task, but reset for processing */
  'TIMED_OUT', /* ran out of time */
  'YIELDED', /* pending asynchronous task */
  'DOA'); /* dead on arrival, basically, a NOP */


/* Defines task so that it can be pushed */
CREATE TYPE async.task_push_t AS
(
  task_data JSONB,
  target TEXT,
  priority INT,
  query TEXT,
  concurrency_pool TEXT
);


CREATE TYPE async.task_run_type_t AS ENUM
(
  'EXECUTE', /* task run as normal */
  'DOA', /* do not run and presume failed */
  'EMPTY', /* do not run and presume success */
  'EXECUTE_NOASYNC' /* run and disable the task asynchronous flag */
);

\endif

/* getter/setter to fetch server id or update it if passed. */
CREATE OR REPLACE FUNCTION async.server(
  _new_connection_string INOUT TEXT DEFAULT NULL) RETURNS TEXT AS
$$
BEGIN
  INSERT INTO async.client_control DEFAULT VALUES ON CONFLICT DO NOTHING;
  
  IF _new_connection_string IS NOT NULL
  THEN
    UPDATE async.client_control SET connection_string = _new_connection_string;
  END IF;

  SELECT INTO _new_connection_string connection_string 
  FROM async.client_control;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION async.configure(
  _configuration JSONB) RETURNS VOID AS
$$
BEGIN
  INSERT INTO async.control DEFAULT VALUES ON CONFLICT DO NOTHING;

  UPDATE async.control c SET
    workers = COALESCE(t.workers, c.workers),
    idle_sleep = COALESCE(t.idle_sleep, c.idle_sleep),
    heavy_maintenance_sleep = COALESCE(t.heavy_maintenance_sleep, c.heavy_maintenance_sleep),
    task_keep_duration = COALESCE(t.task_keep_duration, c.task_keep_duration),
    default_timeout = COALESCE(t.default_timeout, c.default_timeout),
    self_target = COALESCE(t.self_target, c.self_target),
    self_connection_string = COALESCE(t.self_connection_string, c.self_connection_string),
    self_concurrency = COALESCE(t.self_concurrency, c.self_concurrency),
    light_maintenance_sleep = COALESCE(t.light_maintenance_sleep, c.light_maintenance_sleep),
    default_concurrency_pool_workers = COALESCE(t.default_concurrency_pool_workers, c.default_concurrency_pool_workers)
  FROM 
  (
    SELECT * FROM jsonb_populate_record(
      null::async.control,
      _configuration->'control')
  ) t;

  CREATE TEMP TABLE tmp_target (LIKE async.target) ON COMMIT DROP;
  INSERT INTO tmp_target SELECT * FROM jsonb_populate_recordset(
    null::tmp_target,
    _configuration->'targets');

  /* forge target for async maintenance */
  INSERT INTO tmp_target SELECT
    self_target,
    self_concurrency,
    self_connection_string,
    false
  FROM async.control;

  INSERT INTO async.target SELECT * FROM tmp_target 
    ON CONFLICT ON CONSTRAINT target_pkey DO UPDATE SET
       max_concurrency = COALESCE(excluded.max_concurrency, target.max_concurrency),
       connection_string = COALESCE(excluded.connection_string, target.connection_string),
       asynchronous_finish = COALESCE(excluded.asynchronous_finish, target.asynchronous_finish),
       default_timeout = COALESCE(excluded.default_timeout, target.default_timeout);

  DELETE FROM async.worker WHERE target NOT IN (SELECT target FROM tmp_target);
  DELETE FROM async.target WHERE target NOT IN (SELECT target FROM tmp_target);

  CREATE TEMP TABLE tmp_concurrency_pool(LIKE async.concurrency_pool);

  INSERT INTO tmp_concurrency_pool SELECT * FROM jsonb_populate_recordset(
    null::tmp_concurrency_pool,
    _configuration->'concurrency_pools');  

  INSERT INTO async.concurrency_pool(concurrency_pool)
  SELECT t.concurrency_pool FROM tmp_concurrency_pool t
  WHERE t.concurrency_pool NOT IN (
    SELECT concurrency_pool FROM async.concurrency_pool);  
  
  UPDATE async.concurrency_pool p SET
    max_workers = COALESCE(t.max_workers, p.max_workers)
  FROM tmp_concurrency_pool t
  WHERE t.concurrency_pool = p.concurrency_pool;

  DELETE FROM async.concurrency_pool WHERE concurrency_pool NOT IN(
    SELECT t.concurrency_pool FROM tmp_concurrency_pool t);

  DROP TABLE tmp_concurrency_pool;

  INSERT INTO async.concurrency_pool SELECT 
    target,
    max_concurrency
  FROM async.target 
    ON CONFLICT ON CONSTRAINT concurrency_pool_pkey DO UPDATE SET
      max_workers = EXCLUDED.max_workers;
END;
$$ LANGUAGE PLPGSQL;




/* 
 * Routine to finish tasks.  Unlike most server API routines, this is designed
 * to be called from inside or outside the server process.
 */
CREATE OR REPLACE FUNCTION async.finish(
  _task_ids BIGINT[],
  _status async.finish_status_t,
  _error_message TEXT) RETURNS VOID AS
$$
DECLARE
  _processed TIMESTAMPTZ;
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format(
        'SELECT 0 FROM async.finish(%s, %s, %s)',
        quote_literal($1),
        quote_literal($2),
        quote_nullable($3))) AS R(V INT);

    RETURN;
  END IF; 

  _task_ids := async.check_task_ids(_task_ids, 'finish_async');

  /* is this a foreground request? If so, convert to task */
  IF pg_backend_pid() = (SELECT pid FROM async.control) 
  THEN
    PERFORM async.finish_internal(
      _task_ids, 
      _status, 
      'finish_async',
      _error_message);  
  ELSE
    PERFORM async.push_tasks(
      array[(
        NULL,
        self_target,
        -1,
        format(
          'SELECT async.finish_internal(%s, %s::async.finish_status_t, %s, %s)',
          quote_literal(_task_ids),
          quote_literal(_status),
          quote_literal('finish_async'),
          quote_nullable(_error_message)),
        NULL
      )::async.task_push_t],
      _source := 'finish_async')
    FROM async.control; 
  END IF;
END;    
$$ LANGUAGE PLPGSQL;


/* wrapper to finish to cancel tasks */
CREATE OR REPLACE FUNCTION async.cancel(
  _task_ids BIGINT[],
  _error_message TEXT DEFAULT 'manual cancel') RETURNS VOID AS
$$
  SELECT async.finish($1, 'CANCELED', _error_message);
$$ LANGUAGE SQL;


/* helper function to build task_push_t with arguments defaulted */
CREATE OR REPLACE FUNCTION async.task(
  _query TEXT,
  target TEXT,
  task_data JSONB DEFAULT NULL,
  priority INT DEFAULT 0,
  concurrency_pool TEXT DEFAULT NULL) RETURNS async.task_push_t AS
$$
  SELECT ($3, $2, $4, $1, $5)::async.task_push_t;
$$ LANGUAGE SQL IMMUTABLE;


/* add tasks to the processing queue.  tasks can be added DOA to preserve 
 * history and to support external trigger based processing, replication, etc.
 *
 * can be called directly but only from the orchestration server itself.
 * If on client or unsure if on server, use the push_tasks() wrapper.
 */
CREATE OR REPLACE FUNCTION async.push_tasks(
  _tasks async.task_push_t[],
  _run_type async.task_run_type_t DEFAULT 'EXECUTE',
  _source TEXT DEFAULT NULL) RETURNS SETOF BIGINT AS
$$
DECLARE 
  _debug BOOL DEFAULT false;
  _tasks_out BIGINT[];
  _pools_out TEXT[];
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    RETURN QUERY SELECT * FROM dblink(
      async.server(), 
      format(
        'SELECT * FROM async.push_tasks(%s, %s, %s)',
        quote_literal($1),
        quote_literal($2),
        quote_nullable($3))) AS R(V BIGINT);

    RETURN;
  END IF; 

  IF _debug
  THEN
    RAISE NOTICE 'Received tasks % status % source %',
      to_jsonb(_tasks),
      _run_type,
      _source;
  END IF;
  
  WITH data AS  
  (
    INSERT INTO async.task(
      task_data, 
      target, 
      priority, 
      query, 
      asynchronous_finish,
      failed,
      finish_status,
      consumed,
      processed,
      source,
      concurrency_pool)
    SELECT 
      q.task_data,
      q.target,
      q.priority,
      q.query,
      t.asynchronous_finish AND _run_type NOT IN('EXECUTE_NOASYNC', 'EMPTY'),
      CASE 
        WHEN _run_type = 'DOA' THEN true
        WHEN _run_type = 'EMPTY' THEN false 
      END,
      CASE 
        WHEN _run_type = 'DOA' THEN 'DOA'
        WHEN _run_type = 'EMPTY' THEN 'FINISHED' 
      END::async.finish_status_t,
      CASE WHEN _run_type IN('DOA', 'EMPTY') THEN clock_timestamp() END,
      CASE WHEN _run_type IN('DOA', 'EMPTY') THEN clock_timestamp() END,
      _source,
      COALESCE(q.concurrency_pool, t.target)
    FROM 
    (
      SELECT * 
      FROM unnest(_tasks)
    ) q
    JOIN async.target t USING(target)
    RETURNING *
  ) 
  SELECT INTO 
    _tasks_out, 
    _pools_out
    array_agg(d.task_id),
    array_agg(DISTINCT concurrency_pool) FILTER (WHERE processed IS NULL)
  FROM data d;

  PERFORM async.set_concurrency_pool_tracker(_pools_out);

  IF _debug
  THEN
    RAISE NOTICE 'after tasks %', to_jsonb(_tasks_out);
  END IF;

  RETURN QUERY SELECT * FROM unnest(_tasks_out);
  
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION async.push_task(
  _task async.task_push_t,
  _run_type async.task_run_type_t DEFAULT 'EXECUTE',
  _source TEXT DEFAULT 'manual push') RETURNS BIGINT AS
$$
  SELECT * FROM async.push_tasks(array[_task], _run_type, _source);
$$ LANGUAGE SQL;


