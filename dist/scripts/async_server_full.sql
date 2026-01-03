
DO
$bootstrap$
BEGIN

PERFORM 1 FROM pg_extension WHERE extname = 'dblink';

IF NOT FOUND 
THEN 
  RAISE EXCEPTION 'dblink extension must be installed before installing async';
END IF;

DO
$$
BEGIN
  PERFORM 1 FROM async.client_control;
  RETURN;
EXCEPTION WHEN undefined_table THEN NULL;
END;
$$;

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
  concurrency_pool TEXT,
  manual_timeout INTERVAL,
  track_yielded BOOL
);


CREATE TYPE async.task_run_type_t AS ENUM
(
  'EXECUTE', /* task run as normal */
  'DOA', /* do not run and presume failed */
  'EMPTY', /* do not run and presume success */
  'EXECUTE_NOASYNC' /* run and disable the task asynchronous flag */
);

END;
$bootstrap$;


DO
$code$
BEGIN

BEGIN
  PERFORM 1 FROM async.client_control LIMIT 0;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Async client library incorrectly installed';
  RETURN;
END;

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
       default_timeout = COALESCE(excluded.default_timeout, target.default_timeout),
       concurrency_track_yielded = COALESCE(excluded.concurrency_track_yielded, target.concurrency_track_yielded);

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
  _internal_priority INT DEFAULT -99;
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

  /* is this a foreground request? If so, convert to task */
  IF pg_backend_pid() = (SELECT pid FROM async.control) 
  THEN
    PERFORM async.finish_internal(
      _task_ids, 
      _status, 
      'async.finish',
      _error_message);  
  ELSE
    PERFORM async.push_tasks(
      array[(
        jsonb_build_object(
          'task_ids', _task_ids,
          'status', _status,
          'error_message', _error_message
        ),
        self_target,
        _internal_priority,
        NULL,
        NULL,
        NULL,
        NULL
      )::async.task_push_t],
      _source := 'async.finish')
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
  concurrency_pool TEXT DEFAULT NULL,
  manual_timeout INTERVAL DEFAULT NULL,
  track_yielded BOOL DEFAULT NULL) RETURNS async.task_push_t AS
$$
  SELECT ($3, $2, $4, $1, $5, $6, $7)::async.task_push_t;
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
      concurrency_pool,
      manual_timeout,
      track_yielded)
    SELECT 
      q.task_data,
      q.target,
      COALESCE(q.priority, 0),
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
      COALESCE(q.concurrency_pool, t.target),
      q.manual_timeout,
      COALESCE(q.track_yielded, t.concurrency_track_yielded, true)
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


CREATE OR REPLACE FUNCTION async.wait_for_latch() RETURNS VOID AS
$$
DECLARE
  _request_latch_id BIGINT;
  _timeout INTERVAL DEFAULT '5 minutes';
  _ready TIMESTAMPTZ;
  _start_time TIMESTAMPTZ;
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      'SELECT 0 FROM async.wait_for_latch()') AS R(V INT);

    RETURN;
  END IF; 

  _start_time := clock_timestamp();

  SELECT INTO _request_latch_id v
  FROM dblink(
    (SELECT self_connection_string FROM async.control), 
    format(
      'INSERT INTO async.request_latch DEFAULT VALUES '
      'RETURNING request_latch_id')) AS R(V BIGINT);

  LOOP
    SELECT ready INTO _ready
    FROM async.request_latch WHERE request_latch_id = _request_latch_id;

    IF _ready IS NOT NULL 
    THEN 
      EXIT;
    END IF;

    IF clock_timestamp() - _start_time > _timeout
    THEN 
      EXIT;
    END IF;

    PERFORM pg_sleep(.000001);
  END LOOP;

  IF _ready IS NULL
  THEN
    RAISE EXCEPTION 'Unable to obtain client latch for latch id %',
      _request_latch_id;
  END IF;
END;  
$$ LANGUAGE PLPGSQL;


/* Gets tasks with no routine.  When there is no query to call, the presumption 
 * is that the client service is managing invocation and possibly threading. 
 * However, concurrency pool limits remain enforced.  
 *
 * Each task returned must be marked complete before the timeout or it will be 
 * assumed failed.
 *
 * Getting tasks in this way is only possible for for asynchronous targets.  
 * async.get_tasks can be called from multiple threads but not for the same 
 * target.  If concurrent execution is needed, it is the client's responsibility
 * to dispatch work to multiple threads locally and ensure that
 * the finish function is ultimately called.
 */
CREATE OR REPLACE FUNCTION async.get_tasks(
  _target TEXT,
  _limit INT DEFAULT 1,
  _timeout INTERVAL DEFAULT '30 seconds',
  task_id OUT BIGINT,
  priority OUT INT,
  times_up OUT TIMESTAMPTZ,
  task_data OUT JSONB) RETURNS SETOF RECORD AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    RETURN QUERY SELECT * FROM dblink(
      async.server(), 
      format(
        'SELECT * FROM async.get_tasks(%s, %s)',
        quote_literal($1),
        quote_literal($2))) AS R(
          task_id BIGINT,
          priorty INT,
          times_up TIMESTAMPTZ,
          task_data JSONB);

    RETURN;
  END IF;

  RETURN QUERY SELECT * FROM async.get_tasks_internal(_target, _limit);
END;
$$ LANGUAGE PLPGSQL;

END;
$code$;/* Standard routines and structures for asynchronous processing.   Intended
 * to be standalone module.  More or less a glorified wrapper to dblink(), 
 * but facilitates background query processing to arbitrary targets.
 * 
 */

DO
$bootstrap$
BEGIN

BEGIN
  UPDATE async.client_control SET client_only = false;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Please install client library first';
END;

BEGIN
  PERFORM 1 FROM async.control LIMIT 0;
  RETURN;
EXCEPTION WHEN undefined_table THEN NULL;
END;


CREATE TABLE async.control
(
  enabled BOOL DEFAULT true,
  workers INT DEFAULT 20,
  idle_sleep FLOAT8 DEFAULT 1.0,
  heavy_maintenance_sleep INTERVAL DEFAULT '24 hours'::INTERVAL,
  task_keep_duration INTERVAL DEFAULT '30 days'::INTERVAL,
  default_timeout INTERVAL DEFAULT '1 hour'::INTERVAL,
  advisory_mutex_id INT DEFAULT 0,

  self_target TEXT DEFAULT 'async.self', 
  self_connection_string TEXT DEFAULT 'host=localhost',
  self_concurrency INT DEFAULT 4,

  last_heavy_maintenance TIMESTAMPTZ,
  running_since TIMESTAMPTZ,
  paused BOOL NOT NULL DEFAULT false,
  pid INT, /* pid of main background process */
  busy_sleep FLOAT8 DEFAULT 0.1,

  light_maintenance_sleep INTERVAL DEFAULT '5 minutes'::INTERVAL,
  last_light_maintenance TIMESTAMPTZ,

  default_concurrency_pool_workers INT DEFAULT 4,

  /* called upon async start up */
  startup_routines TEXT[],

  /* called upon on async loop */
  loop_routines TEXT[]

);

CREATE UNIQUE INDEX ON async.control((1));
INSERT INTO async.control DEFAULT VALUES;

CREATE TABLE async.target
(
  target TEXT PRIMARY KEY,
  max_concurrency INT DEFAULT 8,
  connection_string TEXT,
  asynchronous_finish BOOL DEFAULT false,
  default_timeout INTERVAL,
  /* if true, yielded threads will not release concurrency slot */
  concurrency_track_yielded BOOL DEFAULT false 
);


 
CREATE TABLE async.task
(
  task_id BIGSERIAL PRIMARY KEY,
  target TEXT REFERENCES async.target ON UPDATE CASCADE ON DELETE CASCADE,
  priority INT DEFAULT 0,
  entered TIMESTAMPTZ DEFAULT clock_timestamp(), 
  
  consumed TIMESTAMPTZ, 
  processed TIMESTAMPTZ, 
  
  /* for 'asynchronous finish' tasks, the time submitting query resolved */
  yielded TIMESTAMPTZ,
  finish_status async.finish_status_t,
  source TEXT,
  
  query TEXT,
  failed BOOL,
  processing_error TEXT,
  
  /* if true, task is only considered processed via external processing */
  asynchronous_finish BOOL,


  /* tasks not finished at this time will cancel */
  times_up TIMESTAMPTZ,
  task_data JSONB,  /* opaque to hold arbitrary data */

  concurrency_pool TEXT,

  manual_timeout INTERVAL,

  /* this task is counted for purposes of determining how many threads are 
   * running in a concurrency pool
   */
  tracked BOOL,

  track_yielded BOOL
);

/* supports fetching eligible tasks */
CREATE INDEX ON async.task(priority, entered) 
WHERE 
  consumed IS NULL
  AND yielded IS NULL
  AND processed IS NULL;

CREATE INDEX ON async.task(concurrency_pool, priority, entered) 
WHERE 
  consumed IS NULL
  AND yielded IS NULL
  AND processed IS NULL;  

CREATE INDEX ON async.task(times_up)
  WHERE processed IS NULL; 
    


CREATE UNLOGGED TABLE async.worker
(
  slot INT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,

  target TEXT REFERENCES async.target,
  task_id INT,
  running_since TIMESTAMPTZ
);

/* initialized from configuration */
CREATE TABLE async.concurrency_pool
(
  concurrency_pool TEXT PRIMARY KEY,
  max_workers INT DEFAULT 8
);

CREATE UNLOGGED TABLE async.concurrency_pool_tracker
(
  concurrency_pool TEXT PRIMARY KEY,
  workers INT,
  max_workers INT DEFAULT 8
);

CREATE INDEX ON async.concurrency_pool_tracker(
  concurrency_pool, workers, max_workers)
WHERE workers < max_workers;

CREATE TABLE async.server_log
(
  server_log BIGSERIAL PRIMARY KEY,
  happened TIMESTAMPTZ DEFAULT clock_timestamp(),
  level TEXT,
  message TEXT
);

CREATE INDEX ON async.server_log(happened);



CREATE UNLOGGED TABLE async.request_latch
(
  request_latch_id BIGSERIAL PRIMARY KEY,
  ready TIMESTAMPTZ
);

/* for deletions */
CREATE INDEX ON async.request_latch((1)) WHERE ready IS NOT NULL;


CREATE TYPE async.routine_type_t AS ENUM
(
  'STARTUP',
  'LOOP'
);

END;
$bootstrap$;


DO
$code$
BEGIN

BEGIN
  PERFORM 1 FROM async.control LIMIT 0;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Async server library incorrectly installed';
  RETURN;
END;

CREATE OR REPLACE FUNCTION async.log(
  _level TEXT,
  _message TEXT) RETURNS VOID AS
$$
BEGIN
  IF _level = 'NOTICE'
  THEN 
    RAISE NOTICE '%', _message;
  ELSEIF _level = 'WARNING'
  THEN
    RAISE WARNING '%', _message;
   ELSEIF _level = 'DEBUG'
  THEN
    RAISE WARNING '%', _message;
  ELSE
    PERFORM dblink_exec(
      (SELECT self_connection_string FROM async.control),
      format(
        'INSERT INTO async.server_log(level, message) VALUES (%s, %s)',
        quote_literal(_level),
        quote_literal(_message)));

    RAISE EXCEPTION '%', _message;
  END IF;

  INSERT INTO async.server_log(level, message) VALUES (_level, _message);
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION async.log(
  _message TEXT) RETURNS VOID AS
$$
BEGIN
  PERFORM async.log('NOTICE', _message);
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE VIEW async.v_target AS 
  SELECT 
    t.*,
    COUNT(task_id) AS active_workers
  FROM async.target t
  LEFT JOIN async.worker USING(target)
  GROUP BY target;


CREATE OR REPLACE VIEW async.v_target_status AS
  SELECT 
    target,
    COUNT(*) AS tasks_entered_24h,
    COUNT(*) FILTER(WHERE entered >= now() - '1 hour'::INTERVAL) 
      AS tasks_entered_1h,
    COUNT(*) FILTER(WHERE entered >= now() - '1 minute'::INTERVAL) 
      AS tasks_entered_1m,    
    COUNT(*) FILTER (WHERE consumed IS NULL AND processed IS NULL) AS tasks_pending,
    COUNT(*) FILTER (WHERE processed IS NULL AND yielded IS NULL) AS tasks_running,
    COUNT(*) FILTER (WHERE processed IS NULL AND yielded IS NOT NULL) AS tasks_yielded,
    COALESCE(cpt.workers, 0) AS concurency_assigned_workers,
    COUNT(*) FILTER (
        WHERE 
          consumed IS NOT NULL 
          AND processed IS NULL
          AND t.concurrency_pool IS DISTINCT FROM t.target) 
      AS concurrency_external_pool,
    COALESCE(cp.max_workers, target.max_concurrency) AS concurency_max_workers
  FROM async.target
  LEFT JOIN async.task t USING(target)
  LEFT JOIN async.concurrency_pool cp ON 
    target.target = cp.concurrency_pool
  LEFT JOIN async.concurrency_pool_tracker cpt ON 
    cp.concurrency_pool = cpt.concurrency_pool
  WHERE entered >= now() - '1 day'::INTERVAL
  GROUP BY target, cp.concurrency_pool, cpt.concurrency_pool
  ORDER BY target;


/* set up the table that manages tracking of threads in flight. Also set
 * up the concurrency pool tracker.   There is no requirement for pools to be
 * explicitly configured, but if they are, they can be set up here.
 */
CREATE OR REPLACE FUNCTION async.initialize_workers(
  g async.control,
  _startup BOOL DEFAULT false) RETURNS VOID AS
$$
BEGIN
  /* leaving open for hot worker recalibration */
  IF _startup
  THEN
    PERFORM async.log('Force disconnecting existing workers');
    PERFORM async.disconnect(n, 'worker initialize')
    FROM 
    (
      SELECT unnest(dblink_get_connections()) n
    ) q
    WHERE n LIKE 'async.worker%';

    DELETE FROM async.worker;
  END IF;

  PERFORM async.log('Initializing concurrency pools');
  DELETE FROM async.concurrency_pool_tracker;

  PERFORM async.set_concurrency_pool_tracker(array_agg(concurrency_pool))
  FROM async.task
  WHERE processed IS NULL;

  INSERT INTO async.worker SELECT 
    s,
    'async.worker_' || s
  FROM generate_series(1, g.workers) s;

  PERFORM async.log('Worker initialization complete');
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION async.active_workers() RETURNS BIGINT AS
$$
  SELECT count(*) 
  FROM async.worker
  WHERE task_id IS NOT NULL;
$$ LANGUAGE SQL STABLE;


CREATE OR REPLACE FUNCTION async.candidate_tasks() RETURNS SETOF async.task AS
$$
DECLARE
  r RECORD;
BEGIN
  FOR r IN 
    SELECT *
    FROM async.v_target 
    WHERE max_concurrency - active_workers > 0
  LOOP
    RETURN QUERY SELECT * 
    FROM async.task t
    WHERE 
      consumed IS NULL
      AND processed IS NULL
      AND t.yielded IS NULL
      AND t.target = r.target
    ORDER BY t.priority, t.entered
    LIMIT r.max_concurrency - r.active_workers;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE VIEW async.v_candidate_task AS 
  SELECT * FROM
  (
    SELECT 
      t.*,
      tg.connection_string,
      tg.default_timeout,
      pt.max_workers,
      pt.workers,
      0::BIGINT AS n
    FROM async.concurrency_pool_tracker pt
    CROSS JOIN async.control
    CROSS JOIN LATERAL (
      SELECT * FROM async.task t
      WHERE
        pt.concurrency_pool = t.concurrency_pool
        AND t.consumed IS NULL
        AND t.processed IS NULL
        AND t.yielded IS NULL
      ORDER BY priority, entered
      LIMIT pt.max_workers - pt.workers
    ) t 
    JOIN async.target tg USING(target)
    WHERE 
      pt.workers < pt.max_workers 
      AND t.target != self_target
      AND pt.concurrency_pool != self_target
  ) 
  ORDER BY priority, entered
  LIMIT (SELECT g.workers - async.active_workers() FROM async.control g);




CREATE OR REPLACE FUNCTION async.disconnect(
  _name TEXT,
  _reason TEXT) RETURNS VOID AS
$$
BEGIN
  PERFORM async.log(format('Disconnecting worker %s via %s', _name, _reason));
  
  BEGIN
    PERFORM dblink_disconnect(_name);
  EXCEPTION WHEN OTHERS THEN 
    PERFORM async.log(
      'WARNING', 
      format('Got %s during connection disconnect', SQLERRM));
  END;

  UPDATE async.worker w SET 
    target = NULL,
    task_id = NULL,
    running_since = NULL
  WHERE w.name = _name;
END;
$$ LANGUAGE PLPGSQL;


/* dblink obnoxiously requires the result structure to be specified when the
 * result is pulled from the asynchronous query.  Since we don't know what the
 * query is is doing, or what it is returning, it is wrapped in a DO block in 
 * order to coerce the result stucture. 'EXECUTE' does not require result 
 * specfication so we use it to actually drive the query, (introducing some 
 * overhead for very large queries, but that's not a good fit for this library).
 *
 * Stored procedure invocations (CALL) are a specific and limited exception to 
 * that; only top level procedures can manage transaction state so wrapping them
 * does nothing helpful.  It's tempting to have a function that receives the 
 * query and executes it on the receiving side of the asynchronous call, 
 * something like async.receive_query(), but this presumes the asynchronous
 * library is loaded on the target which is for now not a requirement.
 */
CREATE OR REPLACE FUNCTION async.wrap_query(
  _query TEXT, 
  _task_id BIGINT) RETURNS TEXT AS
$$
  SELECT 
    CASE WHEN left(trim(_query), 4) ILIKE 'CALL'
      THEN _query
      ELSE
        format($abc$
DO
$def$
BEGIN
  /* executing asynchronous query from async for task_id %s */
  EXECUTE %s;
END;
$def$ 
$abc$,
        _task_id,
        quote_literal(_query))
      END;
$$ LANGUAGE SQL IMMUTABLE;  

/* deferred tasks are run synchronously from the orchestrator */
CREATE OR REPLACE FUNCTION async.run_deferred_task(
  _task_id BIGINT,
  _query TEXT,
  did_stuff OUT BOOL) RETURNS BOOL AS
$$
DECLARE
  _failed BOOL;
  _error_message TEXT;
  _context TEXT;
BEGIN
  UPDATE async.task SET consumed = clock_timestamp()
  WHERE task_id = _task_id;

  BEGIN 
    EXECUTE _query;
  EXCEPTION WHEN OTHERS THEN
    _failed := true;

    GET STACKED DIAGNOSTICS _context = pg_exception_context;
    _error_message := format(
      E'query: %s errored with:\n%s\ncontext: %s', 
      _query, SQLERRM, _context);
  END;

  PERFORM async.finish_internal(
    array[_task_id], 
    CASE 
      WHEN _failed THEN 'FAILED'
      ELSE 'FINISHED'
    END::async.finish_status_t,
    'async.run_deferred_task',
    _error_message);

  did_stuff := true;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION async.set_concurrency_pool_tracker(
  _pools TEXT[]) RETURNS VOID AS
$$
DECLARE
  c RECORD;
BEGIN
  SELECT INTO c * FROM async.control;

  INSERT INTO async.concurrency_pool_tracker(
    concurrency_pool,
    workers,
    max_workers)
  SELECT
    q.concurrency_pool,
    0,
    COALESCE(cp.max_workers, c.default_concurrency_pool_workers)
  FROM
  (
    SELECT unnest(_pools) AS concurrency_pool
  ) q
  LEFT JOIN async.concurrency_pool cp USING(concurrency_pool)
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE PLPGSQL;


/* returns true if at least one task was run */
CREATE OR REPLACE FUNCTION async.run_tasks(did_stuff OUT BOOL) RETURNS BOOL AS
$$
DECLARE
  r async.v_candidate_task;
  w RECORD;
  c async.control;
  _max_retry_count INT DEFAULT 5;
  _retry_counter INT DEFAULT 1;
BEGIN 
  did_stuff := false;

  SELECT INTO c * FROM async.control;

  FOR r IN SELECT * FROM async.v_candidate_task
    WHERE query IS NOT NULL
  LOOP
    SELECT INTO w 
      *,
      CASE 
        WHEN w2.target IS NULL THEN 'connect'
        WHEN r.target IS NOT DISTINCT FROM w2.target THEN 'keep'
        ELSE 'reconnect' 
      END AS connect_action
    FROM async.worker w2
    WHERE task_id IS NULL
    ORDER BY 
      CASE WHEN r.target IS NOT DISTINCT FROM w2.target 
        THEN 0
        ELSE 1
      END, 
      CASE WHEN w2.target IS NULL THEN 0 ELSE 1 END,
      slot 
    LIMIT 1;

    BEGIN
      PERFORM async.log(
        format(
          'Running task id %s pool %s slot: %s %s %s[action %s]', 
          r.task_id, 
          r.concurrency_pool,
          w.slot,
          COALESCE('data: "' || r.task_data::TEXT || '" ', ''),
          CASE WHEN r.source IS NOT NULL
            THEN format('via %s ',  r.source)
            ELSE ''
          END,
          w.connect_action));
    
      IF w.connect_action = 'keep'
      THEN
        BEGIN
          PERFORM * FROM dblink(w.name, 'SELECT 0') AS R(v INT);
        EXCEPTION WHEN OTHERS THEN
          PERFORM async.log(
            'WARNING',
            format(
              'When attempting to run task: %s in slot: %s '
              'for connection: %s, got %s', 
              r.task_id,
              w.slot,
              to_json(w),
              SQLERRM));

            w.connect_action := 'reconnect';
        END;
      END IF;

      IF w.connect_action = 'reconnect'
      THEN
        PERFORM async.disconnect(w.name, 'reconnect');
        w.connect_action = 'connect';
      END IF;

      IF w.connect_action = 'connect'
      THEN
        LOOP
          /* give it the old college try...if connection fields repeat a few 
           * times before giving up.
           * XXX: Maybe better to yield the task 
           * back with some future execution time.
           */
          BEGIN 
            PERFORM dblink_connect(w.name, r.connection_string);

            EXIT;
          EXCEPTION WHEN OTHERS THEN
            IF SQLERRM = 'could not establish connetction' 
              AND _retry_counter < _max_retry_count
            THEN
              PERFORM async.log(
                'WARNING',
                format(
                  'Retrying failed connection when runnning task %s '
                  '(attempt %s of %s)', 
                  r.task_id,
                  _retry_counter,
                  _max_retry_count));            

              _retry_counter := _retry_counter + 1;
            ELSE 
              RAISE;
            END IF;
          END;
        END LOOP;
      END IF;

      /* because the task id is not available to the task creators, inject it
       * via special macro.
       */
      PERFORM dblink_send_query(w.name, async.wrap_query(
        replace(r.query, '##flow.TASK_ID##', r.task_id::TEXT), r.task_id));

      UPDATE async.worker SET
        task_id = r.task_id,
        target = r.target,
        running_since = clock_timestamp()
      WHERE slot = w.slot;

      UPDATE async.task SET 
        consumed = clock_timestamp(),
        times_up = now() + COALESCE(
          manual_timeout, 
          r.default_timeout, 
          c.default_timeout),
        tracked = true
      WHERE task_id = r.task_id;

      UPDATE async.concurrency_pool_tracker
      SET workers = workers + 1
      WHERE concurrency_pool = r.concurrency_pool;

    EXCEPTION WHEN OTHERS THEN
      PERFORM async.log(
        'WARNING',
        format('Got %s when attempting to run task %s', SQLERRM, r.task_id));

      UPDATE async.task SET 
        consumed = clock_timestamp(),
        processed = clock_timestamp(),
        failed = true,
        finish_status = 'FAILED',
        processing_error = SQLERRM
      WHERE task_id = r.task_id;

      PERFORM async.disconnect(w.name, 'run task failure');
    END;

    did_stuff := true;
  END LOOP;
END;  
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION async.get_tasks_internal(
  _target TEXT,
  _limit INT DEFAULT 1,
  _timeout INTERVAL DEFAULT '30 seconds',
  task_id OUT BIGINT,
  priority OUT INT,
  times_up OUT TIMESTAMPTZ,
  task_data OUT JSONB) RETURNS SETOF RECORD AS
$$
DECLARE
  r RECORD;
  c async.control;
  _started TIMESTAMPTZ;
  _found BOOL DEFAULT false;
BEGIN 
  SELECT INTO c * FROM async.control;

  _started := clock_timestamp();

  LOOP
    FOR r IN SELECT * FROM async.v_candidate_task
      WHERE 
        target = _target
        AND query IS NULL
      LIMIT _limit  
    LOOP
      _found := true;

      PERFORM async.log(
        format(
          'Providing task id %s to target %s pool %s', 
          r.task_id, 
          _target,
          r.concurrency_pool));

      RETURN QUERY WITH data AS
      (
        UPDATE async.task t SET 
          consumed = clock_timestamp(),
          times_up = now() + COALESCE(
            manual_timeout, 
            r.default_timeout, 
            c.default_timeout),
          tracked = true
        WHERE t.task_id = r.task_id
        RETURNING t.task_id, t.priority, t.times_up, t.task_data
      )
      SELECT * FROM data;

      UPDATE async.concurrency_pool_tracker
      SET workers = workers + 1
      WHERE concurrency_pool = r.concurrency_pool;
    END LOOP;

    EXIT WHEN _found OR clock_timestamp() - _started > _timeout;

    PERFORM pg_sleep(.001);
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION async.check_task_ids(
  _task_ids BIGINT[],
  _context TEXT) RETURNS BIGINT[] AS
$$
DECLARE
  _missing_task_ids BIGINT[];
  _duplicate_task_ids BIGINT[];
  _eligible_task_ids BIGINT[];
BEGIN
  SELECT INTO _missing_task_ids, _duplicate_task_ids, _eligible_task_ids
    array_agg(t.task_id) FILTER(WHERE t2.task_id IS NULL),
    array_agg(t.task_id) FILTER(
      WHERE t2.task_id IS NOT NULL AND processed IS NOT NULL),
    array_agg(t.task_id) FILTER(
      WHERE t2.task_id IS NOT NULL AND processed IS NULL)    
  FROM 
  (
    SELECT unnest(_task_ids) task_id
  ) t
  LEFT JOIN async.task t2 ON
    t.task_id = t2.task_id;

  IF array_upper(_missing_task_ids, 1) > 0
  THEN
    PERFORM async.log(
      'WARNING',
      format(
        'Attempted action on bad task_ids %s for %s', 
        _missing_task_ids,
        _context));
  END IF;

  IF array_upper(_duplicate_task_ids, 1) > 0
  THEN
    PERFORM async.log(
      'WARNING',
      format(
        'Attempted action on already finished task_ids %s for %s', 
        _duplicate_task_ids,
        _context));
  END IF;

  RETURN _eligible_task_ids;

END;
$$ LANGUAGE PLPGSQL;
  


/* certain routines should only be called from within the orchestrator itself,
 * so check for that.
 */
CREATE OR REPLACE FUNCTION async.check_orchestrator(
  _context TEXT) RETURNS VOID AS
$$
DECLARE 
  _pid INT DEFAULT pg_backend_pid();
  _orchestrator_pid INT DEFAULT (SELECT pid FROM async.control);
BEGIN
  IF _pid IS DISTINCT FROM _orchestrator_pid
  THEN
    PERFORM async.log(
      'ERROR',
      format(
        'pid %s attempted illegal action %s reserved to orchestrator pid %s',
        _pid, 
        _context,
        _orchestrator_pid));
  END IF;
END;
$$ LANGUAGE PLPGSQL;

/* 
 * Sets complete and release worker with attached connection (if any).  If
 * the connection is active, the query will be cancelled and additionally 
 * disconnected.  Various adjustments to the task table are then made depending
 * on reason why the task is to be completed as given by status.
 *
 * Some task completions are not actually completions, for example, tasks can be
 * yielded pending some external action, or paused (which will express as a 
 * cancel but allow the task to be picked up again).
 *
 * This function may only be run from the asnyc main server itself.
 */
CREATE OR REPLACE FUNCTION async.finish_internal(
  _task_ids BIGINT[],
  _status async.finish_status_t,
  _context TEXT,
  _error_message TEXT DEFAULT NULL) RETURNS VOID AS
$$
DECLARE
  _finish_time TIMESTAMPTZ DEFAULT clock_timestamp();
  r RECORD;
  _disconnect BOOL;
  _task_id_keep_connections BIGINT[];
  _level TEXT;
  _reap_error_message TEXT;
  _failed BOOL;
  _reaping BOOL;

  _reaping_status JSONB DEFAULT '[]'::JSONB;
BEGIN
  /* only the orchestration process is allowed to finish tasks */
  PERFORM async.check_orchestrator('finish()');

  /* if coming in from the reaper (connections finishing), status is determined
   * from the query result.
   */
  _reaping := _status IS NULL;

  /* log tasks that are fininshed in bulk with explicit status */
  PERFORM async.log(
    format(
      'Finishing task ids {%s} via %s with status, "%s"%s',   
      array_to_string(_task_ids, ', '),
      _context,
      _status,
      COALESCE(' via: ' || _error_message, '')))
  WHERE NOT _reaping;

  /* Cancel queries and disconnect as appropriate.  Any "non-success" result
   * will additionally always disconnect as a precaution.  Properly executed 
   * tasks may keep connection open as an optmiization.  Processing connections 
   * in a loop is painful, but the list should normally be small and the error 
   * trapping has to be precise.
   *
   * Note, reap_tasks checks query 'is_busy', but there are other paths into 
   * this code (for example cancelling) so we double check.
   */
  FOR r IN 
    SELECT 
      w.slot, 
      w.name, 
      q.task_id, 
      t.task_id IS NOT NULL AS valid_task,
      t.asynchronous_finish,
      t.processed IS NOT NULL AS already_finished,
      t.finish_status = 'CANCELED' AS canceled,
      d.name IS NOT NULL AS has_connection
    FROM
    (
      SELECT unnest(_task_ids) task_id
    ) q
    LEFT JOIN async.task t USING(task_id)
    LEFT JOIN async.worker w USING(task_id)
    LEFT JOIN
    (
      SELECT unnest(dblink_get_connections()) name
    ) d USING(name)
  LOOP
    IF NOT r.valid_task
    THEN
      PERFORM async.log(
        'WARNING',
        format(
          'Attempting to finish invalid task ids {%s} via %s with status, "%s"%s',   
          r.task_id,
          _context,
          COALESCE(_status::TEXT, '(reaping)'),
          COALESCE(' via: ' || _error_message, '')));

      CONTINUE;
    ELSIF r.already_finished
    THEN
      PERFORM async.log(
        'WARNING',
        format(
          'Attempting to finish already finished task ids {%s} via %s with status, "%s"%s',   
          r.task_id,
          _context,
          COALESCE(_status::TEXT, '(reaping)'),
          COALESCE(' via: ' || _error_message, '')))
      WHERE NOT r.canceled;

      CONTINUE;
    END IF;

    /* retest worker just in case something else cancelled task from earlier
     * in the loop (say, a cascaded trigger).  Verify via double checking 
     * the task_id in the worker.
     */
    CONTINUE WHEN (SELECT task_id FROM async.worker WHERE slot = r.slot)
      IS DISTINCT FROM r.task_id;

    /* It's time to clear the dblink result state.
     *
     * The dblink API requires two get result calls for async, one to get the 
     * result, one to reset the connection to read-ready state.  errors are 
     * trapped there, but there are edge scenarios where an exception is 
     * thrown from a disconnect, so they are trapped and the task is presumed
     * failed.
     */
    IF r.has_connection
    THEN
      BEGIN
        PERFORM * FROM dblink_get_result(r.name, false) AS R(v TEXT);
        _reap_error_message := dblink_error_message(r.name);
        PERFORM * FROM dblink_get_result(r.name) AS R(v TEXT);
      EXCEPTION WHEN OTHERS THEN
        _reap_error_message := SQLERRM;
      END;
    END IF;

    _error_message := COALESCE(_error_message, _reap_error_message);      

    /* reap tasks leaves responsibility of status to finish. */
    IF _reaping
    THEN
      _failed := _reap_error_message IS DISTINCT FROM 'OK';

      IF NOT _failed
      THEN
        _error_message := NULL;
      END IF;

      _status := CASE 
        WHEN _failed THEN 'FAILED'
        WHEN r.asynchronous_finish THEN 'YIELDED'
        ELSE 'FINISHED'
      END::async.finish_status_t;

      _reaping_status := _reaping_status || jsonb_build_object(
        'task_id', r.task_id,
        'status', _status,
        'error_message', _error_message);

      PERFORM async.log(
        'NOTICE',
        format(
          'Finishing task ids {%s} via %s with status, "%s"%s',   
          array_to_string(array[r.task_id], ', '),
          _context,
          _status,
          COALESCE(' via: ' || _error_message, '')));
    END IF;

    /* assume we don't have to disconnect if the executed task returns 
     * normally.
     */
    _disconnect := _status NOT IN('FINISHED', 'YIELDED');

    /* dblink can raise spurious erorrs during various network induced edge 
     * cases...if so capture them and blend message into the task error.  
     * Precise error trapping is important so that edge case errors can be 
     * recorded to the task table.
     */
    BEGIN
      IF dblink_is_busy(r.name) = 1
      THEN
        _disconnect := true;
        PERFORM dblink_cancel_query(r.name);
      END IF;
    EXCEPTION WHEN OTHERS THEN
      _disconnect := true;
      _error_message := 
        COALESCE(_error_message || '. also, ', '') 
        || format('Got unexpected error %s while canceling %s',
          SQLERRM, r.name); 
    END;

    /* Also be nervous about thrown errors from disconnect.
     * XXX: TODO: preserve connection in some scenarios 
     */
    IF _disconnect
    THEN
      PERFORM async.disconnect(r.name, 'failure during finish task');
    ELSE
      _task_id_keep_connections := _task_id_keep_connections || r.task_id;
    END IF;
  END LOOP;

  /* clear worker.  if connection is kept, target will be left alone */
  UPDATE async.worker SET 
    task_id = NULL,
    running_since = NULL
  WHERE task_id = ANY(_task_ids);   

  /* mark task complete! */
  UPDATE async.task t SET
    processed = CASE 
      WHEN q.status NOT IN ('YIELDED', 'PAUSED') THEN _finish_time
    END,
    yielded = CASE WHEN q.status = 'YIELDED' THEN _finish_time END,
    failed = q.status IN ('FAILED', 'CANCELED', 'TIMED_OUT'),
    processing_error = NULLIF(error_message, 'OK'),
    /* pause state is special; move task back into unprocessed state */
    consumed = CASE WHEN q.status != 'PAUSED' THEN consumed END,
    finish_status = NULLIF(q.status, 'PAUSED')
  FROM
  (
    SELECT 
      task_id,
      COALESCE(reap.status, _status) AS status,
      COALESCE(reap.error_message, _error_message) AS error_message
    FROM unnest(_task_ids) task_id
    LEFT JOIN 
    (
      SELECT
        (j->>'task_id')::BIGINT AS task_id,
        (j->>'status')::async.finish_status_t AS status,
        (j->>'error_message') AS error_message
      FROM jsonb_array_elements(_reaping_status) j 
    ) reap USING(task_id)
  ) q
  WHERE t.task_id = q.task_id;
  
  WITH untrack AS
  (
    /* manage concurrency pool thread count. If finished, it will bet set false,
     * but if yielded, it will be set null to revert to original state.
     */
    UPDATE async.task t SET
      tracked = CASE WHEN processed IS NOT NULL THEN false END
    WHERE 
      task_id = any(_task_ids)
      AND tracked
      AND (
        processed IS NOT NULL 
        OR (
          yielded IS NOT NULL
          AND NOT COALESCE(track_yielded, false)
        ) 
      )
    RETURNING t.concurrency_pool   
  ) 
  UPDATE async.concurrency_pool_tracker p SET 
    workers = workers - newly_finished
  FROM 
  (
    SELECT 
      concurrency_pool, 
      count(*) AS newly_finished
    FROM untrack
    GROUP BY 1
  ) q
  WHERE p.concurrency_pool = q.concurrency_pool;
END;
$$ LANGUAGE PLPGSQL;





/* Update task to completion state based on query resolving in background. */
CREATE OR REPLACE FUNCTION async.reap_tasks() RETURNS VOID AS 
$$
DECLARE
  r RECORD;
  _error_message TEXT;
  _failed BOOL;
  _reap_task_ids BIGINT[];
BEGIN
  PERFORM async.finish_internal(
    tasks,
    'FAILED'::async.finish_status_t,
    'time out tasks',
    'Canceling due to time out')
  FROM
  (
    SELECT array_agg(task_id) AS tasks
    FROM async.task t
    WHERE 
      processed IS NULL
      /* making extra extra sure partial index is utilized */
      AND times_up IS NOT NULL
      AND times_up < now()
  )
  WHERE array_upper(tasks, 1) >= 1;

  PERFORM async.finish_internal(
    task_ids,
    NULL::async.finish_status_t,
    'async.reap_tasks')
  FROM
  (
    SELECT array_agg(t.task_id) AS task_ids
    FROM async.worker w 
    LEFT JOIN async.task t USING(task_id)
    WHERE 
      w.task_id IS NOT NULL
      AND name = any(dblink_get_connections())
      AND dblink_is_busy(w.name) != 1
  ) q
  WHERE array_upper(task_ids, 1) >= 1;
END;
$$ LANGUAGE PLPGSQL;




CREATE OR REPLACE PROCEDURE async.maintenance(
  _did_work BOOL) AS
$$
DECLARE
  g async.control;
  _bad_pools TEXT[];
  _start TIMESTAMPTZ;
BEGIN 
  SELECT INTO g * FROM async.control;
  
  IF now() > g.last_heavy_maintenance + g.heavy_maintenance_sleep
    OR g.last_heavy_maintenance IS NULL
  THEN
    _start := clock_timestamp();

    PERFORM async.log('Performing heavy maintenance');
    UPDATE async.control SET last_heavy_maintenance = now();

    DELETE FROM async.task WHERE processed <= now() - g.task_keep_duration;

    PERFORM async.log(format(
      'Performed heavy maintenance in %s seconds',
      round(extract('epoch' from clock_timestamp() - _start), 2)));    

    _did_work := true;
  END IF;

  IF 
    (now() > g.last_light_maintenance + g.light_maintenance_sleep AND _did_work)
    OR g.last_light_maintenance IS NULL
  THEN
    _start := clock_timestamp();

    COMMIT;

    PERFORM dblink_exec(
      (SELECT connection_string FROM async.target WHERE target = g.self_target), 
      'VACUUM FULL ANALYZE async.worker');

    PERFORM dblink_exec(
      (SELECT connection_string FROM async.target WHERE target = g.self_target), 
      'VACUUM FULL ANALYZE async.concurrency_pool_tracker');

    DELETE FROM async.concurrency_pool_tracker cpt
    WHERE 
      NOT EXISTS (
        SELECT 1 FROM async.task t
        WHERE 
          t.processed IS NULL
          AND t.concurrency_pool = cpt.concurrency_pool
      )
      AND
      (
        workers < 0
        OR 
        (
          workers = 0
          AND concurrency_pool NOT IN (SELECT target FROM async.target)
        )
      );

    UPDATE async.control SET last_light_maintenance = now();

    /* clean up client latch, but not for very recently set latches so as to
     * not race in front of the latch waiter
     */
    DELETE FROM async.request_latch 
    WHERE ready IS NOT NULL and now() - ready > '5 minutes'::INTERVAL;

    PERFORM async.log(format(
      'Performed light maintenance in %s seconds',
      round(extract('epoch' from clock_timestamp() - _start), 2)));
  END IF;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION async.run_internal() RETURNS BOOL AS
$$
DECLARE
  r RECORD;
  _did_stuff BOOL DEFAULT false;
BEGIN
  /* optimized path for bulk task finish */
  FOR r IN 
    SELECT 
      array_agg(task_id) AS task_ids, 
      status,
      error_message,
      array_agg(deferred_task_id) AS deferred_task_ids
    FROM 
    (
      SELECT 
        deferred_task_id,
        status,
        error_message,
        unnest(task_ids) AS task_id
      FROM
      (
        SELECT 
          t.task_id AS deferred_task_id,
          d.*
        FROM async.task t
        CROSS JOIN LATERAL
        (
          SELECT * 
          FROM jsonb_to_record(task_data) AS R(
            task_ids BIGINT[],
            status async.finish_status_t,
            error_message TEXT)
        ) d                 
        WHERE t.consumed IS NULL
        AND t.processed IS NULL
        AND t.yielded IS NULL
        AND t.concurrency_pool = (SELECT self_target FROM async.control)
      ) q
    )
    GROUP BY 2, 3
  LOOP
    UPDATE async.task SET 
      consumed = now(),
      processed = now()
    WHERE task_id = any(r.deferred_task_ids);

    PERFORM async.finish_internal(
      r.task_ids, 
      r.status,
      'async.run_internal',
      r.error_message);

    _did_stuff := true;
  END LOOP;

  /* XXX: handle non finished tasks (with partial index supporting) */
  RETURN _did_stuff;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE async.do_work(did_work INOUT BOOL DEFAULT NULL) AS
$$
DECLARE
  _when TIMESTAMPTZ;
  _reap_time INTERVAL;
  _internal_time INTERVAL;
  _routine_time INTERVAL;

  _run_time INTERVAL;
  _did_internal BOOL;
  _did_run BOOL;
BEGIN
  _when := clock_timestamp();
  PERFORM async.reap_tasks();
  _reap_time := clock_timestamp() - _when;

  _when := clock_timestamp();
  _did_internal := async.run_internal();
  _internal_time := clock_timestamp() - _when;    

  _when := clock_timestamp();
  _did_run := async.run_tasks();
  _run_time := clock_timestamp() - _when;

  did_work := _did_internal OR _did_run;

  _when := clock_timestamp();
  CALL async.run_routines('LOOP');
  _routine_time := clock_timestamp() - _when;

  IF did_work
  THEN
    PERFORM async.log(
      format(
        'timing: reap: %s internal: %s run: %s routine: %s', 
        _reap_time, _internal_time, _run_time, _routine_time));
  END IF;

  CALL async.maintenance(did_work);
END;  
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION async.push_routine(
  _routine_type async.routine_type_t,
  _routine TEXT) RETURNS VOID AS
$$
BEGIN
  IF _routine_type = 'STARTUP'
  THEN
    UPDATE async.control SET startup_routines = startup_routines ||
      _routine
    WHERE startup_routines IS NULL OR NOT _routine = any(startup_routines);
  ELSEIF _routine_type = 'LOOP'
  THEN
    UPDATE async.control SET loop_routines = loop_routines ||
      _routine
    WHERE loop_routines IS NULL OR NOT _routine = any(loop_routines);
  END IF;
END;
$$ LANGUAGE PLPGSQL;




CREATE OR REPLACE FUNCTION async.clear_latches() RETURNS VOID AS
$$
DECLARE
  r RECORD;
BEGIN
  FOR r IN 
    UPDATE async.request_latch SET ready = now()
    WHERE ready IS NULL
    RETURNING request_latch_id
  LOOP
    PERFORM async.log(
      format('Cleared request_latch_id %s', r.request_latch_id));
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE async.acquire_mutex(
  _lock_id INT,
  _force BOOL DEFAULT FALSE,
  acquired INOUT BOOL DEFAULT FALSE) AS
$$
DECLARE
  _acquired BOOL;
  _pid INT;
BEGIN
  IF NOT pg_try_advisory_lock(_lock_id)
  THEN
    IF NOT _force 
    THEN
      acquired := false;
      RETURN;
    END IF;

    /* force enabled flag false to kindly ask other process to halt */
    UPDATE async.control SET enabled = false;
    COMMIT;

    SELECT INTO _pid pid FROM pg_locks 
    WHERE 
      granted 
      AND locktype = 'advisory'
      AND (objid, classid) = (0, 0);

    IF _pid IS NULL
    THEN
      PERFORM async.log(
        'ERROR', 
        'Unable to acquire lock with no controlling pid...try again?');
    END IF;

    IF _pid IS DISTINCT FROM (SELECT pid FROM async.control)
    THEN
      PERFORM async.log(
        'WARNING',
        format(
          'Lock owning pid %s does not match expected pid %s',
          _pid,
          (SELECT pid FROM async.control)));
    END IF;

    PERFORM async.log(format('Force acquiring over main process %s', _pid));
    PERFORM pg_terminate_backend(_pid);

    FOR x IN 1..5
    LOOP
      acquired := pg_try_advisory_lock(0);

      IF acquired 
      THEN
        /* force enabled flag false to kindly ask other process to halt */
        UPDATE async.control SET enabled = true;

        PERFORM async.log('Lock forcefully acquired');
        acquired := true;
        RETURN;
      END IF;

      PERFORM async.log(format('Waiting on pid %s to release lock', _pid));
      PERFORM pg_sleep(1.0);
    END LOOP;

    IF NOT acquired
    THEN
      PERFORM async.log('ERROR', 'Unable to acquire lock...try again later?');
    END IF;
  END IF;

  acquired := true;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE async.run_routines(
  _routine_type async.routine_type_t) AS
$$
DECLARE
  _routine TEXT;
BEGIN
  FOR _routine IN 
    SELECT unnest(
      CASE _routine_type
        WHEN 'STARTUP' THEN startup_routines
        WHEN 'LOOP' THEN loop_routines
      END)
    FROM async.control
  LOOP
    EXECUTE 'CALL ' || _routine;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE PROCEDURE async.main(_force BOOL DEFAULT false) AS
$$
DECLARE 
  g async.control;
  _show_message BOOL DEFAULT TRUE;

  _did_stuff BOOL DEFAULT FALSE;

  _last_did_stuff TIMESTAMPTZ;
  _back_off INTERVAL DEFAULT '30 seconds';

  _acquired BOOL;

  _routine TEXT;
BEGIN
  SELECT INTO g * FROM async.control;

  /* yeet! */
  IF NOT g.enabled AND NOT _force
  THEN
    IF NOT g.enabled
    THEN
      PERFORM async.log('Orchestrator is disabled (see async.control). Exiting');
    END IF;

    RETURN;
  END IF;

  /* attempt to acquire process lock */
  CALL async.acquire_mutex(g.advisory_mutex_id, _force, _acquired);

  IF NOT _acquired
  THEN
    RETURN;
  END IF;

  /* attempt to acquire process lock */
  PERFORM async.log('Initializing asynchronous query processor');

  /* if being run from pg_cron, lower client messages to try and minimize log
   * pollution.
   */
  IF (SELECT backend_type FROM pg_stat_activity WHERE pid = pg_backend_pid())
    = 'pg_cron'
  THEN
    PERFORM async.log('pg_cron detected.  Client logging set to WARNING');
    SET client_min_messages = 'WARNING';
  END IF;

  /* dragons live forever, but not so little boys */
  SET statement_timeout = 0;
  
  /* run maintenance now as a precaution */
  CALL async.maintenance(true);

  /* clear any oustanding latches */
  DELETE FROM async.request_latch;

  /* clear out any tasks that may have been left in running state */
  SET LOCAL enable_seqscan TO false;
  SET LOCAL enable_bitmapscan TO false;

  PERFORM async.log('Cleaning up unfinished tasks (if any)');
  UPDATE async.task SET 
    processed = now(),
    failed = true,
    processing_error = 'presumed failed due to async startup'
  WHERE 
    consumed IS NOT NULL
    AND processed IS NULL;

  SET LOCAL enable_seqscan TO DEFAULT;
  SET LOCAL enable_bitmapscan TO DEFAULT;

  /* run startup routines for user supplied cleanup */
  CALL async.run_routines('STARTUP');

  /* reset worker table with a startup flag here so that the initialization to 
   * extend at runtime if needed.
   */
  PERFORM async.log('Initializing workers');
  PERFORM async.initialize_workers(g, true);    

  UPDATE async.control SET 
    pid = pg_backend_pid(),
    running_since = clock_timestamp(),
    enabled = true;

  PERFORM async.log('Initialization of query processor complete');    
  LOOP
    SELECT INTO g * FROM async.control;

    IF NOT g.Paused
    THEN
      CALL async.do_work(_did_stuff);
    END IF;

    /* flush transaction state */
    COMMIT;

    PERFORM async.clear_latches();

    IF _did_stuff
    THEN
      _show_message := true;
      _last_did_stuff := clock_timestamp();
    ELSE
      IF NOT g.enabled
      THEN
        RETURN;
      END IF;    
 
      /* wait a little bit before showing message, and be aggressive */      
      IF _show_message 
      THEN
        IF clock_timestamp() - _last_did_stuff > _back_off  
          OR _last_did_stuff IS NULL
        THEN
          _show_message := false;  
          PERFORM async.log('Nothing to do. sleeping...');
        END IF;

        PERFORM pg_sleep(g.busy_sleep);
      ELSE
        PERFORM pg_sleep(g.idle_sleep);  
      END IF;
    END IF;
  END LOOP;  
END;
$$ LANGUAGE PLPGSQL;


END;
$code$;





/* Views and functions to support flow administration from UI */

CREATE OR REPLACE FUNCTION async.worker_info(
  worker_info OUT JSONB) RETURNS JSONB AS
$$
BEGIN
  SELECT jsonb_agg(q) INTO worker_info
  FROM
  (
    SELECT 
      slot,
      w.target,
      task_id,
      running_since,
      query, 
      priority
    FROM async.worker w
    LEFT JOIN async.task t USING(task_id)
    ORDER BY slot
  ) q;  
END;
$$ LANGUAGE PLPGSQL;