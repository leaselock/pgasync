/* Standard routines and structures for asynchronous processing.   Intended
 * to be standalone module (hence calls to RAISE NOTICE vs log()).  More or less
 * a glorified wrapper to dblink(), but facilitates background query processing
 * to arbitrary targets.
 */

\if :bootstrap


UPDATE async.client_control SET client_only = false;


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
  pid INT /* pid of main background process */
);

CREATE UNIQUE INDEX ON async.control((1));
INSERT INTO async.control DEFAULT VALUES;

CREATE TABLE async.target
(
  target TEXT PRIMARY KEY,
  max_concurrency INT DEFAULT 8,
  connection_string TEXT,
  asynchronous_finish BOOL DEFAULT false
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

  /* alterate processing time for concurrency tracking purposes */
  concurrency_processed TIMESTAMPTZ
);

/* supports fetching eligible tasks */
CREATE INDEX ON async.task(priority, entered) 
WHERE 
  consumed IS NULL
  AND yielded IS NULL
  AND processed IS NULL;


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


-- SELECT cron.schedule('api processor daemon', '* * * * *', 'CALL main()');

\endif


CREATE OR REPLACE VIEW async.v_target AS 
  SELECT 
    t.*,
    COUNT(task_id) AS active_workers
  FROM async.target t
  LEFT JOIN async.worker USING(target)
  GROUP BY target;


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
    PERFORM async.disconnect(n, 'worker initialize')
    FROM 
    (
      SELECT unnest(dblink_get_connections()) n
    ) q
    WHERE n LIKE 'async.worker%';

    DELETE FROM async.worker;
  END IF;

  DELETE FROM async.concurrency_pool_tracker;
  INSERT INTO async.worker SELECT 
    s,
    'async.worker_' || s
  FROM generate_series(1, g.workers) s;
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
      *,
      row_number() OVER (PARTITION BY concurrency_pool) n
    FROM
    (
      SELECT 
        t.*,
        tg.connection_string,
        pt.max_workers,
        pt.workers      
      FROM async.task t
      JOIN async.target tg USING(target)
      LEFT JOIN async.concurrency_pool_tracker pt USING(concurrency_pool)    
      WHERE 
        t.consumed IS NULL
        AND t.processed IS NULL
        AND t.yielded IS NULL  
        AND (pt.concurrency_pool IS NULL OR pt.max_workers > pt.workers)
      ORDER BY priority, entered
      LIMIT (SELECT g.workers * 2 FROM async.control g)
    ) q
  ) q
  WHERE n <= COALESCE(max_workers - workers, 8)
  LIMIT (SELECT g.workers - async.active_workers() FROM async.control g);



CREATE OR REPLACE FUNCTION async.disconnect(
  _name TEXT,
  _reason TEXT) RETURNS VOID AS
$$
BEGIN
  RAISE NOTICE 'Disconnecting worker % via %', _name, _reason;

  BEGIN
    PERFORM dblink_disconnect(_name);
  EXCEPTION WHEN OTHERS THEN 
    RAISE WARNING 'Got % during connection disconnect', SQLERRM;
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
BEGIN
  UPDATE async.task SET consumed = clock_timestamp()
  WHERE task_id = _task_id;

  BEGIN 
    EXECUTE _query;
  EXCEPTION WHEN OTHERS THEN
    _failed := true;
    _error_message := SQLERRM;
  END;

  PERFORM async.finish_internal(
    array[_task_id], 
    CASE 
      WHEN _failed THEN 'FAILED'
      ELSE 'FINISHED'
    END::async.finish_status_t,
    'run deferred task',
    _error_message);

  did_stuff := true;
END;
$$ LANGUAGE PLPGSQL;

/* returns true if at least one task was run */
CREATE OR REPLACE FUNCTION async.run_tasks(did_stuff OUT BOOL) RETURNS BOOL AS
$$
DECLARE
  r async.v_candidate_task;
  w RECORD;
BEGIN 
  did_stuff := false;

  FOR r IN SELECT * FROM async.v_candidate_task
  LOOP
    IF r.target = (SELECT self_target FROM async.control)
    THEN
      /* logging async deferrals pollutes log */
      IF r.source != 'finish_async'
      THEN
        RAISE NOTICE 'Running deferred task id % via %', r.task_id, r.source;
      END IF;

      did_stuff := async.run_deferred_task(r.task_id, r.query);

      CONTINUE;
    END IF;

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
      IF w.connect_action = 'keep'
      THEN
        BEGIN
          PERFORM * FROM dblink(w.name, 'SELECT 0') AS R(v INT);
        EXCEPTION WHEN OTHERS THEN
          RAISE WARNING 'Got % when attempting to recycle connection %', 
            SQLERRM, to_json(w);
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
        PERFORM dblink_connect(w.name, r.connection_string);
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

      UPDATE async.task SET consumed = clock_timestamp()
      WHERE task_id = r.task_id;

      RAISE NOTICE 'Running task id % pool % slot: % % %[action %]', 
        r.task_id, 
        r.concurrency_pool,
        w.slot,
        COALESCE('data: "' || r.task_data::TEXT || '" ', ''),
        CASE WHEN r.source IS NOT NULL
          THEN format('via %s ',  r.source)
          ELSE ''
        END,
        w.connect_action;

      INSERT INTO async.concurrency_pool_tracker(
        concurrency_pool,
        workers,
        max_workers)
      SELECT
        r.concurrency_pool,
        1,
        COALESCE(cp.max_workers, 8)
      FROM
      (
        SELECT r.concurrency_pool
      )
      LEFT JOIN async.concurrency_pool cp USING(concurrency_pool)
      ON CONFLICT ON CONSTRAINT concurrency_pool_tracker_pkey DO UPDATE SET
        workers = concurrency_pool_tracker.workers + 1;

    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'Got % when attempting to run task', SQLERRM;

      UPDATE async.task SET 
        consumed = clock_timestamp(),
        processed = clock_timestamp(),
        failed = true,
        processing_error = SQLERRM
      WHERE task_id = r.task_id;

      PERFORM async.disconnect(w.name, 'run task failure');
    END;

    did_stuff := true;
  END LOOP;
END;  
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION async.check_task_ids(
  _task_ids BIGINT[],
  _context TEXT) RETURNS VOID AS
$$
DECLARE
  _bad_task_ids BIGINT[];
BEGIN
  SELECT INTO _bad_task_ids array_agg(t)
  FROM unnest(_task_ids) t
  WHERE 
    NOT EXISTS (
      SELECT 1 FROM async.task t2
      WHERE t2.task_id = t
    );

  IF array_upper(_bad_task_ids, 1) > 0
  THEN
    RAISE EXCEPTION 'Attempted action on bad task_ids % for %', 
      _bad_task_ids,
      _context;
  END IF;

  SELECT INTO _bad_task_ids array_agg(t)
  FROM unnest(_task_ids) t
  WHERE EXISTS (
      SELECT 1 FROM async.task t2
      WHERE 
        t2.task_id = t
        AND t2.processed IS NOT NULL
    );

  IF array_upper(_bad_task_ids, 1) > 0
  THEN
    RAISE EXCEPTION 'Attempted action on already finished task_ids % for %', 
      _bad_task_ids,
      _context;
  END IF;

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
    RAISE EXCEPTION 
      'pid % attempted illegal action % reserved to orchestrator pid %',
      _pid, 
      _context,
      _orchestrator_pid;
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
  _task_id_keep_connections INT[];
BEGIN
  /* only the orchestration process is allowed to finish tasks */
  PERFORM async.check_orchestrator('finish()');

  /* cheezy check to look for deferred async tasks.
   * they pad the logs for no value 
   */
  IF NOT EXISTS (
    SELECT 1 FROM async.task t
    WHERE 
      t.task_id = _task_ids[1]
      AND source = 'finish_async'
      AND _status = 'FINISHED')
  THEN
    RAISE NOTICE 
      'Finishing task ids {%} via % with status, "%"%', 
      array_to_string(_task_ids, ', '),
      _context,
      _status,
      COALESCE(' via: ' || _error_message, '');
  END IF;

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
    SELECT * 
    FROM async.worker w
    WHERE 
      name = ANY(dblink_get_connections()) 
      AND w.task_id = any(_task_ids)
  LOOP
    /* retest worker just in case something else cancelled task from earlier
     * in the loop (say, a cascaded trigger).  Verify via double checking 
     * the task_id in the worker.
     */
    CONTINUE WHEN (SELECT task_id FROM async.worker WHERE slot = r.slot)
      IS DISTINCT FROM r.task_id;

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
  UPDATE async.task SET
    processed = CASE 
      WHEN _status NOT IN ('YIELDED', 'PAUSED') THEN _finish_time
    END,
    yielded = CASE WHEN _status = 'YIELDED' THEN _finish_time END,
    failed = _status IN ('FAILED', 'CANCELED', 'TIMED_OUT'),
    processing_error = NULLIF(_error_message, 'OK'),
    /* pause state is special; move task back into unprocessed state */
    consumed = CASE WHEN _status != 'PAUSED' THEN consumed END,
    finish_status = NULLIF(_status, 'PAUSED'),
    concurrency_processed = coalesce(concurrency_processed, now())
  WHERE task_id = ANY(_task_ids);

  UPDATE async.concurrency_pool_tracker p SET 
    workers = workers - count
  FROM
  (
    SELECT 
      concurrency_pool, 
      count(*)
    FROM async.task
    WHERE 
      task_id = ANY(_task_ids)
      AND concurrency_pool IS NOT NULL
      AND consumed IS NOT NULL
      AND concurrency_pool != (SELECT self_target FROM async.control)
      AND _status != 'DOA'
      AND source != 'run deferred task'
      AND concurrency_processed = now()
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
BEGIN
  FOR r IN 
    SELECT 
      w.name,
      w.slot,
      t.task_id,
      t.asynchronous_finish
    FROM async.worker w 
    LEFT JOIN async.task t USING(task_id)
  WHERE 
    w.task_id IS NOT NULL
    AND name = any(dblink_get_connections())
  LOOP
    BEGIN
      CONTINUE WHEN dblink_is_busy(r.name) = 1;

      /* The dblink API requires to get result calls for async, one to get the 
       * result, one to reset the connection to read-ready state.  errors are 
       * trapped there, but there are edge scenarios where an exception is 
       * thrown from a disconnect, so they are trapped and the task is presumed
       * failed.
       */
      PERFORM * FROM dblink_get_result(r.name) AS R(v TEXT);
      _error_message := dblink_error_message(r.name);
      PERFORM * FROM dblink_get_result(r.name) AS R(v TEXT);

    EXCEPTION WHEN OTHERS THEN
      _error_message := SQLERRM;
    END;

    _failed := _error_message != 'OK';

    PERFORM async.finish_internal(
      array[r.task_id], 
      CASE 
        WHEN _failed THEN 'FAILED'
        WHEN r.asynchronous_finish THEN 'YIELDED'
        ELSE 'FINISHED'
      END::async.finish_status_t,
      'reap tasks',
      _error_message);
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;




CREATE OR REPLACE FUNCTION async.heavy_maintenance() RETURNS VOID AS
$$
DECLARE
  g async.control;  
BEGIN 
  SELECT INTO g * FROM async.control;

  DELETE FROM async.task 
  WHERE processed < clock_timestamp() - g.task_keep_duration;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION async.do_work() RETURNS BOOL AS
$$
BEGIN
  PERFORM async.heavy_maintenance() 
  FROM async.control
  WHERE clock_timestamp() > last_heavy_maintenance + heavy_maintenance_sleep;
  
  PERFORM async.reap_tasks();

  RETURN async.run_tasks();
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
      /* ???? */
      RAISE EXCEPTION 
        'Unable to acquire lock with no controlling pid...try again?';
    END IF;

    IF _pid IS DISTINCT FROM (SELECT pid FROM async.control)
    THEN
      RAISE WARNING 'Lock owning pid % does not match expected pid %',
        _pid, (SELECT pid FROM async.control);
    END IF;

    RAISE NOTICE 'Force acquiring over main process %', _pid;
    PERFORM pg_terminate_backend(_pid);

    FOR x IN 1..5
    LOOP
      acquired := pg_try_advisory_lock(0);

      IF acquired 
      THEN
        /* force enabled flag false to kindly ask other process to halt */
        UPDATE async.control SET enabled = true;

        RAISE NOTICE 'Lock forcefully acquired';
        acquired := true;
        RETURN;
      END IF;

      RAISE NOTICE 'Waiting on pid % to release lock', _pid;
      PERFORM pg_sleep(1.0);
    END LOOP;

    IF NOT acquired
    THEN
      RAISE EXCEPTION 'Unable to acquire lock...try again later?';
    END IF;
  END IF;

  acquired := true;
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
BEGIN
  SELECT INTO g * FROM async.control;

  /* yeet! */
  IF NOT g.enabled AND NOT _force
  THEN
    RETURN;
  END IF;

  CALL async.acquire_mutex(g.advisory_mutex_id, _force, _acquired);

  IF NOT _acquired
  THEN
    RETURN;
  END IF;

  /* attempt to acquire process lock */
  RAISE NOTICE 'Initializing asynchronous query processor';

  /* dragons live forever, but not so little boys */
  SET statement_timeout = 0;

  /* attempt to acquire process lock */
  RAISE NOTICE 'Heavy heavy_maintenance';

  /* run heavy maitenance now as a precaution */
  PERFORM async.heavy_maintenance();

  /* attempt to acquire process lock */
  RAISE NOTICE 'Initializing workers';

  /* reset worker table with a startup flag here so that the initialization to 
   * extend at runtime if needed.
   */
  PERFORM async.initialize_workers(g, true);

  /* attempt to acquire process lock */
  RAISE NOTICE 'Cleaning up unfinished tasks (if any)';

  /* clear out any tasks that may have been left in running state */
  UPDATE async.task SET 
    processed = now(),
    failed = true,
    processing_error = 'ERROR: presumed failed due to async startup'
  WHERE 
    consumed IS NOT NULL
    AND processed IS NULL;

  UPDATE async.control SET 
    pid = pg_backend_pid(),
    running_since = clock_timestamp();

  RAISE NOTICE 'Initialization of query processor complete.';    
  LOOP
    IF NOT g.Paused
    THEN
      _did_stuff := async.do_work();
    END IF;

    /* flush transaction state */
    COMMIT;

    IF _did_stuff
    THEN
      _show_message := true;
      _last_did_stuff := clock_timestamp();
    ELSE
      IF NOT (SELECT enabled FROM async.control)
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
          RAISE NOTICE 'Nothing to do. sleeping';
        END IF;

        PERFORM pg_sleep(0.000001);
      ELSE
        PERFORM pg_sleep(g.idle_sleep);  
      END IF;
    END IF;
  END LOOP;  
END;
$$ LANGUAGE PLPGSQL;




