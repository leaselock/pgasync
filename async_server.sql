/* Standard routines and structures for asynchronous processing.   Intended
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
  workers INT DEFAULT 100,
  idle_sleep FLOAT8 DEFAULT 0.1,
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
  busy_sleep FLOAT8 DEFAULT 0.01,

  light_maintenance_sleep INTERVAL DEFAULT '5 minutes'::INTERVAL,
  last_light_maintenance TIMESTAMPTZ,

  default_concurrency_pool_workers INT DEFAULT 4,

  /* called upon async start up */
  startup_routines TEXT[],

  /* called upon on async loop */
  loop_routines TEXT[],

  version TEXT DEFAULT '1.0',

  debug_log BOOL DEFAULT true

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

  track_yielded BOOL,

  /* task has been delayed until after this time (if not null) */
  eligible_when TIMESTAMPTZ
);

/* supports fetching eligible tasks */
CREATE INDEX ON async.task(concurrency_pool, priority, entered) 
WHERE 
  consumed IS NULL
  AND yielded IS NULL
  AND processed IS NULL;  

CREATE INDEX ON async.task(times_up)
  WHERE 
    processed IS NULL
    /* postgres strangely picks this index when it should not, so force an
     * unusual qual so that it is ineligible for standard queries
     */
    AND times_up IS NOT NULL;

/* supports cleaning up dead tasks on startup */
CREATE INDEX ON async.task(task_id)
  WHERE 
    consumed IS NOT NULL
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

CREATE OR REPLACE FUNCTION async.debug_log() RETURNS BOOL AS
$$
  SELECT debug_log FROM async.control;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OR REPLACE FUNCTION async.log(
  _level TEXT,
  _message TEXT) RETURNS VOID AS
$$
DECLARE
  _debug_log BOOL DEFAULT (SELECT async.debug_log());
BEGIN
  IF _level = 'NOTICE'
  THEN 
    RAISE NOTICE '%', _message;
  ELSEIF _level = 'WARNING'
  THEN
    RAISE WARNING '%', _message;
  ELSEIF _level = 'DEBUG'
  THEN
    IF NOT _debug_log
    THEN
      RETURN;
    END IF;
    
    RAISE NOTICE '%', _message;
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
  WHERE 
    processed IS NULL
    AND concurrency_pool IS NOT NULL;

  INSERT INTO async.worker SELECT 
    s,
    'async.worker_' || s
  FROM generate_series(1, g.workers) s;

  PERFORM async.log('Worker initialization complete');
END;
$$ LANGUAGE PLPGSQL;


/* reconfigure worker count on the fly. Will only adjust down if the 
 * until hitting a busy slot.
 */
CREATE OR REPLACE FUNCTION async.reinitialize_workers() RETURNS VOID AS
$$
DECLARE
  _live_worker_count INT;
  _configured_worker_count INT;
  _active_connections TEXT[];
  w RECORD;
  _min_free_slot INT;
BEGIN
  SELECT INTO _live_worker_count count(*) FROM async.worker;
  SELECT INTO _configured_worker_count workers FROM async.control;

  IF _configured_worker_count = _live_worker_count
  THEN
    RETURN;
  END IF;

  IF _configured_worker_count > _live_worker_count
  THEN
    PERFORM async.log(
      format('Expanding worker count from %s to %s workers',
      _live_worker_count, 
      _configured_worker_count));

    INSERT INTO async.worker SELECT 
      s,
      'async.worker_' || s
    FROM generate_series(_live_worker_count + 1, _configured_worker_count) s;
  ELSE
    SELECT INTO _min_free_slot min(slot)
    FROM async.worker w1
    WHERE 
      slot > _configured_worker_count
      AND NOT EXISTS (
        SELECT 1 FROM async.worker w2
        WHERE 
          w2.slot >= w1.slot
          AND w2.slot > _configured_worker_count
          AND w2.task_id IS NOT NULL
      );

    IF _min_free_slot IS NULL
    THEN
      PERFORM async.log('Unable to free any workers due to being active');
      RETURN;
    END IF;

    IF _min_free_slot > _configured_worker_count + 1
    THEN
      PERFORM async.log(
        format(
          'Reducing to %s workers, accounting for tasks in progress',
          _min_free_slot));
    ELSE
      PERFORM async.log(
        format(
          'Reducing worker count from %s to %s workers',
          _live_worker_count, 
          _configured_worker_count));    
    END IF;

    _active_connections := dblink_get_connections();

    FOR w IN 
      SELECT *, name = ANY(_active_connections) AS active 
      FROM async.worker w
      WHERE slot >= _min_free_slot
    LOOP
      IF w.active 
      THEN
        PERFORM async.disconnect(name, 'worker shutdown');
      END IF;

      DELETE FROM async.worker WHERE slot = w.slot;
    END LOOP;
  END IF;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION async.active_workers() RETURNS BIGINT AS
$$
  SELECT count(*) 
  FROM async.worker
  WHERE task_id IS NOT NULL;
$$ LANGUAGE SQL STABLE;



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
        AND (eligible_when IS NULL OR eligible_when <= now())
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

CREATE OR REPLACE VIEW async.v_task_assigned_worker AS 
  WITH tasks_to_run AS MATERIALIZED
  (
    SELECT 
      t.*,
      row_number() OVER (PARTITION BY target) idx 
    FROM async.v_candidate_task t
    WHERE query IS NOT NULL
  ),
  avail_workers AS
  (
    SELECT 
      w2.*,
      row_number() OVER (PARTITION BY target) idx
    FROM async.worker w2
    WHERE w2.task_id IS NULL
  ),
  matched_target AS
  (
    SELECT 
      t.*,
      NULL::INT AS idx2,
      aw.slot,
      aw.name,
      'keep'::TEXT AS connect_action
    FROM tasks_to_run t
    JOIN avail_workers aw USING(target, idx)
  ),
  remain_workers AS
  (
    SELECT 
      w2.*,
      row_number() OVER (
        PARTITION BY w2.target
        ORDER BY CASE WHEN w2.target IS NULL THEN 0 ELSE 1 END) idx2
    FROM async.worker w2
    LEFT JOIN matched_target mt USING(slot)
    WHERE 
      mt.target IS NULL
      AND w2.task_id IS NULL
  ),
  remain_tasks AS
  (
    SELECT 
      t.*,
      row_number() OVER (
        PARTITION BY t.target) idx2
    FROM tasks_to_run t
    LEFT JOIN matched_target mt USING (task_id)
    WHERE mt.task_id IS NULL
  )
  SELECT mt.* 
  FROM matched_target mt
  UNION ALL SELECT 
    rt.*,
    rw.slot,
    rw.name,
    CASE WHEN 
      rw.target IS NULL THEN 'connect'::TEXT 
      ELSE 'reconnect' 
    END AS connect_action      
  FROM remain_tasks rt
  JOIN remain_workers rw USING(idx2);




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


/* experimental version to consolidate queries reading and writing */
CREATE OR REPLACE FUNCTION async.run_tasks_experimental(did_stuff OUT BOOL) RETURNS BOOL AS
$$
DECLARE
  r RECORD;
  c async.control;
  _max_retry_count INT DEFAULT 5;
  _retry_counter INT DEFAULT 1;

  _tasks_ran JSONB DEFAULT '[]'::JSONB;
BEGIN 
  did_stuff := false;

  SELECT INTO c * FROM async.control;

  FOR r IN SELECT * FROM async.v_task_assigned_worker
  LOOP
    BEGIN
      PERFORM async.log(
        'DEBUG',
        format(
          'Running task id %s pool %s slot: %s %s %s[action %s]', 
          r.task_id, 
          r.concurrency_pool,
          r.slot,
          COALESCE('data: "' || r.task_data::TEXT || '" ', ''),
          CASE WHEN r.source IS NOT NULL
            THEN format('via %s ',  r.source)
            ELSE ''
          END,
          r.connect_action));
    
      IF r.connect_action = 'keep'
      THEN
        BEGIN
          PERFORM * FROM dblink(r.name, 'SELECT 0') AS R(v INT);
        EXCEPTION WHEN OTHERS THEN
          PERFORM async.log(
            'WARNING',
            format(
              'When attempting to run task: %s in slot: %s '
              'for connection: %s, got %s', 
              r.task_id,
              r.slot,
              r.name,
              SQLERRM));

            r.connect_action := 'reconnect';
        END;
      END IF;

      IF r.connect_action = 'reconnect'
      THEN
        PERFORM async.disconnect(r.name, 'reconnect');
        r.connect_action = 'connect';
      END IF;

      IF r.connect_action = 'connect'
      THEN
        LOOP
          /* give it the old college try...if connection fields repeat a few 
           * times before giving up.
           * XXX: Maybe better to yield the task 
           * back with some future execution time.
           */
          BEGIN 
            PERFORM dblink_connect(r.name, r.connection_string);

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
      PERFORM dblink_send_query(r.name, async.wrap_query(
        replace(r.query, '##flow.TASK_ID##', r.task_id::TEXT), r.task_id));

      _tasks_ran := _tasks_ran || jsonb_build_object(
        'task_id', r.task_id,
        'target', r.target,
        'slot', r.slot,
        'pool', r.concurrency_pool,
        'timeout', r.default_timeout);

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

      PERFORM async.disconnect(r.name, 'run task failure');
    END;

    did_stuff := true;
  END LOOP;

  IF did_stuff
  THEN
    WITH tasks_ran AS
    (
      SELECT
        (j->>'task_id')::BIGINT AS task_id,
        j->>'target' AS target,
        (j->>'slot')::INT AS slot,
        j->>'pool' AS concurrency_pool,
        (j->>'timeout')::INTERVAL AS default_timeout,
        timing
      FROM jsonb_array_elements(_tasks_ran) j
      CROSS JOIN (SELECT clock_timestamp() AS timing) 
    ),
    upd_worker AS
    (
      UPDATE async.worker aw SET
        task_id = tr.task_id,
        target = tr.target,
        running_since = timing
      FROM tasks_ran tr
      WHERE tr.slot = aw.slot
    ),
    upd_task AS
    (
      UPDATE async.task t SET 
        consumed = timing,
        times_up = timing + COALESCE(
          manual_timeout, 
          default_timeout, 
          c.default_timeout),
        tracked = true
      FROM tasks_ran tr
      WHERE tr.task_id = t.task_id
    )
    UPDATE async.concurrency_pool_tracker cpt
    SET workers = workers + count
    FROM
    (
      SELECT
        concurrency_pool,
        COUNT(*)
      FROM tasks_ran 
      GROUP BY 1
    ) q
    WHERE cpt.concurrency_pool = q.concurrency_pool;         
  END IF;
END; 
$$ LANGUAGE PLPGSQL;

/* looks up eligible tasks and assigns to a worker */
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
        'DEBUG',
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
        times_up = 
          CASE 
            WHEN finish_status = 'DEFERRED' AND times_up IS NOT NULL
              THEN times_up
            ELSE
              now() + COALESCE(
                manual_timeout,
                r.default_timeout,
                c.default_timeout)
            END,
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
        'DEBUG',
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
  _error_message TEXT,
  _duration INTERVAL) RETURNS VOID AS
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
    'DEBUG',
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
        'DEBUG',
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
    _disconnect := _status NOT IN('FINISHED', 'YIELDED', 'DEFERRED');

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
      WHEN q.status NOT IN ('YIELDED', 'PAUSED', 'DEFERRED') THEN _finish_time
    END,
    yielded = CASE WHEN q.status = 'YIELDED' THEN _finish_time END,
    failed = q.status IN ('FAILED', 'CANCELED', 'TIMED_OUT'),
    processing_error = NULLIF(error_message, 'OK'),
    /* pause state is special; move task back into unprocessed state */
    consumed = CASE WHEN q.status NOT IN('PAUSED', 'DEFERRED') THEN consumed END,
    finish_status = NULLIF(q.status, 'PAUSED'),
    eligible_when = CASE WHEN q.status = 'DEFERRED' THEN now() + _duration END
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
     * reducing the tracker.
     *
     * Yielded task will reduce tracker if configured to do so.  Deferred
     * tasks will reduce tracker.
     */
    UPDATE async.task t SET
      tracked = CASE 
        WHEN processed IS NOT NULL THEN false
        WHEN finish_status = 'DEFERRED' THEN false 
      END
    WHERE 
      task_id = any(_task_ids)
      AND tracked
      AND (
        processed IS NOT NULL 
        OR (
          yielded IS NOT NULL
          AND NOT COALESCE(track_yielded, false)
        )
        OR finish_status = 'DEFERRED'
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
CREATE OR REPLACE FUNCTION async.reap_tasks() RETURNS BOOL AS 
$$
DECLARE
  r RECORD;
  _error_message TEXT;
  _failed BOOL;
  _reap_task_ids BIGINT[];
  _did_stuff BOOL DEFAULT false;
BEGIN
  PERFORM async.finish_internal(
    tasks,
    'FAILED'::async.finish_status_t,
    'time out tasks',
    'Canceling due to time out',
    NULL::INTERVAL)
  FROM
  (
    SELECT array_agg(task_id) AS tasks
    FROM async.task t
    WHERE 
      processed IS NULL
      AND times_up < now()
  )
  WHERE array_upper(tasks, 1) >= 1;

  _did_stuff := FOUND;

  PERFORM async.finish_internal(
    task_ids,
    NULL::async.finish_status_t,
    'async.reap_tasks',
    NULL,
    NULL::INTERVAL)
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

  RETURN _did_stuff OR FOUND;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION async.query_with_timeout(
  _query TEXT, 
  _timeout INTERVAL DEFAULT '30 seconds'::INTERVAL,
  _connection_name TEXT DEFAULT 'async.query_with_timeout',
  _connection_string TEXT DEFAULT NULL,
  response OUT TEXT,
  failed OUT BOOL) RETURNS RECORD AS
$$
DECLARE
  _return TEXT;
  _query_start TIMESTAMPTZ DEFAULT clock_timestamp();
  g async.control;

  _connected BOOL;
BEGIN
  SELECT INTO g * FROM async.control;

  _connection_string := COALESCE(_connection_string,
    (SELECT connection_string FROM async.target WHERE target = g.self_target));

  PERFORM dblink_connect(
    _connection_name,
    _connection_string);

  _connected := true;

  PERFORM dblink_send_query(_connection_name, _query);

  LOOP
    EXIT WHEN dblink_is_busy(_connection_name) = 0 
      OR clock_timestamp() - _query_start > _timeout;
    PERFORM pg_sleep(0.001);
  END LOOP;

  IF dblink_is_busy(_connection_name) = 0
  THEN
    failed := false;
    BEGIN
      PERFORM * FROM dblink_get_result(_connection_name, false) AS R(v TEXT);
      response := dblink_error_message(_connection_name);
    EXCEPTION WHEN OTHERS THEN
      failed := false;
      response := SQLERRM;
    END;
    
    PERFORM * FROM dblink_get_result(_connection_name) AS R(v TEXT);  
  ELSE
    failed := true;
    response := 'Timeout exceeded';

    PERFORM dblink_cancel_query(_connection_name);
  END IF;

  PERFORM dblink_disconnect(_connection_name);
EXCEPTION 
  WHEN OTHERS THEN
    failed := true;
    response := SQLERRM;

    IF _connected
    THEN
      BEGIN
        PERFORM dblink_disconnect(_connection_name);
      EXCEPTION
      WHEN OTHERS THEN NULL;
      END;
    END IF;

END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE PROCEDURE async.maintenance(
  _did_work BOOL) AS
$$
DECLARE
  g async.control;
  _bad_pools TEXT[];
  _start TIMESTAMPTZ;

  cpt RECORD;
BEGIN 
  SELECT INTO g * FROM async.control;
  
  IF now() > g.last_heavy_maintenance + g.heavy_maintenance_sleep
    OR g.last_heavy_maintenance IS NULL
  THEN
    _start := clock_timestamp();

    PERFORM async.log('Performing heavy maintenance');
    COMMIT;

    UPDATE async.control SET last_heavy_maintenance = clock_timestamp();

    DELETE FROM async.task WHERE processed <= clock_timestamp() 
      - g.task_keep_duration;

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

    PERFORM async.log('Performing light maintenance');

    COMMIT;

    /* be very tight on vacuuming worker and pool tracking tables...they are
     * hit hard and slower scan times can really hurt overall performance.
     */
    PERFORM async.log(
      'WARNING',
      format('Got %s when vaccuming async.worker', response))
    FROM async.query_with_timeout(
      'VACUUM FULL async.worker',
      '5 seconds')
    WHERE failed;

    PERFORM async.log(
      'WARNING',
      format('Got %s when vaccuming async.concurrency_pool_tracker', response))
    FROM async.query_with_timeout(
      'VACUUM FULL async.concurrency_pool_tracker',
      '5 seconds')
    WHERE failed;

    FOR cpt IN SELECT * FROM async.concurrency_pool_tracker 
      WHERE 
        workers < 0
        OR 
        (
          workers = 0
          AND concurrency_pool NOT IN (SELECT target FROM async.target)
        )
    LOOP
      PERFORM * FROM async.task t WHERE 
        t.processed IS NULL
        AND t.concurrency_pool IS NOT NULL
        AND t.concurrency_pool = cpt.concurrency_pool
      LIMIT 1;

      IF FOUND 
      THEN
        CONTINUE;
      END IF;

      DELETE FROM async.concurrency_pool_tracker 
      WHERE concurrency_pool = cpt.concurrency_pool;
    END LOOP;

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
      duration,
      array_agg(deferred_task_id) AS deferred_task_ids
    FROM 
    (
      SELECT 
        deferred_task_id,
        status,
        error_message,
        duration,
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
            error_message TEXT,
            duration INTERVAL)
        ) d                 
        WHERE t.consumed IS NULL
        AND t.processed IS NULL
        AND t.yielded IS NULL
        AND t.concurrency_pool = (SELECT self_target FROM async.control)
      ) q
    )
    GROUP BY 2, 3, 4
  LOOP
    UPDATE async.task SET 
      consumed = now(),
      processed = now()
    WHERE task_id = any(r.deferred_task_ids);

    PERFORM async.finish_internal(
      r.task_ids, 
      r.status,
      'async.run_internal',
      r.error_message,
      r.duration::INTERVAL);

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
  _work_start TIMESTAMPTZ;
  _reap_time INTERVAL;
  _internal_time INTERVAL;
  _routine_time INTERVAL;
  _since_commit INTERVAL;
  _total_time INTERVAL;
  _finish_time TIMESTAMPTZ;
  _commit_start TIMESTAMPTZ;

  _run_time INTERVAL;
  _did_internal BOOL;
  _did_run BOOL;
  _did_reap BOOL;
BEGIN
  _commit_start := now();

  _when := clock_timestamp();

  _work_start := _when;
  _did_internal := async.run_internal();
  _internal_time := clock_timestamp() - _when;

  _when := clock_timestamp();
  _did_reap := async.reap_tasks();
  _reap_time := clock_timestamp() - _when;    

  _when := clock_timestamp();
  _did_run := async.run_tasks();
  did_work := _did_internal OR _did_run OR _did_reap;
  _run_time := clock_timestamp() - _when;

  _when := clock_timestamp();
  CALL async.run_routines('LOOP');
  _finish_time := clock_timestamp();
  _routine_time := _finish_time - _when;

  _total_time := _finish_time - _work_start;
  _since_commit := _finish_time - _commit_start;

  IF did_work
  THEN
    PERFORM async.log(
      format(
        'timing: total: %s since commit: %s reap: %s internal: %s run: %s routine: %s', 
        _total_time,
        _finish_time - now(),
        _reap_time, 
        _internal_time, 
        _run_time, 
        _routine_time));
  END IF;

EXCEPTION
  WHEN OTHERS THEN 
    PERFORM async.log('ERROR', format('Unexpected error %s', SQLERRM));
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
  UPDATE async.request_latch SET ready = now()
  WHERE ready IS NULL;
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


CREATE OR REPLACE PROCEDURE async.cycle(
  _last_did_stuff INOUT TIMESTAMPTZ DEFAULT NULL,
  _show_message INOUT BOOL DEFAULT NULL) AS
$$
DECLARE
  g async.control;

  _did_stuff BOOL DEFAULT FALSE;

  _back_off INTERVAL DEFAULT '30 seconds';  
BEGIN
  SELECT INTO g * FROM async.control;

  IF NOT g.Paused
  THEN
    CALL async.do_work(_did_stuff);
    PERFORM async.clear_latches();

    IF _last_did_stuff IS NULL
    THEN 
      RETURN;
    END IF;
  END IF;

  /* flush transaction state */
  COMMIT;

  CALL async.maintenance(_did_stuff);

  IF _did_stuff
  THEN
    _show_message := true;
    _last_did_stuff := clock_timestamp();
  ELSE
    IF NOT g.enabled
    THEN
      RETURN;
    END IF;    

    /* wait a little bit before showing message */      
    IF _show_message OR g.paused
    THEN
      IF clock_timestamp() - _last_did_stuff > _back_off  
        OR _last_did_stuff IS NULL
      THEN
        IF g.paused 
        THEN
          _last_did_stuff := now(); 
          _show_message := true;
          PERFORM async.log('Server paused.');
        ELSE 
          _show_message := false;  
          PERFORM async.log('Nothing to do. sleeping...');
        END IF;
      END IF;

      PERFORM pg_sleep(g.busy_sleep);
    ELSE
      PERFORM pg_sleep(g.idle_sleep);  
    END IF;
  END IF;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE PROCEDURE async.main(
  _force BOOL DEFAULT false,
  _manual_mode BOOL DEFAULT false) AS
$$
DECLARE 
  g async.control;

  _acquired BOOL;
  _last_did_stuff TIMESTAMPTZ DEFAULT now();
  _show_message BOOL DEFAULT true;

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

  UPDATE async.control SET 
    running_since = clock_timestamp();

  /* attempt to acquire process lock */
  PERFORM async.log('Initializing asynchronous query processor');
  COMMIT;

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

  /* jit can lead to performance problems in main orchestrator loop */
  SET jit = off;
  
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
    enabled = true;

  PERFORM async.log('Initialization of query processor complete');    

  IF _manual_mode
  THEN
    PERFORM async.log('Entering manual mode (CALL async.cycle() to iterate)');    
    RETURN;
  END IF;

  LOOP
    CALL async.cycle(_last_did_stuff, _show_message);
  END LOOP;  
END;
$$ LANGUAGE PLPGSQL;


END;
$code$;




