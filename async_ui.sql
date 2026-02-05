
/* Views and functions to support flow administration from UI */

CREATE OR REPLACE FUNCTION async.interval_pretty(i INTERVAL) RETURNS TEXT AS
$$
  SELECT
    CASE
      WHEN d > 0 THEN format('%sd %sh %sm %ss', d, h, m, round(s, 0))
      WHEN h > 0 THEN format('%sh %sm %ss', h, m, round(s, 0))
      WHEN m > 0 THEN format('%sm %ss', m, round(s, 0))
      WHEN s < 0.0001 THEN format('%ss', round(s, 5))
      WHEN s < 0.001 THEN format('%ss', round(s, 4))
      WHEN s < 0.01 THEN format('%ss', round(s, 3))
      WHEN s < 0.1 THEN format('%ss', round(s, 2))
      ELSE format('%ss', round(s, 1))
    END
  FROM
  (
    SELECT
      extract('days' FROM i) d,
      extract('hours' FROM i) h,
      extract('minutes' FROM i) m,
      extract('seconds' FROM i)::numeric s
  ) q
$$ LANGUAGE SQL STRICT;

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



CREATE OR REPLACE VIEW async.v_worker_info AS
  SELECT 
    slot,
    w.target,
    task_id,
    running_since,
    query, 
    priority,
    concurrency_pool
  FROM async.worker w
  LEFT JOIN async.task t USING(task_id)
  LEFT JOIN async.concurrency_pool cp USING(concurrency_pool)
  ORDER BY slot;

CREATE OR REPLACE VIEW async.v_concurrency_pool_info AS
  SELECT 
    cp.concurrency_pool,
    COALESCE(cpt.workers, 0) AS workers,
    cp.max_workers,
    t.task_id,
    t.query,
    t.consumed
  FROM async.concurrency_pool cp
  LEFT JOIN async.concurrency_pool_tracker cpt USING(concurrency_pool)
  LEFT JOIN async.task t ON 
    cpt.concurrency_pool = t.concurrency_pool
    AND t.consumed IS NOT NULL
    AND t.processed IS NULL
  ORDER BY lower(cp.concurrency_pool); 


CREATE OR REPLACE VIEW async.v_server_info AS
  SELECT
    CASE WHEN server_pid = observed_pid THEN server_pid END AS server_pid,
    CASE WHEN server_pid = observed_pid THEN now() - running_since END 
      AS uptime,
    last_message,
    CASE 
      WHEN NOT enabled AND observed_pid IS NULL THEN 'disabled'
      WHEN last_message LIKE 'Initializing asynchronous query processor%'
        THEN format(
          'starting up for %s',
          async.interval_pretty(now() - running_since))
      WHEN server_pid IS NULL THEN 'never started'
      WHEN server_pid IS DISTINCT FROM observed_pid 
        AND lock_pid IS NOT NULL THEN 'Not running (locked in console?)'
      WHEN server_pid IS DISTINCT FROM observed_pid THEN 'Not running'
      WHEN paused THEN 'paused'
      WHEN last_message = 'Performing heavy maintenance'
        THEN format(
          'In heavy maintenance for %s', 
          async.interval_pretty(now() - last_message_time))
      WHEN last_message = 'Performing light maintenance'
        THEN format(
          'In light maintenance for %s', 
          async.interval_pretty(now() - last_message_time))        
      WHEN last_message LIKE 'Nothing to do%' THEN 'idle'
      WHEN last_message LIKE 'Initialization of query processor complete%' 
        THEN 'idle'
      WHEN now() - last_message_time > '1 minute' THEN 'stuck'
      WHEN loop_time > '5 seconds'  THEN 'running (critical)'
      WHEN loop_time > '1 second'  THEN 'running (overloaded)'
      WHEN loop_time > '.5 seconds'  THEN 'running (busy)'
      WHEN loop_time > '.1 seconds'  THEN 'running (moderate load)'
      WHEN loop_time <= '.1 seconds'  THEN 'running (healthy)'
      ELSE 
        format(
          'starting up for %s',
          async.interval_pretty(now() - running_since))
    END AS server_status,
    async.interval_pretty(loop_time) AS loop_time,
    async.interval_pretty(reap_time) AS reap_time,
    async.interval_pretty(internal_time) AS internal_time,
    async.interval_pretty(run_time) AS run_time,
    async.interval_pretty(routine_time) AS routine_time,
    async.interval_pretty(since_commit_time) AS since_commit_time,
    w.*,
    p.*,
    r.*
  FROM
  (
    SELECT 
      l1.message AS last_message,
      l1.happened AS last_message_time,
      running_since,
      substring(l2.message from 'total: ([0-9:.]+)')::INTERVAL AS loop_time,
      substring(l2.message from 'reap: ([0-9:.]+)')::INTERVAL AS reap_time,
      substring(l2.message from 'internal: ([0-9:.]+)')::INTERVAL 
        AS internal_time,
      substring(l2.message from 'run: ([0-9:.]+)')::INTERVAL AS run_time,
      substring(l2.message from 'routine: ([0-9:.]+)')::INTERVAL AS routine_time,
      substring(l2.message from 'since commit: ([0-9:.]+)')::INTERVAL 
        AS since_commit_time,
      c.pid AS server_pid,      
      s.pid AS observed_pid,
      enabled,
      paused,
      lk.pid AS lock_pid
    FROM async.control c
    LEFT JOIN pg_stat_activity s ON 
      s.query ILIKE '%call async.main(%'
      AND s.pid != pg_backend_pid()
      AND state != 'idle'
      AND datname = current_database()
    LEFT JOIN pg_locks lk ON 
      locktype = 'advisory'
      AND objid = c.advisory_mutex_id
      AND granted
      AND database = 
        (SELECT oid FROM pg_database WHERE datname = current_database())
    LEFT JOIN
    (
      SELECT * FROM async.server_log ORDER BY 1 DESC LIMIT 1
    ) l1 ON true
    LEFT JOIN
    (
      SELECT * FROM async.server_log 
      WHERE message LIKE 'timing: %'
      ORDER BY 1 DESC LIMIT 1
    ) l2 ON true    
  )
  LEFT JOIN
  (
    SELECT
      COUNT(*) AS workers,
      COUNT(*) FILTER (WHERE task_id IS NOT NULL) AS running_workers,
      async.interval_pretty(max(now() - running_since))
        AS longest_running_worker
    FROM async.worker
  ) w ON true
  LEFT JOIN
  (
    SELECT
      COALESCE(COUNT(*), 0) AS concurrency_pools,
      COALESCE(COUNT(*) FILTER (WHERE workers = 0), 0) AS idle_pools,
      COALESCE(COUNT(*) FILTER (WHERE workers > 0 AND workers < max_workers), 0) 
        AS running_pools,
      COALESCE(COUNT(*) FILTER (WHERE workers = max_workers), 0) AS full_pools,
      COALESCE(sum(workers), 0) AS tracked_running_tasks
    FROM async.concurrency_pool_tracker
  ) p ON true
  LEFT JOIN
  (
    WITH running AS
    (
      SELECT * 
      FROM async.task
      WHERE 
        processed IS NULL
        AND concurrency_pool IS NOT NULL      
    )
    SELECT
      a.*,
      r.task_id AS longest_running_task_id,
      r.query AS longest_running_task_query,
      longest_running_task_duration
    FROM
    (
      SELECT 
        COALESCE(COUNT(*), 0) AS unfinished_tasks,
        COALESCE(COUNT(*) FILTER (
          WHERE 
            r.consumed IS NULL
            AND r.yielded IS NULL), 0) AS pending_tasks,
        COALESCE(COUNT(*) FILTER (
          WHERE r.yielded IS NOT NULL), 0) AS yielded_tasks,
        COALESCE(COUNT(*) FILTER (
          WHERE r.consumed IS NOT NULL AND r.yielded IS NULL), 0)
          AS running_tasks,
        async.interval_pretty(max(now() - r.entered) FILTER(
          WHERE 
            r.consumed IS NULL
            AND r.yielded IS NULL))
          AS longest_pending_task_duration      
      FROM running r
    ) a
    LEFT JOIN
    (
      SELECT 
        r.*,
        async.interval_pretty(now() - r.consumed) 
          AS longest_running_task_duration
      FROM running r
      WHERE 
        consumed IS NOT NULL
        AND yielded IS NULL
      ORDER BY consumed
      LIMIT 1
    ) r ON true
  ) r ON true;

CREATE OR REPLACE FUNCTION async.tail_once(
  _server_log_id BIGINT DEFAULT NULL,
  _grep TEXT DEFAULT NULL,
  _limit INT DEFAULT 1000) RETURNS SETOF async.server_log AS
$$
DECLARE
  l async.server_log;
  _pre_fetch_rows INT DEFAULT 100;
BEGIN
  _server_log_id := COALESCE(
    _server_log_id, 
    (
      SELECT min(server_log)
      FROM
      (
        SELECT server_log 
        FROM async.server_log
        ORDER BY server_log DESC
        LIMIT _pre_fetch_rows
      ) q
    ));

  RETURN QUERY SELECT * 
    FROM async.server_log 
    WHERE 
      server_log > _server_log_id
      AND (_grep IS NULL OR message LIKE '%' || _grep || '%')
    ORDER BY server_log LIMIT _limit;  
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE PROCEDURE async.tail(
  _grep TEXT DEFAULT NULL,
  _limit INT DEFAULT 1000) AS
$$
DECLARE
  l RECORD;
  _server_log_id BIGINT;
  _found BOOL;
BEGIN
  LOOP
    FOR l IN 
      SELECT * 
      FROM async.tail_once(_server_log_id, _grep, _limit)
    LOOP
      _found := true;
      RAISE NOTICE '[% - %]: %', l.happened::TIMESTAMPTZ(2), l.server_log, l.message;

      _server_log_id := l.server_log;
    END LOOP;

    IF NOT _found 
    THEN
      PERFORM pg_sleep(.1);
    END IF;

    COMMIT;

    _found := false;
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;

