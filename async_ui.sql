
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
      RAISE NOTICE '[%]: %', l.server_log, l.message;

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

