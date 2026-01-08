
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


CREATE OR REPLACE FUNCTION async.tail(
  _server_log_id BIGINT DEFAULT NULL,
  _notify BOOL DEFAULT true,
  _limit INT DEFAULT 1000) RETURNS SETOF async.server_log AS
$$
DECLARE
  l async.server_log;
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
        LIMIT _limit
      ) q
    ));

  IF _notify
  THEN
    LOOP
      SELECT INTO l * 
      FROM async.server_log 
      WHERE server_log > _server_log_id
      ORDER BY server_log LIMIT 1;

      IF FOUND
      THEN
        RAISE NOTICE '%', l.message;
        _server_log_id = l.server_log;
      ELSE
        PERFORM pg_sleep(.1);
      END IF;
    END LOOP;
  ELSE
    RETURN QUERY SELECT * 
      FROM async.server_log 
      WHERE server_log > _server_log_id
      ORDER BY server_log LIMIT _limit;  
  END IF;
END;
$$ LANGUAGE PLPGSQL;




