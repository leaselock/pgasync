
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





