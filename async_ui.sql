
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