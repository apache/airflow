# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance

DAG = DagModel
DR = DagRun
TI = TaskInstance

DAG_TABLE = DagModel.__tablename__
DR_TABLE = DagRun.__tablename__
TI_TABLE = TaskInstance.__tablename__
POOL_TABLE = Pool.__tablename__

mysql_ti_selector_proc = f"""
CREATE PROCEDURE select_scheduled_tis_to_queue(IN max_tis INT)
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE rec_{TI.id.name} CHAR(36);
  DECLARE rec_{TI.dag_id.name} VARCHAR(250);
  DECLARE rec_{TI.task_id.name} VARCHAR(250);
  DECLARE rec_{TI.run_id.name} VARCHAR(250);
  DECLARE rec_{TI.pool.name} VARCHAR(256);
  DECLARE rec_{TI.max_active_tis_per_dag.name} INT;
  DECLARE rec_{TI.max_active_tis_per_dagrun.name} INT;
  DECLARE rec_{DAG.max_active_tasks.name} INT;
  DECLARE rec_{TI.pool_slots.name} INT;
  DECLARE rec_{Pool.slots.name} INT;

  DECLARE mapped_running INT;
  DECLARE dag_running INT;
  DECLARE dagrun_running INT;
  DECLARE pool_running INT;

  DECLARE task_counter INT DEFAULT 0;

  DECLARE tasks_for_scheduling_cur CURSOR FOR
    SELECT ti.{TI.id.name}, ti.{TI.dag_id.name}, ti.{TI.task_id.name}, ti.{TI.run_id.name}, ti.{TI.pool.name},
           ti.{TI.max_active_tis_per_dag.name}, ti.{TI.max_active_tis_per_dagrun.name}, dag.{DAG.max_active_tasks.name},
           ti.{TI.pool_slots.name}, pool.{Pool.slots.name}
    FROM {TI_TABLE} ti
    JOIN {DAG_TABLE} dag ON ti.{TI.dag_id.name} = dag.{DAG.dag_id.name}
    JOIN {DR_TABLE} dr ON dr.{DR.dag_id.name} = ti.{TI.dag_id.name} AND dr.{DR.run_id.name} = ti.{TI.run_id.name}
    JOIN {POOL_TABLE} pool ON ti.{TI.pool.name} = pool.{Pool.pool.name}
    WHERE dr.{DR.state.name} = 'running'
      AND NOT dag.{DAG.is_paused.name}
      AND ti.{TI.state.name} = 'scheduled'
      AND dag.{DAG.bundle_name.name} IS NOT NULL
    ORDER BY -ti.{TI.priority_weight.name}, dr.{DR.logical_date.name}, ti.{TI.map_index.name};

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  DROP TEMPORARY TABLE IF EXISTS mapped_counts;
  DROP TEMPORARY TABLE IF EXISTS dagrun_counts;
  DROP TEMPORARY TABLE IF EXISTS dag_counts;
  DROP TEMPORARY TABLE IF EXISTS pool_counts;
  DROP TEMPORARY TABLE IF EXISTS scheduled_tasks;

  -- Temporary tables to hold concurrency counts
  CREATE TEMPORARY TABLE mapped_counts (
    {TI.dag_id.name} VARCHAR(250),
    {TI.run_id.name} VARCHAR(250),
    {TI.task_id.name} VARCHAR(250),
    count INT DEFAULT 0,
    PRIMARY KEY (dag_id, run_id, task_id)
  ) ENGINE=MEMORY;

  CREATE TEMPORARY TABLE dagrun_counts (
    {TI.dag_id.name} VARCHAR(250),
    {TI.run_id.name} VARCHAR(250),
    count INT DEFAULT 0,
    PRIMARY KEY(dag_id, run_id)
  ) ENGINE=MEMORY;

  CREATE TEMPORARY TABLE dag_counts (
    {TI.dag_id.name} VARCHAR(250),
    {TI.task_id.name} VARCHAR(250),
    count INT DEFAULT 0,
    PRIMARY KEY(dag_id, task_id)
  ) ENGINE=MEMORY;

  CREATE TEMPORARY TABLE pool_counts (
    {TI.pool.name} VARCHAR(256) PRIMARY KEY,
    count INT DEFAULT 0
  ) ENGINE=MEMORY;

  -- Temporary table for scheduled tasks IDs
  CREATE TEMPORARY TABLE scheduled_tasks (
    id CHAR(36) PRIMARY KEY
  ) ENGINE=MEMORY;

  -- Initialize concurrency counts with currently running tasks
  INSERT INTO mapped_counts ({TI.dag_id.name}, {DR.run_id.name}, {TI.task_id.name}, count)
  SELECT {TI.dag_id.name}, {DR.run_id.name}, {TI.task_id.name}, COUNT(*)
  FROM {TI_TABLE}
  WHERE state IN ('queued', 'running')
  GROUP BY {TI.dag_id.name}, {DR.run_id.name}, {TI.task_id.name};

  INSERT INTO dagrun_counts ({TI.dag_id.name}, {TI.run_id.name}, count)
  SELECT {TI.dag_id.name}, {DR.run_id.name}, COUNT(*)
  FROM {TI_TABLE}
  WHERE state IN ('queued', 'running')
  GROUP BY {TI.dag_id.name}, {TI.run_id.name};

  INSERT INTO dag_counts ({TI.dag_id.name}, {TI.task_id.name}, count)
  SELECT {TI.dag_id.name}, {TI.task_id.name}, COUNT(*)
  FROM {TI_TABLE}
  WHERE state IN ('queued', 'running')
  GROUP BY {TI.dag_id.name}, {TI.task_id.name};

  INSERT INTO pool_counts ({TI.pool.name}, count)
  SELECT pool, COUNT(*)
  FROM {TI_TABLE}
  WHERE state IN ('queued', 'running', 'deferred')
  GROUP BY {TI.pool.name};


  OPEN tasks_for_scheduling_cur;

  read_loop: LOOP
    FETCH tasks_for_scheduling_cur INTO
      rec_{TI.id.name}, rec_{TI.dag_id.name}, rec_{TI.task_id.name}, rec_{TI.run_id.name}, rec_{TI.pool.name},
               rec_{TI.max_active_tis_per_dag.name}, rec_{TI.max_active_tis_per_dagrun.name}, rec_{DAG.max_active_tasks.name},
               rec_{TI.pool_slots.name}, rec_{Pool.slots.name};

      IF done THEN
      LEAVE read_loop;
    END IF;

    -- Check max tasks limit
    IF task_counter >= max_tis THEN
      LEAVE read_loop;
    END IF;

    -- Get current counts or zero if not present
    SET mapped_running = COALESCE((
      SELECT count
      FROM mapped_counts
      WHERE {TI.dag_id.name} = rec_{TI.dag_id.name}
        AND {DR.run_id.name} = rec_{DR.run_id.name}
        AND {TI.task_id.name} = rec_{TI.task_id.name}
    ), 0);

    SET dagrun_running = COALESCE((
      SELECT count
      FROM dagrun_counts
      WHERE {TI.dag_id.name} = rec_{TI.dag_id.name}
        AND {DR.run_id.name} = rec_{DR.run_id.name}
    ), 0);

    SET dag_running = COALESCE((
      SELECT count
      FROM dag_counts
      WHERE {TI.dag_id.name} = rec_{TI.dag_id.name}
        AND {TI.task_id.name} = rec_{TI.task_id.name}
    ), 0);

    SET pool_running = COALESCE((
      SELECT count
      FROM pool_counts
      WHERE {TI.pool.name} = rec_{TI.pool.name}
    ), 0);

    -- Apply concurrency limits
    IF rec_{TI.max_active_tis_per_dagrun.name} IS NOT NULL AND
    mapped_running + 1 > rec_{TI.max_active_tis_per_dagrun.name}
      THEN ITERATE read_loop;
    END IF;
    IF rec_{DAG.max_active_tasks.name} IS NOT NULL AND
    dagrun_running + 1 > rec_{DAG.max_active_tasks.name}
      THEN ITERATE read_loop;
    END IF;
    IF rec_{TI.max_active_tis_per_dag.name} IS NOT NULL AND
    dag_running + 1 > rec_{TI.max_active_tis_per_dag.name}
      THEN ITERATE read_loop;
    END IF;
    IF pool_running + rec_{TI.pool_slots.name} > rec_{Pool.slots.name}
      THEN ITERATE read_loop;
    END IF;

    -- Insert the scheduled task id into scheduled_tasks
    INSERT IGNORE INTO scheduled_tasks (id) VALUES (rec_{TI.id.name});
    SET task_counter = task_counter + 1;

    -- Increment concurrency counts
    INSERT INTO mapped_counts ({TI.dag_id.name}, {TI.run_id.name}, {TI.task_id.name}, count)
    VALUES (rec_{TI.dag_id.name}, rec_{TI.run_id.name}, rec_{TI.task_id.name}, 1)
    ON DUPLICATE KEY UPDATE count = mapped_running + 1;

    INSERT INTO dagrun_counts ({TI.dag_id.name}, {TI.run_id.name}, count)
    VALUES (rec_{TI.dag_id.name}, rec_{TI.run_id.name}, 1)
    ON DUPLICATE KEY UPDATE count = dagrun_running + 1;

    INSERT INTO dag_counts ({TI.dag_id.name}, {TI.task_id.name}, count)
    VALUES (rec_{TI.dag_id.name}, rec_{TI.task_id.name}, 1)
    ON DUPLICATE KEY UPDATE count = dag_running + 1;

    INSERT INTO pool_counts ({TI.pool.name}, count)
    VALUES (rec_{TI.pool.name}, rec_{TI.pool_slots.name})
    ON DUPLICATE KEY UPDATE count = pool_running + rec_{TI.pool_slots.name};

  END LOOP;

  CLOSE tasks_for_scheduling_cur;

  -- Return scheduled tasks's ids
  SELECT id from scheduled_tasks;

  DROP TEMPORARY TABLE mapped_counts;
  DROP TEMPORARY TABLE dagrun_counts;
  DROP TEMPORARY TABLE dag_counts;
  DROP TEMPORARY TABLE pool_counts;
  DROP TEMPORARY TABLE scheduled_tasks;
END;
"""


pgsql_ti_selector_proc = f"""
CREATE OR REPLACE FUNCTION select_scheduled_tis_to_queue(max_tis INTEGER)
RETURNS TABLE(id UUID)
AS $$
DECLARE
    rec RECORD;
    mapped_running INTEGER;
    dag_running INTEGER;
    dagrun_running INTEGER;
    pool_running INTEGER;
    tasks_for_scheduling UUID[] := '{{}}';

    dag_counts      hstore := ''::hstore;
    dagrun_counts   hstore := ''::hstore;
    pool_counts     hstore := ''::hstore;
    mapped_counts   hstore := ''::hstore;
BEGIN
    -- Compute concurrency maps
    SELECT COALESCE(hstore(array_agg(key), array_agg(value)), ''::hstore) INTO mapped_counts
    FROM (
        SELECT ({TI.dag_id.name} || ',' || {TI.run_id.name} || ',' || {TI.task_id.name}) AS key,
               count(*)::text AS value
        FROM {TI_TABLE}
        WHERE state IN ('queued', 'running')
        GROUP BY {TI.dag_id.name}, {TI.run_id.name}, {TI.task_id.name}
    ) sub;

    SELECT COALESCE(hstore(array_agg(key), array_agg(value)), ''::hstore) INTO dagrun_counts
    FROM (
        SELECT ({TI.dag_id.name} || ',' || {TI.run_id.name}) AS key,
               count(*)::text AS value
        FROM {TI_TABLE}
        WHERE state IN ('queued', 'running')
        GROUP BY {TI.dag_id.name}, {TI.run_id.name}
    ) sub;

    SELECT COALESCE(hstore(array_agg(key), array_agg(value)), ''::hstore) INTO dag_counts
    FROM (
        SELECT ({TI.dag_id.name} || ',' || {TI.task_id.name}) AS key,
               count(*)::text AS value
        FROM {TI_TABLE}
        WHERE state IN ('queued', 'running')
        GROUP BY {TI.dag_id.name}, {TI.task_id.name}
    ) sub;

    SELECT COALESCE(hstore(array_agg(pool), array_agg(value)), ''::hstore) INTO pool_counts
    FROM (
        SELECT pool,
               count(*)::text AS value
        FROM {TI_TABLE}
        WHERE state IN ('queued', 'running', 'deferred')
        GROUP BY {TI.pool.name}
    ) sub;

    -- Fire base query
    FOR rec IN
        SELECT ti.{TI.id.name}, ti.{TI.dag_id.name}, ti.{TI.task_id.name}, ti.{TI.run_id.name}, ti.{TI.pool.name},
           ti.{TI.max_active_tis_per_dag.name}, ti.{TI.max_active_tis_per_dagrun.name}, dag.{DAG.max_active_tasks.name},
           ti.{TI.pool_slots.name}, pool.{Pool.slots.name}
        FROM {TI_TABLE} ti
        JOIN {DAG_TABLE} dag ON ti.{TI.dag_id.name} = dag.{DAG.dag_id.name}
        JOIN {DR_TABLE} dr ON dr.{DR.dag_id.name} = ti.{TI.dag_id.name} AND dr.{DR.run_id.name} = ti.{TI.run_id.name}
        JOIN {POOL_TABLE} pool ON ti.{TI.pool.name} = pool.{Pool.pool.name}
        WHERE dr.{DR.state.name} = 'running'
          AND NOT dag.{DAG.is_paused.name}
          AND ti.{TI.state.name} = 'scheduled'
          AND dag.{DAG.bundle_name.name} IS NOT NULL
        ORDER BY -ti.{TI.priority_weight.name}, dr.{DR.logical_date.name}, ti.{TI.map_index.name}
    LOOP
        IF array_length(tasks_for_scheduling, 1) >= max_tis THEN
            EXIT;
        END IF;

        mapped_running := COALESCE((mapped_counts -> (rec.{TI.dag_id.name}
                                                    || ',' || rec.{TI.run_id.name}
                                                    || ',' || rec.{TI.task_id.name})::text)::int, 0);
        dagrun_running := COALESCE((dagrun_counts -> (rec.{TI.dag_id.name}
                                                    || ',' || rec.{TI.run_id.name})::text)::int, 0);
        dag_running := COALESCE((dag_counts -> (rec.{TI.dag_id.name}
                                                    || ',' || rec.{TI.task_id.name})::text)::int, 0);
        pool_running := COALESCE((pool_counts -> rec.{TI.pool.name}::text)::int, 0);

        -- Compare limits including newly scheduling tasks (hstores maintain increments)
        IF rec.{TI.max_active_tis_per_dagrun.name} IS NOT NULL AND
        (mapped_running + 1) > rec.{TI.max_active_tis_per_dagrun.name}
            THEN CONTINUE;
        ELSIF rec.{TI.max_active_tis_per_dag.name} IS NOT NULL AND
        (dag_running + 1) > rec.{TI.max_active_tis_per_dag.name}
            THEN CONTINUE;
        ELSIF rec.{DAG.max_active_tasks.name} IS NOT NULL AND
        (dagrun_running + 1) > rec.{DAG.max_active_tasks.name}
            THEN CONTINUE;
        ELSIF (pool_running + rec.{TI.pool_slots.name}) > rec.{Pool.slots.name}
            THEN CONTINUE;
        END IF;

        tasks_for_scheduling := array_append(tasks_for_scheduling, rec.id);

        mapped_counts := mapped_counts || hstore(
            (rec.{TI.dag_id.name} || ',' || rec.{TI.run_id.name} || ',' || rec.{TI.task_id.name})::text, (mapped_running + 1)::text
        );
        dag_counts := dag_counts || hstore(
            (rec.{TI.dag_id.name} || ',' || rec.{TI.run_id.name})::text, (dag_running + 1)::text
        );
        dagrun_counts := dagrun_counts || hstore(
            (rec.{TI.dag_id.name}  || ',' || rec.{TI.task_id.name})::text, (dagrun_running + 1)::text
        );
        pool_counts := pool_counts || hstore(
            rec.pool::text, (pool_running + rec.{TI.pool_slots.name})::text
        );

    END LOOP;
    RETURN QUERY SELECT UNNEST(tasks_for_scheduling);

END;
$$ LANGUAGE plpgsql;
"""

mysql_ti_selector_proc_drop = """
DROP PROCEDURE IF EXISTS select_scheduled_tis_to_queue;
"""

pgsql_ti_selector_proc_drop = """
DROP FUNCTION IF EXISTS select_scheduled_tis_to_queue(INTEGER);
"""

pgsql_hstore_extension = """
CREATE EXTENSION IF NOT EXISTS hstore;
"""

pgsql_hstore_extension_drop = """
DROP EXTENSION IF EXISTS hstore;
"""
