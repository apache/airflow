#
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

"""
Add stored procedures for task selection

Revision ID: 28abf6d28d2f
Revises: 2f49f2dae90c
Create Date: 2025-09-12 23:20:39.502229

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "28abf6d28d2f"
down_revision = "2f49f2dae90c"
branch_labels = None
depends_on = None
airflow_version = "3.2.0"


mysql_ti_selector_proc = """
CREATE PROCEDURE select_scheduled_tis_to_queue(IN max_tis INT)
BEGIN
  DECLARE done INT DEFAULT FALSE;
  DECLARE rec_id CHAR(36);
  DECLARE rec_dag_id VARCHAR(250);
  DECLARE rec_task_id VARCHAR(250);
  DECLARE rec_run_id VARCHAR(250);
  DECLARE rec_pool VARCHAR(256);
  DECLARE rec_max_active_tis_per_dag INT;
  DECLARE rec_max_active_tis_per_dagrun INT;
  DECLARE rec_max_active_tasks INT;
  DECLARE rec_pool_slots INT;
  DECLARE rec_slots INT;

  DECLARE mapped_running INT;
  DECLARE dag_running INT;
  DECLARE dagrun_running INT;
  DECLARE pool_running INT;

  DECLARE task_counter INT DEFAULT 0;

  DECLARE tasks_for_scheduling_cur CURSOR FOR
    SELECT ti.id, ti.dag_id, ti.task_id, ti.run_id, ti.pool,
           ti.max_active_tis_per_dag, ti.max_active_tis_per_dagrun,
           dag.max_active_tasks, ti.pool_slots, slot_pool.slots
    FROM task_instance ti
    JOIN dag_run ON dag_run.dag_id = ti.dag_id AND dag_run.run_id = ti.run_id
    JOIN dag ON ti.dag_id = dag.dag_id
    JOIN slot_pool ON ti.pool = slot_pool.pool
    WHERE dag_run.state = 'running'
      AND NOT dag.is_paused
      AND ti.state = 'scheduled'
      AND dag.bundle_name IS NOT NULL
    ORDER BY ti.priority_weight DESC, dag_run.logical_date, ti.map_index;

  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

  DROP TEMPORARY TABLE IF EXISTS mapped_counts;
  DROP TEMPORARY TABLE IF EXISTS dagrun_counts;
  DROP TEMPORARY TABLE IF EXISTS dag_counts;
  DROP TEMPORARY TABLE IF EXISTS pool_counts;
  DROP TEMPORARY TABLE IF EXISTS scheduled_tasks;

  -- Temporary tables to hold concurrency counts
  CREATE TEMPORARY TABLE mapped_counts (
    dag_id VARCHAR(250),
    run_id VARCHAR(250),
    task_id VARCHAR(250),
    count INT DEFAULT 0,
    PRIMARY KEY (dag_id, run_id, task_id)
  ) ENGINE=MEMORY;

  CREATE TEMPORARY TABLE dagrun_counts (
    dag_id VARCHAR(250),
    run_id VARCHAR(250),
    count INT DEFAULT 0,
    PRIMARY KEY(dag_id, run_id)
  ) ENGINE=MEMORY;

  CREATE TEMPORARY TABLE dag_counts (
    dag_id VARCHAR(250),
    task_id VARCHAR(250),
    count INT DEFAULT 0,
    PRIMARY KEY(dag_id, task_id)
  ) ENGINE=MEMORY;

  CREATE TEMPORARY TABLE pool_counts (
    pool VARCHAR(256) PRIMARY KEY,
    count INT DEFAULT 0
  ) ENGINE=MEMORY;

  -- Temporary table for scheduled tasks IDs
  CREATE TEMPORARY TABLE scheduled_tasks (
    id CHAR(36) PRIMARY KEY
  ) ENGINE=MEMORY;

  -- Initialize concurrency counts with currently running tasks
  INSERT INTO mapped_counts (dag_id, run_id, task_id, count)
  SELECT dag_id, run_id, task_id, COUNT(*)
  FROM task_instance
  WHERE state IN ('queued', 'running')
  GROUP BY dag_id, run_id, task_id;

  INSERT INTO dagrun_counts (dag_id, run_id, count)
  SELECT dag_id, run_id, COUNT(*)
  FROM task_instance
  WHERE state IN ('queued', 'running')
  GROUP BY dag_id, run_id;

  INSERT INTO dag_counts (dag_id, task_id, count)
  SELECT dag_id, task_id, COUNT(*)
  FROM task_instance
  WHERE state IN ('queued', 'running')
  GROUP BY dag_id, task_id;

  INSERT INTO pool_counts (pool, count)
  SELECT pool, COUNT(*)
  FROM task_instance
  WHERE state IN ('queued', 'running', 'deferred')
  GROUP BY pool;


  OPEN tasks_for_scheduling_cur;

  read_loop: LOOP
    FETCH tasks_for_scheduling_cur INTO
      rec_id, rec_dag_id, rec_task_id, rec_run_id, rec_pool,
      rec_max_active_tis_per_dag, rec_max_active_tis_per_dagrun,
      rec_max_active_tasks, rec_pool_slots, rec_slots;

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
      WHERE dag_id = rec_dag_id
        AND run_id = rec_run_id
        AND task_id = rec_task_id
    ), 0);

    SET dagrun_running = COALESCE((
      SELECT count
      FROM dagrun_counts
      WHERE dag_id = rec_dag_id
        AND run_id = rec_run_id
    ), 0);

    SET dag_running = COALESCE((
      SELECT count
      FROM dag_counts
      WHERE dag_id = rec_dag_id
        AND task_id = rec_task_id
    ), 0);

    SET pool_running = COALESCE((
      SELECT count
      FROM pool_counts
      WHERE pool = rec_pool
    ), 0);


    -- Apply concurrency limits
    IF mapped_running + 1 > COALESCE(rec_max_active_tis_per_dagrun, max_tis) THEN
      ITERATE read_loop;
    END IF;

    IF dagrun_running + 1 > COALESCE(rec_max_active_tasks, max_tis) THEN
      ITERATE read_loop;
    END IF;

    IF dag_running + 1 > COALESCE(rec_max_active_tis_per_dag, max_tis) THEN
      ITERATE read_loop;
    END IF;

    IF pool_running + rec_pool_slots > rec_slots THEN
      ITERATE read_loop;
    END IF;

    -- Insert the scheduled task id into scheduled_tasks
    INSERT IGNORE INTO scheduled_tasks (id) VALUES (rec_id);
    SET task_counter = task_counter + 1;

    -- Increment concurrency counts
    INSERT INTO mapped_counts (dag_id, run_id, task_id, count)
    VALUES (rec_dag_id, rec_run_id, rec_task_id, 1)
    ON DUPLICATE KEY UPDATE count = mapped_running + 1;

    INSERT INTO dagrun_counts (dag_id, run_id, count)
    VALUES (rec_dag_id, rec_run_id, 1)
    ON DUPLICATE KEY UPDATE count = dagrun_running + 1;

    INSERT INTO dag_counts (dag_id, task_id, count)
    VALUES (rec_dag_id, rec_task_id, 1)
    ON DUPLICATE KEY UPDATE count = dag_running + 1;

    INSERT INTO pool_counts (pool, count)
    VALUES (rec_pool, rec_pool_slots)
    ON DUPLICATE KEY UPDATE count = pool_running + rec_pool_slots;

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

mysql_ti_selector_proc_drop = """
DROP PROCEDURE IF EXISTS select_scheduled_tis_to_queue;
"""


def _mysql_upgrade():
    conn = op.get_bind()
    conn.execute(text(mysql_ti_selector_proc_drop))
    conn.execute(text(mysql_ti_selector_proc))


def _mysql_downgrade():
    conn = op.get_bind()
    conn.execute(text(mysql_ti_selector_proc_drop))


_UPGRADE_BY_DIALECT = {
    "mysql": _mysql_upgrade,
}

_DOWNGRADE_BY_DIALECT = {
    "mysql": _mysql_downgrade,
}


def upgrade():
    """Apply Add stored procedures for task selection."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    upgrade_fn = _UPGRADE_BY_DIALECT.get(dialect_name)
    if upgrade_fn:
        upgrade_fn()


def downgrade():
    """Unapply Add stored procedures for task selection."""
    conn = op.get_bind()
    dialect_name = conn.dialect.name
    downgrade_fn = _DOWNGRADE_BY_DIALECT.get(dialect_name)
    if downgrade_fn:
        downgrade_fn()
