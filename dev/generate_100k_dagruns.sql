/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
*/

-- SQL script to generate 100,000 completed DAG runs
-- This script creates DAG runs with state='success' (completed)
--
-- Requirements:
-- - Unique constraint on (dag_id, run_id)
-- - Unique constraint on (dag_id, logical_date)
-- - Foreign key to log_template.id
-- - All required non-nullable fields must be populated
--
-- Strategy: Spread 100k runs across 100 DAGs (1000 runs per DAG)
-- Each DAG gets unique logical_dates and run_ids
--
-- Database Compatibility: PostgreSQL (uses PostgreSQL-specific functions)
-- For MySQL/SQLite, modify TO_CHAR, LPAD, MOD, and INTERVAL syntax
--
-- Prerequisites:
-- - log_template table must exist and have at least one row
-- - Sufficient database permissions to INSERT into dag_run table
-- - Enough disk space for 100k rows
--
-- Usage:
--   psql -d airflow_db -f dev/generate_100k_dagruns.sql
--   or execute via SQL client connected to Airflow metadata database

-- Get the maximum log_template_id to use for foreign key reference
-- If log_template table is empty, we'll need to handle that separately
WITH log_template_max AS (
    SELECT COALESCE(MAX(id), 1) AS max_id FROM log_template
),
-- Generate 100,000 DAG runs spread across 100 DAGs (1000 runs per DAG)
dag_run_data AS (
    SELECT
        -- Calculate DAG index (0-99) and run index within DAG (0-999)
        (row_number() OVER () - 1) / 1000 AS dag_index,
        MOD((row_number() OVER () - 1), 1000) AS run_index_in_dag,
        -- DAG ID: spread across 100 DAGs (test_dag_001 to test_dag_100)
        'test_dag_' || LPAD(((row_number() OVER () - 1) / 1000 + 1)::text, 3, '0') AS dag_id,
        -- Run ID: scheduled__<iso_timestamp>__<sequence> format, ensures uniqueness per DAG
        'scheduled__' ||
        TO_CHAR(
            NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
            INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000),
            'YYYY-MM-DD"T"HH24:MI:SS'
        ) ||
        '__' ||
        LPAD(MOD((row_number() OVER () - 1), 1000)::text, 6, '0') AS run_id,
        -- Logical date: unique per DAG, each DAG gets 1000 unique dates spaced 1 day apart
        -- DAG 0: days 0-999, DAG 1: days 1000-1999, etc.
        (NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
         INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000))::timestamp AS logical_date,
        -- Run after: same as logical_date for scheduled runs
        (NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
         INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000))::timestamp AS run_after,
        -- Start date: logical_date + small offset (0-30 minutes)
        (NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
         INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000) +
         INTERVAL '1 minute' * MOD((row_number() OVER () - 1), 30))::timestamp AS start_date,
        -- End date: start_date + duration (5-60 minutes)
        (NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
         INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000) +
         INTERVAL '1 minute' * MOD((row_number() OVER () - 1), 30) +
         INTERVAL '1 minute' * (5 + MOD((row_number() OVER () - 1), 55)))::timestamp AS end_date,
        -- Data interval: same as logical_date for scheduled runs
        (NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
         INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000))::timestamp AS data_interval_start,
        (NOW() - INTERVAL '1 day' * MOD((row_number() OVER () - 1), 1000) -
         INTERVAL '1 day' * 1000 * ((row_number() OVER () - 1) / 1000) + INTERVAL '1 day')::timestamp AS data_interval_end,
        -- Row number for reference
        row_number() OVER () AS rn
    FROM generate_series(1, 100000)
)
INSERT INTO dag_run (
    dag_id,
    run_id,
    logical_date,
    run_after,
    start_date,
    end_date,
    state,
    run_type,
    data_interval_start,
    data_interval_end,
    log_template_id,
    updated_at,
    clear_number,
    span_status,
    queued_at
)
SELECT
    dr.dag_id,
    dr.run_id,
    dr.logical_date,
    dr.run_after,
    dr.start_date,
    dr.end_date,
    'success' AS state,  -- Completed state
    'scheduled' AS run_type,  -- Scheduled run type
    dr.data_interval_start,
    dr.data_interval_end,
    ltm.max_id AS log_template_id,  -- Use max log_template_id
    NOW() AS updated_at,
    0 AS clear_number,
    'NOT_STARTED' AS span_status,
    dr.start_date AS queued_at  -- Set queued_at same as start_date for completed runs
FROM dag_run_data dr
CROSS JOIN log_template_max ltm
ORDER BY dr.rn;

-- Verify the insert
SELECT
    COUNT(*) AS total_dagruns,
    COUNT(DISTINCT dag_id) AS unique_dags,
    state,
    run_type
FROM dag_run
WHERE dag_id LIKE 'test_dag_%'
GROUP BY state, run_type;
