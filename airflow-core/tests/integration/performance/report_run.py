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
Summarise the harness's runs from the metadata DB, per test case.

The test runs it at the end of each test case. It can also be run by hand inside breeze, before the
next run resets the database::

    python airflow-core/tests/integration/performance/report_run.py --dag-id perf_dag
"""

from __future__ import annotations

import argparse

from sqlalchemy import text

from airflow.utils.session import create_session

# Only task instances that ran to an end: a test case that hit its time limit leaves unfinished
# ones behind, and those have no duration to attribute.
FINISHED_TASK_INSTANCES = """
FROM task_instance
WHERE dag_id = ANY(:dag_ids)
  AND start_date IS NOT NULL AND end_date IS NOT NULL
"""

# The test names every run "<test case>__<n>", so the prefix is the test case.
OVERALL = f"""
SELECT
  split_part(run_id, '__', 1)                                             AS test_case,
  count(*)                                                                AS tis,
  extract(epoch FROM (max(end_date) - min(start_date)))                   AS drain_s,
  sum(extract(epoch FROM (end_date - start_date)))
    / nullif(extract(epoch FROM (max(end_date) - min(start_date))), 0)    AS mean_concurrency,
  avg(extract(epoch FROM (queued_dttm - scheduled_dttm)))                 AS avg_scheduled_s,
  avg(extract(epoch FROM (start_date - queued_dttm)))                     AS avg_queued_s,
  avg(extract(epoch FROM (end_date - start_date)))                        AS avg_running_s
{FINISHED_TASK_INSTANCES}
GROUP BY 1 ORDER BY min(start_date)
"""

# The most task instances running at once, from a running sum over start and end events.
PEAK_CONCURRENCY = f"""
WITH events AS (
  SELECT split_part(run_id, '__', 1) AS test_case, start_date AS at, 1 AS delta {FINISHED_TASK_INSTANCES}
  UNION ALL
  SELECT split_part(run_id, '__', 1) AS test_case, end_date AS at, -1 AS delta {FINISHED_TASK_INSTANCES}
),
running AS (
  SELECT test_case, sum(delta) OVER (PARTITION BY test_case ORDER BY at, delta ROWS UNBOUNDED PRECEDING) AS running
  FROM events
)
SELECT test_case, max(running) AS peak_concurrency FROM running GROUP BY test_case ORDER BY test_case
"""

BY_TASK = f"""
SELECT
  split_part(run_id, '__', 1)                                 AS test_case,
  task_id,
  count(*)                                                    AS tis,
  avg(extract(epoch FROM (queued_dttm - scheduled_dttm)))     AS avg_scheduled_s,
  avg(extract(epoch FROM (start_date - queued_dttm)))         AS avg_queued_s,
  avg(extract(epoch FROM (end_date - start_date)))            AS avg_running_s
{FINISHED_TASK_INSTANCES}
GROUP BY 1, 2 ORDER BY 1, min(start_date)
"""

REPORTS = (("OVERALL", OVERALL), ("PEAK CONCURRENCY", PEAK_CONCURRENCY), ("BY TASK", BY_TASK))


def format_value(value: object) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Summarise the performance harness's runs per test case.")
    parser.add_argument(
        "--dag-id", action="append", dest="dag_ids", help="Dag whose runs to report on; repeatable"
    )
    args = parser.parse_args(argv)
    dag_ids = args.dag_ids or ["perf_dag"]

    with create_session() as session:
        for title, sql in REPORTS:
            rows = session.execute(text(sql), {"dag_ids": dag_ids})
            print(f"\n=== {title} ===")
            print(" | ".join(rows.keys()))
            for row in rows:
                print(" | ".join(format_value(value) for value in row))


if __name__ == "__main__":
    main()
