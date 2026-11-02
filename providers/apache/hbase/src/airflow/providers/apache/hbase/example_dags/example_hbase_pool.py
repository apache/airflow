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
Example DAG demonstrating Thrift2 connection pool performance benefits.

This DAG compares single connection vs pooled connection performance
for large-scale batch operations.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from airflow.providers.apache.hbase.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG
else:
    from airflow import DAG
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

from airflow.providers.apache.hbase.hooks.hbase import HBaseThriftHook

logger = logging.getLogger(__name__)

# Connection IDs
# Single connection: no pool
# Pooled connection: {"connection_pool": {"enabled": true, "size": 10}}
SINGLE_CONN_ID = "hbase_thrift2"
POOLED_CONN_ID = "hbase_thrift2_pooled"
TABLE_NAME = "perf_test_table"


def setup_table(conn_id: str):
    """Create test table."""
    hook = HBaseThriftHook(hbase_conn_id=conn_id)

    # Delete if exists
    if hook.table_exists(TABLE_NAME):
        hook.delete_table(TABLE_NAME)

    # Create table
    families: dict[str, dict] = {"cf1": {}, "cf2": {}, "cf3": {}}
    hook.create_table(TABLE_NAME, families)
    logger.info("Created table: %s", TABLE_NAME)
    return f"Table {TABLE_NAME} created"


def benchmark_single_connection():
    """Benchmark with single Thrift2 connection."""
    hook = HBaseThriftHook(hbase_conn_id=SINGLE_CONN_ID)

    try:
        # Generate 10,000 rows
        rows = []
        for i in range(10000):
            rows.append(
                {
                    "row_key": f"single_{i:06d}",
                    "cf1:col1": f"value1_{i}",
                    "cf1:col2": f"value2_{i}",
                    "cf2:col1": f"value3_{i}",
                    "cf2:col2": f"value4_{i}",
                    "cf3:col1": f"value5_{i}",
                }
            )

        start = time.time()
        hook.batch_put_rows(TABLE_NAME, rows, batch_size=200, max_workers=1)
        elapsed = time.time() - start

        logger.info(
            "Single connection: %d rows in %.2fs (%.0f rows/sec)",
            len(rows),
            elapsed,
            len(rows) / elapsed,
        )
        return elapsed
    finally:
        hook.close()


def benchmark_pooled_connection():
    """Benchmark with Thrift2 connection pool."""
    hook = HBaseThriftHook(hbase_conn_id=POOLED_CONN_ID)

    # Generate 10,000 rows
    rows = []
    for i in range(10000):
        rows.append(
            {
                "row_key": f"pooled_{i:06d}",
                "cf1:col1": f"value1_{i}",
                "cf1:col2": f"value2_{i}",
                "cf2:col1": f"value3_{i}",
                "cf2:col2": f"value4_{i}",
                "cf3:col1": f"value5_{i}",
            }
        )

    start = time.time()
    # Use 4 workers (less than pool size of 10)
    hook.batch_put_rows(TABLE_NAME, rows, batch_size=200, max_workers=4)
    elapsed = time.time() - start

    logger.info(
        "Pooled connection (4 workers): %d rows in %.2fs (%.0f rows/sec)",
        len(rows),
        elapsed,
        len(rows) / elapsed,
    )
    return elapsed


def benchmark_large_dataset():
    """Benchmark with very large dataset using pool."""
    hook = HBaseThriftHook(hbase_conn_id=POOLED_CONN_ID)

    # Generate 50,000 rows
    rows = []
    for i in range(50000):
        rows.append(
            {
                "row_key": f"large_{i:06d}",
                "cf1:col1": f"value1_{i}",
                "cf1:col2": f"value2_{i}",
                "cf2:col1": f"value3_{i}",
                "cf2:col2": f"value4_{i}",
                "cf3:col1": f"value5_{i}",
            }
        )

    start = time.time()
    # Use 6 workers (less than pool size of 10)
    hook.batch_put_rows(TABLE_NAME, rows, batch_size=250, max_workers=6)
    elapsed = time.time() - start

    logger.info(
        "Large dataset (6 workers): %d rows in %.2fs (%.0f rows/sec)",
        len(rows),
        elapsed,
        len(rows) / elapsed,
    )
    return elapsed


def verify_data():
    """Verify inserted data."""
    hook = HBaseThriftHook(hbase_conn_id=POOLED_CONN_ID)

    # Scan samples from each benchmark
    results = hook.scan_table(TABLE_NAME, row_start="single_000000", row_stop="single_000010")
    logger.info("Single connection sample: %d rows", len(results))

    results = hook.scan_table(TABLE_NAME, row_start="pooled_000000", row_stop="pooled_000010")
    logger.info("Pooled connection sample: %d rows", len(results))

    results = hook.scan_table(TABLE_NAME, row_start="large_000000", row_stop="large_000010")
    logger.info("Large dataset sample: %d rows", len(results))
    return "Verification complete"


def cleanup_table():
    """Delete test table."""
    hook = HBaseThriftHook(hbase_conn_id=POOLED_CONN_ID)
    hook.delete_table(TABLE_NAME)
    logger.info("Deleted table: %s", TABLE_NAME)
    return f"Table {TABLE_NAME} deleted"


with DAG(
    dag_id="example_hbase_pool",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "pool"],
    doc_md=__doc__,
) as dag:
    setup = PythonOperator(
        task_id="setup_table",
        python_callable=setup_table,
        op_kwargs={"conn_id": SINGLE_CONN_ID},
    )

    bench_single = PythonOperator(
        task_id="benchmark_single_connection",
        python_callable=benchmark_single_connection,
    )

    bench_pooled = PythonOperator(
        task_id="benchmark_pooled_connection",
        python_callable=benchmark_pooled_connection,
    )

    bench_large = PythonOperator(
        task_id="benchmark_large_dataset",
        python_callable=benchmark_large_dataset,
    )

    verify = PythonOperator(
        task_id="verify_data",
        python_callable=verify_data,
    )

    cleanup = PythonOperator(
        task_id="cleanup_table",
        python_callable=cleanup_table,
    )

    (setup >> [bench_single, bench_pooled] >> bench_large >> verify >> cleanup)
