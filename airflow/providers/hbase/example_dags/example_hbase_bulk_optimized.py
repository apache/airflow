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
Example DAG demonstrating optimized HBase bulk operations.

This DAG showcases the new batch_size and max_workers parameters
for efficient bulk data processing with HBase.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.hbase.operators.hbase import (
    HBaseBatchPutOperator,
    HBaseCreateTableOperator,
    HBaseDeleteTableOperator,
    HBaseScanOperator,
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "example_hbase_bulk_optimized",
    default_args=default_args,
    description="Optimized HBase bulk operations example",
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "bulk", "optimized"],
)

TABLE_NAME = "bulk_test_table"
HBASE_CONN_ID = "hbase_thrift"

# Generate sample data
def generate_sample_rows(count: int, prefix: str) -> list[dict]:
    """Generate sample rows for testing."""
    return [
        {
            "row_key": f"{prefix}_{i:06d}",
            "cf1:name": f"User {i}",
            "cf1:age": str(20 + (i % 50)),
            "cf2:department": f"Dept {i % 10}",
            "cf2:salary": str(50000 + (i * 1000)),
        }
        for i in range(count)
    ]

# Cleanup
delete_table_cleanup = HBaseDeleteTableOperator(
    task_id="delete_table_cleanup",
    table_name=TABLE_NAME,
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Create table
create_table = HBaseCreateTableOperator(
    task_id="create_table",
    table_name=TABLE_NAME,
    families={
        "cf1": {"max_versions": 1},
        "cf2": {"max_versions": 1},
    },
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Small batch - single connection (ThriftStrategy)
small_batch = HBaseBatchPutOperator(
    task_id="small_batch_single_thread",
    table_name=TABLE_NAME,
    rows=generate_sample_rows(100, "small"),
    batch_size=200,
    max_workers=1,  # Single-threaded for ThriftStrategy
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Medium batch - connection pool (PooledThriftStrategy)
medium_batch = HBaseBatchPutOperator(
    task_id="medium_batch_pooled",
    table_name=TABLE_NAME,
    rows=generate_sample_rows(1000, "medium"),
    batch_size=200,
    max_workers=4,  # Multi-threaded with pool
    hbase_conn_id="hbase_pooled",  # Use pooled connection
    dag=dag,
)

# Large batch - connection pool optimized
large_batch = HBaseBatchPutOperator(
    task_id="large_batch_pooled",
    table_name=TABLE_NAME,
    rows=generate_sample_rows(5000, "large"),
    batch_size=150,  # Smaller batches for large datasets
    max_workers=6,   # More workers for large data
    hbase_conn_id="hbase_pooled",  # Use pooled connection
    dag=dag,
)

# Verify data
scan_results = HBaseScanOperator(
    task_id="scan_results",
    table_name=TABLE_NAME,
    limit=50,  # Just sample the results
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Cleanup
delete_table = HBaseDeleteTableOperator(
    task_id="delete_table",
    table_name=TABLE_NAME,
    hbase_conn_id=HBASE_CONN_ID,
    dag=dag,
)

# Dependencies
delete_table_cleanup >> create_table >> [small_batch, medium_batch, large_batch] >> scan_results >> delete_table