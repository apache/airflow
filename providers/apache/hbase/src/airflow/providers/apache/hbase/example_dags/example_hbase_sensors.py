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
Example DAG demonstrating HBase sensors usage.

This DAG simulates a real-world scenario where:

#. External system creates a table with data
#. We wait for the table to appear (HBaseTableSensor)
#. We wait for specific row to be written (HBaseRowSensor)
#. We process the data once it's available
#. We clean up

Use case: Data ingestion pipeline where external ETL writes to HBase
and Airflow processes the data once it's ready.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.providers.apache.hbase.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG
else:
    from airflow import DAG
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

from airflow.providers.apache.hbase.hooks.hbase import HBaseThriftHook
from airflow.providers.apache.hbase.operators.hbase import HBaseDeleteTableOperator
from airflow.providers.apache.hbase.sensors.hbase import HBaseRowSensor, HBaseTableSensor

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

HBASE_CONN_ID = "hbase_thrift2"
TABLE_NAME = "external_data_table"
CONTROL_ROW = "data_ready"  # Marker row indicating data is complete


def simulate_external_system():
    """Simulate external system creating table and writing data."""

    hook = HBaseThriftHook(hbase_conn_id=HBASE_CONN_ID)

    # Create table (simulating external ETL)
    if not hook.table_exists(TABLE_NAME):
        hook.create_table(TABLE_NAME, {"cf1": {}, "cf2": {}})
        logger.info("External system created table: %s", TABLE_NAME)

    # Write some data rows
    for i in range(10):
        hook.put_row(
            TABLE_NAME,
            f"data_row_{i}",
            {
                "cf1:value": f"data_{i}",
                "cf2:timestamp": str(datetime.now()),
            },
        )
    logger.info("External system wrote 10 data rows")

    # Write control row to signal completion
    hook.put_row(
        TABLE_NAME,
        CONTROL_ROW,
        {
            "cf1:status": "complete",
            "cf1:row_count": "10",
        },
    )
    logger.info("External system wrote control row: %s", CONTROL_ROW)
    return "External system simulation complete"


def process_data():
    """Process data once it's available."""
    hook = HBaseThriftHook(hbase_conn_id=HBASE_CONN_ID)

    # Read control row to get metadata
    control_data = hook.get_row(TABLE_NAME, CONTROL_ROW)
    logger.info("Control row data: %s", control_data)

    # Scan and process data rows
    rows = hook.scan_table(TABLE_NAME, row_start="data_row_", row_stop="data_row_~")
    logger.info("Processing %d data rows:", len(rows))

    for row_key, data in rows:
        logger.info("  Processing %s: %s", row_key, data)

    logger.info("Data processing complete!")
    return f"Processed {len(rows)} rows"


with DAG(
    "example_hbase_sensors",
    default_args=default_args,
    description="Example DAG demonstrating HBase sensors for data availability",
    schedule=None,
    catchup=False,
    tags=["example", "hbase", "sensors"],
    doc_md=__doc__,
) as dag:
    # Step 1: Cleanup any existing table
    cleanup_existing = HBaseDeleteTableOperator(
        task_id="cleanup_existing_table",
        table_name=TABLE_NAME,
        hbase_conn_id=HBASE_CONN_ID,
    )

    # Step 2: Simulate external system writing data
    # In real scenario, this would be a separate DAG or external process
    simulate_external = PythonOperator(
        task_id="simulate_external_system",
        python_callable=simulate_external_system,
    )

    # Step 3: Wait for table to be created by external system
    # Timeout after 5 minutes, check every 10 seconds
    wait_for_table = HBaseTableSensor(
        task_id="wait_for_table",
        table_name=TABLE_NAME,
        hbase_conn_id=HBASE_CONN_ID,
        timeout=300,  # 5 minutes
        poke_interval=10,  # Check every 10 seconds
        mode="poke",  # Use poke mode (not reschedule)
    )

    # Step 4: Wait for control row indicating data is ready
    # This ensures all data has been written before processing
    wait_for_data_ready = HBaseRowSensor(
        task_id="wait_for_data_ready",
        table_name=TABLE_NAME,
        row_key=CONTROL_ROW,
        hbase_conn_id=HBASE_CONN_ID,
        timeout=300,
        poke_interval=10,
        mode="poke",
    )

    # Step 5: Process the data
    process = PythonOperator(
        task_id="process_data",
        python_callable=process_data,
    )

    # Step 6: Cleanup
    cleanup = HBaseDeleteTableOperator(
        task_id="cleanup_table",
        table_name=TABLE_NAME,
        hbase_conn_id=HBASE_CONN_ID,
    )

    # Define dependencies
    # In real scenario, simulate_external would be a separate DAG
    # and we'd only have:
    # wait_for_table >> wait_for_data_ready >> process >> cleanup
    (cleanup_existing >> simulate_external >> wait_for_table >> wait_for_data_ready >> process >> cleanup)
