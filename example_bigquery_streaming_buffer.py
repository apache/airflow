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
Example DAG demonstrating BigQueryTableStreamingBufferEmptySensor usage.

This example shows how to safely perform DML operations on BigQuery tables
that receive streaming inserts.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableStreamingBufferEmptySensor

PROJECT_ID = "your-project-id"
DATASET_ID = "your_dataset"
TABLE_ID = "your_streaming_table"

with DAG(
    dag_id="example_bigquery_streaming_buffer_sensor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "bigquery", "streaming"],
) as dag:
    # Wait for streaming buffer to be empty before running DML
    wait_for_buffer_empty = BigQueryTableStreamingBufferEmptySensor(
        task_id="wait_for_streaming_buffer_empty",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Timeout after 1 hour
        mode="reschedule",  # Free up worker slots while waiting
    )

    # Run DML operation once buffer is empty
    run_dml = BigQueryInsertJobOperator(
        task_id="run_dml_operation",
        configuration={
            "query": {
                "query": f"""
                    UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
                    SET status = 'processed'
                    WHERE status = 'pending'
                """,
                "useLegacySql": False,
            }
        },
    )

    wait_for_buffer_empty >> run_dml
