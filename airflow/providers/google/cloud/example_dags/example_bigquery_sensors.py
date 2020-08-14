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
Example Airflow DAG for Google BigQuery Sensors.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryDeleteDatasetOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor, BigQueryTablePartitionExistenceSensor,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_sensors_dataset")

TABLE_NAME = "partitioned_table"
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")

PARTITION_NAME = "{{ ds_nodash }}"

INSERT_ROWS_QUERY = \
    f"INSERT {DATASET_NAME}.{TABLE_NAME} VALUES " \
    "(42, '{{ ds }}')"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]

dag_id = "example_bigquery_sensors"

with models.DAG(
    dag_id,
    schedule_interval=None,  # Override to match your needs
    start_date=days_ago(1),
    tags=["example"],
    user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_NAME},
    default_args={"project_id": PROJECT_ID}
) as dag_with_locations:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=SCHEMA,
        time_partitioning={
            "type": "DAY",
            "field": "ds",
        }
    )
    # [START howto_sensor_bigquery_table]
    check_table_exists = BigQueryTableExistenceSensor(
        task_id="check_table_exists", project_id=PROJECT_ID, dataset_id=DATASET_NAME, table_id=TABLE_NAME
    )
    # [END howto_sensor_bigquery_table]

    execute_insert_query = BigQueryExecuteQueryOperator(
        task_id="execute_insert_query", sql=INSERT_ROWS_QUERY, use_legacy_sql=False
    )

    # [START howto_sensor_bigquery_table_partition]
    check_table_partition_exists = BigQueryTablePartitionExistenceSensor(
        task_id="check_table_partition_exists", project_id=PROJECT_ID, dataset_id=DATASET_NAME,
        table_id=TABLE_NAME, partition_id=PARTITION_NAME
    )
    # [END howto_sensor_bigquery_table_partition]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )

    create_dataset >> create_table
    create_table >> check_table_exists
    create_table >> execute_insert_query
    execute_insert_query >> check_table_partition_exists
    check_table_exists >> delete_dataset
    check_table_partition_exists >> delete_dataset
