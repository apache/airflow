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
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceAsyncSensor,
    BigQueryTableExistenceSensor,
    BigQueryTablePartitionExistenceSensor,
)
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "bigquery_sensors"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "")

TABLE_NAME = "partitioned_table"
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")

PARTITION_NAME = "{{ ds_nodash }}"

INSERT_ROWS_QUERY = f"INSERT {DATASET_NAME}.{TABLE_NAME} VALUES (42, '{{{{ ds }}}}')"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
    user_defined_macros={"DATASET": DATASET_NAME, "TABLE": TABLE_NAME},
    default_args={"project_id": PROJECT_ID},
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=SCHEMA,
        time_partitioning={
            "type": "DAY",
            "field": "ds",
        },
    )
    # [START howto_sensor_bigquery_table]
    check_table_exists: BaseOperator = BigQueryTableExistenceSensor(
        task_id="check_table_exists", project_id=PROJECT_ID, dataset_id=DATASET_NAME, table_id=TABLE_NAME
    )
    # [END howto_sensor_bigquery_table]

    # [START howto_sensor_async_bigquery_table]
    check_table_exists_async = BigQueryTableExistenceAsyncSensor(
        task_id="check_table_exists_async",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    # [END howto_sensor_async_bigquery_table]

    execute_insert_query: BaseOperator = BigQueryInsertJobOperator(
        task_id="execute_insert_query",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
    )

    # [START howto_sensor_bigquery_table_partition]
    check_table_partition_exists: BaseSensorOperator = BigQueryTablePartitionExistenceSensor(
        task_id="check_table_partition_exists",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        partition_id=PARTITION_NAME,
    )
    # [END howto_sensor_bigquery_table_partition]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_dataset >> create_table
    create_table >> [check_table_exists, execute_insert_query]
    execute_insert_query >> check_table_partition_exists
    [check_table_exists, check_table_exists_async, check_table_partition_exists] >> delete_dataset

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
