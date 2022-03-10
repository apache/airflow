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
Example Airflow DAG for Google BigQuery service testing data structures with location.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from tests.system.utils.watcher import watcher

ENV_ID = os.environ["SYSTEM_TESTS_ENV_ID"]
DAG_ID = "bigquery_operations_location"

BQ_LOCATION = "europe-north1"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"


with models.DAG(
    DAG_ID,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_dataset_with_location = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_with_location",
        dataset_id=DATASET_NAME,
        location=BQ_LOCATION,
    )

    create_table_with_location = BigQueryCreateEmptyTableOperator(
        task_id="create_table_with_location",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    delete_dataset_with_location = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset_with_location",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST BODY
        create_dataset_with_location
        >> create_table_with_location
        # TEST TEARDOWN
        >> delete_dataset_with_location
    )

    list(dag.tasks) >> watcher()


def test_run():
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run()
