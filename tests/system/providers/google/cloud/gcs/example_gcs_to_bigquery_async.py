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
Example DAG using GCSToBigQueryOperator.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "gcs_to_bigquery_operator_async"

DATASET_NAME_STR = f"dataset_{DAG_ID}_{ENV_ID}_STR"
DATASET_NAME_DATE = f"dataset_{DAG_ID}_{ENV_ID}_DATE"
TABLE_NAME_STR = "test_str"
TABLE_NAME_DATE = "test_date"
MAX_ID_STR = "name"
MAX_ID_DATE = "date"

with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gcs"],
) as dag:
    create_test_dataset_for_string_fileds = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset_str", dataset_id=DATASET_NAME_STR, project_id=PROJECT_ID
    )

    create_test_dataset_for_date_fileds = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset_date", dataset_id=DATASET_NAME_DATE, project_id=PROJECT_ID
    )

    # [START howto_operator_gcs_to_bigquery_async]
    load_string_based_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example_str_csv_async",
        bucket="cloud-samples-data",
        source_objects=["bigquery/us-states/us-states.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_STR}.{TABLE_NAME_STR}",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        max_id_key=MAX_ID_STR,
        deferrable=True,
    )

    load_date_based_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example_date_csv_async",
        bucket="cloud-samples-data",
        source_objects=["bigquery/us-states/us-states-by-date.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_DATE}.{TABLE_NAME_DATE}",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        max_id_key=MAX_ID_DATE,
        deferrable=True,
    )
    # [END howto_operator_gcs_to_bigquery_async]

    delete_test_dataset_str = BigQueryDeleteDatasetOperator(
        task_id="delete_airflow_test_str_dataset",
        dataset_id=DATASET_NAME_STR,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_test_dataset_date = BigQueryDeleteDatasetOperator(
        task_id="delete_airflow_test_date_dataset",
        dataset_id=DATASET_NAME_DATE,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_test_dataset_for_string_fileds
        >> create_test_dataset_for_date_fileds
        # TEST BODY
        >> load_string_based_csv
        >> load_date_based_csv
        # TEST TEARDOWN
        >> delete_test_dataset_str
        >> delete_test_dataset_date
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
