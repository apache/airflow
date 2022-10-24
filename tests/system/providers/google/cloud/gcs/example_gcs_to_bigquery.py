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
DAG_ID = "gcs_to_bigquery_operator"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
TABLE_NAME = "test"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gcs"],
) as dag:
    create_test_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    # [START howto_operator_gcs_to_bigquery]
    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example",
        bucket="cloud-samples-data",
        source_objects=["bigquery/us-states/us-states.csv"],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    # [END howto_operator_gcs_to_bigquery]

    delete_test_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_airflow_test_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_test_dataset
        # TEST BODY
        >> load_csv
        # TEST TEARDOWN
        >> delete_test_dataset
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
