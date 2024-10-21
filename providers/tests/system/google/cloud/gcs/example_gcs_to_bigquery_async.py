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

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "gcs_to_bigquery_operator_async"

DATASET_NAME_STR = f"dataset_{DAG_ID}_{ENV_ID}_STR"
DATASET_NAME_DATE = f"dataset_{DAG_ID}_{ENV_ID}_DATE"
DATASET_NAME_JSON = f"dataset_{DAG_ID}_{ENV_ID}_JSON"
DATASET_NAME_DELIMITER = f"dataset_{DAG_ID}_{ENV_ID}_DELIMITER"
TABLE_NAME_STR = "test_str"
TABLE_NAME_DATE = "test_date"
TABLE_NAME_JSON = "test_json"
TABLE_NAME_DELIMITER = "test_delimiter"
MAX_ID_STR = "name"
MAX_ID_DATE = "date"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gcs"],
) as dag:
    create_test_dataset_for_string_fields = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset_str", dataset_id=DATASET_NAME_STR, project_id=PROJECT_ID
    )

    create_test_dataset_for_date_fields = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset_date", dataset_id=DATASET_NAME_DATE, project_id=PROJECT_ID
    )

    create_test_dataset_for_json_fields = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset_json", dataset_id=DATASET_NAME_JSON, project_id=PROJECT_ID
    )

    create_test_dataset_for_delimiter_fields = BigQueryCreateEmptyDatasetOperator(
        task_id="create_airflow_test_dataset_delimiter",
        dataset_id=DATASET_NAME_DELIMITER,
        project_id=PROJECT_ID,
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
        max_id_key="string_field_0",
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

    load_json = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example_date_json_async",
        bucket="cloud-samples-data",
        source_objects=["bigquery/us-states/us-states.json"],
        source_format="NEWLINE_DELIMITED_JSON",
        destination_project_dataset_table=f"{DATASET_NAME_JSON}.{TABLE_NAME_JSON}",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        max_id_key=MAX_ID_STR,
        deferrable=True,
    )

    load_csv_delimiter = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example_delimiter_async",
        bucket="big-query-samples",
        source_objects=["employees-tabular.csv"],
        source_format="csv",
        destination_project_dataset_table=f"{DATASET_NAME_DELIMITER}.{TABLE_NAME_DELIMITER}",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        field_delimiter="\t",
        quote_character="",
        max_id_key=MAX_ID_STR,
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

    delete_test_dataset_json = BigQueryDeleteDatasetOperator(
        task_id="delete_airflow_test_json_dataset",
        dataset_id=DATASET_NAME_JSON,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_test_dataset_delimiter = BigQueryDeleteDatasetOperator(
        task_id="delete_airflow_test_delimiter",
        dataset_id=DATASET_NAME_JSON,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_test_dataset_for_string_fields
        >> create_test_dataset_for_date_fields
        >> create_test_dataset_for_json_fields
        >> create_test_dataset_for_delimiter_fields
        # TEST BODY
        >> load_string_based_csv
        >> load_date_based_csv
        >> load_json
        >> load_csv_delimiter
        # TEST TEARDOWN
        >> delete_test_dataset_str
        >> delete_test_dataset_date
        >> delete_test_dataset_json
        >> delete_test_dataset_delimiter
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
