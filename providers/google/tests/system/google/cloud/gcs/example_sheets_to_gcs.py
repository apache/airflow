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
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from airflow.models.dag import DAG

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.sheets_to_gcs import GoogleSheetsToGCSOperator
from airflow.providers.google.suite.operators.sheets import GoogleSheetsCreateSpreadsheetOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google.gcp_api_client_helpers import (
    create_airflow_connection,
    delete_airflow_connection,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "sheets_to_gcs"

IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"

log = logging.getLogger(__name__)

with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "sheets"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    @task
    def create_connection(connection_id: str):
        conn_extra = {
            "scope": "https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/cloud-platform",
            "project": PROJECT_ID,
            "keyfile_dict": "",  # Override to match your needs
        }
        conn_extra_json = json.dumps(conn_extra)
        connection: dict[str, Any] = {"conn_type": "google_cloud_platform", "extra": conn_extra_json}
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf=connection,
            is_composer=IS_COMPOSER,
        )

    create_connection_task = create_connection(connection_id=CONNECTION_ID)

    create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
        task_id="create_spreadsheet", spreadsheet=SPREADSHEET, gcp_conn_id=CONNECTION_ID
    )

    # [START upload_sheet_to_gcs]
    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET_NAME,
        spreadsheet_id="{{ task_instance.xcom_pull(task_ids='create_spreadsheet', key='spreadsheet_id') }}",
        gcp_conn_id=CONNECTION_ID,
    )
    # [END upload_sheet_to_gcs]

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection(connection_id=CONNECTION_ID)

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [create_bucket, create_connection_task]
        >> create_spreadsheet
        # TEST BODY
        >> upload_sheet_to_gcs
        # TEST TEARDOWN
        >> [delete_bucket, delete_connection_task]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
