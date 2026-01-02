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
Example DAG using GoogleDriveToLocalOperator.

Using this operator requires the following additional scopes:
https://www.googleapis.com/auth/drive
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor
from airflow.providers.google.suite.transfers.gcs_to_gdrive import GCSToGoogleDriveOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from airflow.providers.google.tests.system.google.gcp_api_client_helpers import (
    create_airflow_connection,
    delete_airflow_connection,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "gdrive_to_local"

IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
FILE_NAME = "empty.txt"
DRIVE_FILE_NAME = f"empty_{DAG_ID}_{ENV_ID}.txt".replace("-", "_")
OUTPUT_FILE = "out_file.txt"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

LOCAL_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)

log = logging.getLogger(__name__)

with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "gdrive"],
) as dag:

    @task
    def create_connection(connection_id: str):
        conn_extra_json = json.dumps(
            {"scope": "https://www.googleapis.com/auth/drive,https://www.googleapis.com/auth/cloud-platform"}
        )
        connection: dict[str, Any] = {"conn_type": "google_cloud_platform", "extra": conn_extra_json}
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf=connection,
            is_composer=IS_COMPOSER,
        )

    create_connection_task = create_connection(connection_id=CONNECTION_ID)

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )

    copy_single_file = GCSToGoogleDriveOperator(
        task_id="copy_single_file",
        gcp_conn_id=CONNECTION_ID,
        source_bucket=BUCKET_NAME,
        source_object=FILE_NAME,
        destination_object=DRIVE_FILE_NAME,
    )

    # [START detect_file]
    detect_file = GoogleDriveFileExistenceSensor(
        task_id="detect_file",
        gcp_conn_id=CONNECTION_ID,
        folder_id="",
        file_name=DRIVE_FILE_NAME,
    )
    # [END detect_file]

    # [START download_from_gdrive_to_local]
    download_from_gdrive_to_local = GoogleDriveToLocalOperator(
        task_id="download_from_gdrive_to_local",
        gcp_conn_id=CONNECTION_ID,
        folder_id="",
        file_name=DRIVE_FILE_NAME,
        output_file=OUTPUT_FILE,
    )
    # [END download_from_gdrive_to_local]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def remove_file_from_drive():
        service = GoogleDriveHook(gcp_conn_id=CONNECTION_ID).get_conn()
        response = service.files().list(q=f"name = '{DRIVE_FILE_NAME}'").execute()
        if files := response["files"]:
            file = files[0]
            log.info("Deleting file {}...", file)
            service.files().delete(fileId=file["id"])
            log.info("Done.")

    remove_file_from_drive_task = remove_file_from_drive()

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection(connection_id=CONNECTION_ID)

    (
        # TEST SETUP
        [create_bucket >> upload_file, create_connection_task]
        >> copy_single_file
        # TEST BODY
        >> detect_file
        >> download_from_gdrive_to_local
        # TEST TEARDOWN
        >> remove_file_from_drive_task
        >> [delete_bucket, delete_connection_task]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
