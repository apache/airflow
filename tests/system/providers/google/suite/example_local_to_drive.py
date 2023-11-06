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
Example DAG using LocalFilesystemToGoogleDriveOperator.

Using this operator requires the following additional scopes:
https://www.googleapis.com/auth/drive
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_local_to_drive"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")

FILE_NAME_1 = "test1"
FILE_NAME_2 = "test2"
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"

LOCAL_PATH = str(Path(__file__).parent / "resources")

SINGLE_FILE_LOCAL_PATHS = [str(Path(LOCAL_PATH) / FILE_NAME_1)]
MULTIPLE_FILES_LOCAL_PATHS = [str(Path(LOCAL_PATH) / FILE_NAME_1), str(Path(LOCAL_PATH) / FILE_NAME_2)]

DRIVE_FOLDER = f"test_folder_{DAG_ID}_{ENV_ID}"

log = logging.getLogger(__name__)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gdrive"],
) as dag:

    @task
    def create_temp_gcp_connection():
        conn = Connection(
            conn_id=CONNECTION_ID,
            conn_type="google_cloud_platform",
        )
        conn_extra_json = json.dumps({"scope": "https://www.googleapis.com/auth/drive"})
        conn.set_extra(conn_extra_json)

        session = Session()
        if session.query(Connection).filter(Connection.conn_id == CONNECTION_ID).first():
            log.warning("Connection %s already exists", CONNECTION_ID)
            return None
        session.add(conn)
        session.commit()

    create_temp_gcp_connection_task = create_temp_gcp_connection()

    # [START howto_operator_local_to_drive_upload_single_file]
    upload_single_file = LocalFilesystemToGoogleDriveOperator(
        gcp_conn_id=CONNECTION_ID,
        task_id="upload_single_file",
        local_paths=SINGLE_FILE_LOCAL_PATHS,
        drive_folder=DRIVE_FOLDER,
    )
    # [END howto_operator_local_to_drive_upload_single_file]

    # [START howto_operator_local_to_drive_upload_multiple_files]
    upload_multiple_files = LocalFilesystemToGoogleDriveOperator(
        gcp_conn_id=CONNECTION_ID,
        task_id="upload_multiple_files",
        local_paths=MULTIPLE_FILES_LOCAL_PATHS,
        drive_folder=DRIVE_FOLDER,
        ignore_if_missing=True,
    )
    # [END howto_operator_local_to_drive_upload_multiple_files]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def remove_files_from_drive():
        service = GoogleDriveHook(gcp_conn_id=CONNECTION_ID).get_conn()
        root_path = (
            service.files()
            .list(q=f"name = '{DRIVE_FOLDER}' and mimeType = 'application/vnd.google-apps.folder'")
            .execute()
        )
        if files := root_path["files"]:
            batch = service.new_batch_http_request()
            for file in files:
                log.info("Preparing to remove file: {}", file)
                batch.add(service.files().delete(fileId=file["id"]))
            batch.execute()
            log.info("Selected files removed.")

    remove_files_from_drive_task = remove_files_from_drive()

    delete_temp_gcp_connection_task = BashOperator(
        task_id="delete_temp_gcp_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_temp_gcp_connection_task
        # TEST BODY
        >> upload_single_file
        >> upload_multiple_files
        # TEST TEARDOWN
        >> remove_files_from_drive_task
        >> delete_temp_gcp_connection_task
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
