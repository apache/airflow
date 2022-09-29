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
Example DAG using GoogleCloudStorageToGoogleDriveOperator.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.suite.transfers.gcs_to_gdrive import GCSToGoogleDriveOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gcs_to_gdrive"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

TMP_PATH = "tmp"
DIR = "tests_dir"
SUBDIR = "subdir"

OBJECT_SRC_1 = "6.txt"

CURRENT_FOLDER = Path(__file__).parent
LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources")

FILE_LOCAL_PATH = str(Path(LOCAL_PATH) / TMP_PATH / DIR)
FILE_NAME = "tmp.tar.gz"

CLOUD_PLATFORM_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'
GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'

os.environ["AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"] = (
    "google-cloud-platform://?extra__google_cloud_platform__scope="
    f"{CLOUD_PLATFORM_SCOPE},{GOOGLE_DRIVE_SCOPE}"
)


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example', 'gcs'],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )
    unzip_file = BashOperator(
        task_id="unzip_data_file", bash_command=f"tar xvf {LOCAL_PATH}/{FILE_NAME} -C {LOCAL_PATH}"
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=f"{FILE_LOCAL_PATH}/{SUBDIR}/*",
        dst=f"{TMP_PATH}/",
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_gcs_to_gdrive_copy_single_file]
    copy_single_file = GCSToGoogleDriveOperator(
        task_id="copy_single_file",
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/{OBJECT_SRC_1}",
        destination_object="copied_tmp/copied_object_1.txt",
    )
    # [END howto_operator_gcs_to_gdrive_copy_single_file]

    # [START howto_operator_gcs_to_gdrive_copy_files]
    copy_files = GCSToGoogleDriveOperator(
        task_id="copy_files",
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/*",
        destination_object="copied_tmp/",
    )
    # [END howto_operator_gcs_to_gdrive_copy_files]

    # [START howto_operator_gcs_to_gdrive_move_files]
    move_files = GCSToGoogleDriveOperator(
        task_id="move_files",
        source_bucket=BUCKET_NAME,
        source_object=f"{TMP_PATH}/*.bin",
        move_object=True,
    )
    # [END howto_operator_gcs_to_gdrive_move_files]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> unzip_file
        >> upload_file
        # TEST BODY
        >> copy_single_file
        >> copy_files
        >> move_files
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
