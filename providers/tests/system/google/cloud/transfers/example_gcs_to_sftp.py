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
Example Airflow DAG for Google Cloud Storage to SFTP transfer operators.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "gcs_to_sftp"

SFTP_CONN_ID = "ssh_default"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DESTINATION_PATH_1 = "/tmp/single-file/"
DESTINATION_PATH_2 = "/tmp/dest-dir-1/"
DESTINATION_PATH_3 = "/tmp/dest-dir-2/"
FILE_NAME = GCS_SRC_FILE = "empty.txt"
UPLOAD_SRC = str(Path(__file__).parent / "resources" / FILE_NAME)
GCS_SRC_FILE_IN_DIR = f"dir-1/{FILE_NAME}"
GCS_SRC_DIR = "dir-2/*"
UPLOAD_IN_DIR_DST = f"dir-2/{FILE_NAME}"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_file_1 = LocalFilesystemToGCSOperator(
        task_id="upload_file_1",
        src=UPLOAD_SRC,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )
    upload_file_2 = LocalFilesystemToGCSOperator(
        task_id="upload_file_2",
        src=UPLOAD_SRC,
        dst=GCS_SRC_FILE_IN_DIR,
        bucket=BUCKET_NAME,
    )
    upload_file_3 = LocalFilesystemToGCSOperator(
        task_id="upload_file_3",
        src=UPLOAD_SRC,
        dst=UPLOAD_IN_DIR_DST,
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_gcs_to_sftp_copy_single_file]
    copy_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_NAME,
        source_object=GCS_SRC_FILE,
        destination_path=DESTINATION_PATH_1,
    )
    # [END howto_operator_gcs_to_sftp_copy_single_file]

    check_copy_file_from_gcs_to_sftp = SFTPSensor(
        task_id="check-file-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_1, GCS_SRC_FILE),
    )

    # [START howto_operator_gcs_to_sftp_move_single_file_destination]
    move_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_NAME,
        source_object=GCS_SRC_FILE_IN_DIR,
        destination_path=DESTINATION_PATH_1,
        move_object=True,
    )
    # [END howto_operator_gcs_to_sftp_move_single_file_destination]

    check_move_file_from_gcs_to_sftp = SFTPSensor(
        task_id="check-file-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_1, GCS_SRC_FILE_IN_DIR),
    )

    # [START howto_operator_gcs_to_sftp_copy_directory]
    copy_dir_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_NAME,
        source_object=GCS_SRC_DIR,
        destination_path=DESTINATION_PATH_2,
    )
    # [END howto_operator_gcs_to_sftp_copy_directory]

    check_copy_dir_from_gcs_to_sftp = SFTPSensor(
        task_id="check-dir-copy-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_2, "dir-2", GCS_SRC_FILE),
    )

    # [START howto_operator_gcs_to_sftp_move_specific_files]
    move_dir_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        source_bucket=BUCKET_NAME,
        source_object=GCS_SRC_DIR,
        destination_path=DESTINATION_PATH_3,
        keep_directory_structure=False,
    )
    # [END howto_operator_gcs_to_sftp_move_specific_files]

    check_move_dir_from_gcs_to_sftp = SFTPSensor(
        task_id="check-dir-move-gsc-to-sftp",
        sftp_conn_id=SFTP_CONN_ID,
        timeout=60,
        path=os.path.join(DESTINATION_PATH_3, GCS_SRC_FILE),
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> (upload_file_1, upload_file_2, upload_file_3)
        # TEST BODY
        >> copy_file_from_gcs_to_sftp
        >> check_copy_file_from_gcs_to_sftp
        >> move_file_from_gcs_to_sftp
        >> check_move_file_from_gcs_to_sftp
        >> copy_dir_from_gcs_to_sftp
        >> check_copy_dir_from_gcs_to_sftp
        >> move_dir_from_gcs_to_sftp
        >> check_move_dir_from_gcs_to_sftp
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
