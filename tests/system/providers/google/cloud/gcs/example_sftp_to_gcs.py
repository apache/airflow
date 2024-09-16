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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.standard.core.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "example_sftp_to_gcs"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"

TMP_PATH = "tmp"
DIR = "tests_sftp_hook_dir"
SUBDIR = "subdir"

OBJECT_SRC_1 = "parent-1.bin"
OBJECT_SRC_2 = "parent-2.bin"

CURRENT_FOLDER = Path(__file__).parent
LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources")

FILE_LOCAL_PATH = str(Path(LOCAL_PATH) / TMP_PATH / DIR)
FILE_NAME = "tmp.tar.gz"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    unzip_file = BashOperator(
        task_id="unzip_data_file", bash_command=f"tar xvf {LOCAL_PATH}/{FILE_NAME} -C {LOCAL_PATH}"
    )

    # [START howto_operator_sftp_to_gcs_copy_single_file]
    copy_file_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="file-copy-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{OBJECT_SRC_1}",
        destination_bucket=BUCKET_NAME,
    )
    # [END howto_operator_sftp_to_gcs_copy_single_file]

    # [START howto_operator_sftp_to_gcs_move_single_file_destination]
    move_file_from_sftp_to_gcs_destination = SFTPToGCSOperator(
        task_id="file-move-sftp-to-gcs-destination",
        source_path=f"{FILE_LOCAL_PATH}/{OBJECT_SRC_2}",
        destination_bucket=BUCKET_NAME,
        destination_path="destination_dir/destination_filename.bin",
        move_object=True,
    )
    # [END howto_operator_sftp_to_gcs_move_single_file_destination]

    # [START howto_operator_sftp_to_gcs_copy_directory]
    copy_directory_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="dir-copy-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{SUBDIR}/*",
        destination_bucket=BUCKET_NAME,
    )
    # [END howto_operator_sftp_to_gcs_copy_directory]

    # [START howto_operator_sftp_to_gcs_move_specific_files]
    move_specific_files_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="dir-move-specific-files-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{SUBDIR}/*.bin",
        destination_bucket=BUCKET_NAME,
        destination_path="specific_files/",
        move_object=True,
    )
    # [END howto_operator_sftp_to_gcs_move_specific_files]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        unzip_file,
        # TEST BODY
        copy_file_from_sftp_to_gcs,
        move_file_from_sftp_to_gcs_destination,
        copy_directory_from_sftp_to_gcs,
        move_specific_files_from_sftp_to_gcs,
        # TEST TEARDOWN
        delete_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
