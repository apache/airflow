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
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from airflow.providers.google.cloud.transfers.sftp_to_gcs import SFTPToGCSOperator
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from system.openlineage.operator import OpenLineageTestOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "example_gcs_to_sftp"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"

TMP_PATH = "tmp"
DIR = "tests_sftp_hook_dir"
SUBDIR = "subdir"

OBJECT_SRC = "parent-1.bin"
OBJECT_DEST = "dest.bin"

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

    copy_file_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="file-copy-sftp-to-gcs-destination",
        source_path=f"{FILE_LOCAL_PATH}/{OBJECT_SRC}",
        destination_bucket=BUCKET_NAME,
        destination_path="destination_dir/destination_filename.bin",
    )

    copy_directory_from_sftp_to_gcs = SFTPToGCSOperator(
        task_id="dir-copy-sftp-to-gcs",
        source_path=f"{FILE_LOCAL_PATH}/{SUBDIR}/*",
        destination_bucket=BUCKET_NAME,
        destination_path="another_dir/",
    )

    copy_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-copy-from-gcs-to-sftp",
        source_bucket=BUCKET_NAME,
        source_object="destination_dir/destination_filename.bin",
        destination_path=f"{FILE_LOCAL_PATH}/",
    )

    copy_directory_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-copy-gcs-to-sftp",
        source_bucket=BUCKET_NAME,
        source_object="another_dir/*",
        destination_path=f"{FILE_LOCAL_PATH}/{SUBDIR}",
        keep_directory_structure=False,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "gcs_to_sftp.json"),
    )

    chain(
        # TEST SETUP
        create_bucket,
        unzip_file,
        # TEST BODY
        copy_file_from_sftp_to_gcs,
        copy_directory_from_sftp_to_gcs,
        copy_file_from_gcs_to_sftp,
        copy_directory_from_gcs_to_sftp,
        # TEST TEARDOWN
        delete_bucket,
        check_openlineage_events,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
