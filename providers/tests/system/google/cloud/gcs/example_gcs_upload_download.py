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
Example Airflow DAG for testing interaction between Google Cloud Storage and local file system.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "gcs_upload_download"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
PATH_TO_SAVED_FILE = "example_upload_download.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "example"],
) as dag:
    # [START howto_operator_gcs_create_bucket]
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )
    # [END howto_operator_gcs_create_bucket]

    # [START howto_operator_local_filesystem_to_gcs]
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )
    # [END howto_operator_local_filesystem_to_gcs]

    # [START howto_operator_gcs_download_file_task]
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=FILE_NAME,
        bucket=BUCKET_NAME,
        filename=PATH_TO_SAVED_FILE,
    )
    # [END howto_operator_gcs_download_file_task]

    # [START howto_operator_gcs_delete_bucket]
    delete_bucket = GCSDeleteBucketOperator(task_id="delete_bucket", bucket_name=BUCKET_NAME)
    # [END howto_operator_gcs_delete_bucket]
    delete_bucket.trigger_rule = TriggerRule.ALL_DONE

    (
        # TEST SETUP
        create_bucket
        >> upload_file
        # TEST BODY
        >> download_file
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
