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

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.adls_to_gcs import ADLSToGCSOperator
from airflow.providers.microsoft.azure.operators.adls import ADLSCreateObjectOperator, ADLSDeleteOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_adls_to_gcs"

# ADLS Gen2 settings
ADLS_FILE_SYSTEM_NAME = os.environ.get("ADLS_FILE_SYSTEM_NAME", "test-file-system")
ADLS_DIRECTORY_PATH = os.environ.get("ADLS_DIRECTORY_PATH", "test-directory")
ADLS_FILE_NAME = f"{ADLS_DIRECTORY_PATH}/test-file.txt"
ADLS_FILE_CONTENT = "Sample file content for ADLS to GCS transfer test"

# GCS settings
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "INVALID BUCKET NAME")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "adls-test")

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "adls", "gcs"],
) as dag:
    # Create a test file in ADLS Gen2
    create_adls_file = ADLSCreateObjectOperator(
        task_id="create_adls_file",
        file_system_name=ADLS_FILE_SYSTEM_NAME,
        file_name=ADLS_FILE_NAME,
        data=ADLS_FILE_CONTENT,
        replace=True,
    )

    # [START how_to_adls_to_gcs]
    # Transfer file from ADLS Gen2 to GCS
    transfer_files_to_gcs = ADLSToGCSOperator(
        task_id="transfer_files_to_gcs",
        src_adls=ADLS_DIRECTORY_PATH,
        dest_gcs=f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}",
        file_system_name=ADLS_FILE_SYSTEM_NAME,
        azure_data_lake_conn_id="azure_data_lake_default",
        gcp_conn_id="google_cloud_default",
        replace=True,
    )
    # [END how_to_adls_to_gcs]

    # Cleanup: Delete the ADLS file
    delete_adls_file = ADLSDeleteOperator(
        task_id="delete_adls_file",
        path=ADLS_FILE_NAME,
        recursive=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Cleanup: Delete GCS objects
    delete_gcs_objects = GCSDeleteObjectsOperator(
        task_id="delete_gcs_objects",
        bucket_name=GCS_BUCKET_NAME,
        prefix=GCS_PREFIX,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_adls_file >> transfer_files_to_gcs >> [delete_adls_file, delete_gcs_objects]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
