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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator

BLOB_NAME = os.environ.get("AZURE_BLOB_NAME", "file.txt")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "airflow")
GCP_BUCKET_FILE_PATH = os.environ.get("GCP_BUCKET_FILE_PATH", "file.txt")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "INVALID BUCKET NAME")
GCP_OBJECT_NAME = os.environ.get("GCP_OBJECT_NAME", "file.txt")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_azure_blob_to_gcs"

# [START how_to_azure_blob_to_gcs]
with DAG(
    DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    default_args={"container_name": AZURE_CONTAINER_NAME, "blob_name": BLOB_NAME},
) as dag:

    wait_for_blob = WasbBlobSensor(task_id="wait_for_blob")

    transfer_files_to_gcs = AzureBlobStorageToGCSOperator(
        task_id="transfer_files_to_gcs",
        # AZURE arg
        file_path=GCP_OBJECT_NAME,
        # GCP args
        bucket_name=GCP_BUCKET_NAME,
        object_name=GCP_OBJECT_NAME,
        filename=GCP_BUCKET_FILE_PATH,
        gzip=False,
        delegate_to=None,
        impersonation_chain=None,
    )
    # [END how_to_azure_blob_to_gcs]

    wait_for_blob >> transfer_files_to_gcs

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
