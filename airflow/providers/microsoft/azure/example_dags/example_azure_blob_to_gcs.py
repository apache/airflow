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
import os

from airflow import DAG
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor
from airflow.providers.microsoft.azure.transfers.azure_blob_to_gcs import AzureBlobStorageToGCSOperator
from airflow.utils.dates import days_ago

BLOB_NAME = os.environ.get("AZURE_BLOB_NAME", "file.txt")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "airflow")
GCP_BUCKET_FILE_PATH = os.environ.get("GCP_BUCKET_FILE_PATH", "file.txt")
GCP_BUCKET_NAME = os.environ.get("GCP_BUCKET_NAME", "azure_bucket")
GCP_OBJECT_NAME = os.environ.get("GCP_OBJECT_NAME", "file.txt")


with DAG(
    "example_azure_blob_to_gcs",
    schedule_interval=None,
    start_date=days_ago(1),  # Override to match your needs
) as dag:

    # [START how_to_wait_for_blob]
    wait_for_blob = WasbBlobSensor(
        task_id="wait_for_blob",
        wasb_conn_id="wasb_default",
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_NAME,
    )
    # [END how_to_wait_for_blob]

    # [START how_to_azure_blob_to_gcs]
    transfer_files_to_gcs = AzureBlobStorageToGCSOperator(
        task_id="transfer_files_to_gcs",
        # AZURE args
        wasb_conn_id="wasb_default",
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_NAME,
        file_path=GCP_OBJECT_NAME,
        # GCP args
        gcp_conn_id="google_cloud_default",
        bucket_name=GCP_BUCKET_NAME,
        object_name=GCP_OBJECT_NAME,
        filename=GCP_BUCKET_FILE_PATH,
        gzip=False,
        delegate_to=None,
        impersonation_chain=None,
    )
    # [END how_to_azure_blob_to_gcs]

    wait_for_blob >> transfer_files_to_gcs
