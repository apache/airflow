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
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from airflow.utils.dates import days_ago

AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "airflow")
BLOB_PREFIX = os.environ.get("AZURE_BLOB_PREFIX", "airflow")
SFTP_SRC_PATH = os.environ.get("AZURE_SFTP_SRC_PATH", "test-sftp-azure")

with DAG(
    "example_sftp_to_azure_blob",
    schedule_interval=None,
    start_date=days_ago(1),  # Override to match your needs
) as dag:

    # [START how_to_sftp_to_azure_blob]
    transfer_files_to_azure = SFTPToWasbOperator(
        task_id="transfer_files_to_gcs",
        # SFTP args
        sftp_conn_id="sftp_default",
        sftp_source_path=SFTP_SRC_PATH,
        # AZURE args
        wasb_conn_id="wasb_default",
        container_name=AZURE_CONTAINER_NAME,
        blob_prefix=BLOB_PREFIX,
    )
    # [END how_to_sftp_to_azure_blob]
