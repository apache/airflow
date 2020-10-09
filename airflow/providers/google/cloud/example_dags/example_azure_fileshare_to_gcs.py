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
from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs import AzureFileShareToGCSOperator

DEST_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET', 'gs://test-gcs-example-bucket')
AZURE_SHARE_NAME = os.environ.get('AZURE_SHARE_NAME', 'test-azure-share')
AZURE_DIRECTORY_NAME = "test-azure-dir"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='azure_fileshare_to_gcs_example',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2018, 11, 1),
    tags=['example'],
) as dag:
    # [START howto_operator_azure_fileshare_to_gcs_basic]
    sync_azure_files_with_gcs = AzureFileShareToGCSOperator(
        task_id='sync_azure_files_with_gcs',
        share_name=AZURE_SHARE_NAME,
        dest_gcs=DEST_GCS_BUCKET,
        directory_name=AZURE_DIRECTORY_NAME,
        wasb_conn_id='azure_fileshare_default',
        gcp_conn_id='google_cloud_default',
        replace=False,
        gzip=True,
        google_impersonation_chain=None,
    )
    # [END howto_operator_azure_fileshare_to_gcs_basic]
