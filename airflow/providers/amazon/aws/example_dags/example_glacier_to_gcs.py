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

from airflow import models
from airflow.providers.amazon.aws.operators.glacier import GlacierCreateJobOperator
from airflow.providers.amazon.aws.sensors.glacier import GlacierJobOperationSensor
from airflow.providers.amazon.aws.transfers.glacier_to_gcs import GlacierToGCSOperator
from airflow.utils.dates import days_ago

VAULT_NAME = "airflow"
BUCKET_NAME = os.environ.get("GLACIER_GCS_BUCKET_NAME", "gs://glacier_bucket")
OBJECT_NAME = os.environ.get("GLACIER_OBJECT", "example-text.txt")

with models.DAG(
    "example_glacier_to_gcs",
    schedule_interval=None,
    start_date=days_ago(1),  # Override to match your needs
) as dag:
    # [START howto_glacier_create_job_operator]
    create_glacier_job = GlacierCreateJobOperator(
        task_id="create_glacier_job",
        aws_conn_id="aws_default",
        vault_name=VAULT_NAME,
    )
    JOB_ID = '{{ task_instance.xcom_pull("create_glacier_job")["jobId"] }}'
    # [END howto_glacier_create_job_operator]

    # [START howto_glacier_job_operation_sensor]
    wait_for_operation_complete = GlacierJobOperationSensor(
        aws_conn_id="aws_default",
        vault_name=VAULT_NAME,
        job_id=JOB_ID,
        task_id="wait_for_operation_complete",
    )
    # [END howto_glacier_job_operation_sensor]

    # [START howto_glacier_transfer_data_to_gcs]
    transfer_archive_to_gcs = GlacierToGCSOperator(
        task_id="transfer_archive_to_gcs",
        aws_conn_id="aws_default",
        gcp_conn_id="google_cloud_default",
        vault_name=VAULT_NAME,
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        gzip=False,
        # Override to match your needs
        # If chunk size is bigger than actual file size
        # then whole file will be downloaded
        chunk_size=1024,
        delegate_to=None,
        google_impersonation_chain=None,
    )
    # [END howto_glacier_transfer_data_to_gcs]

    create_glacier_job >> wait_for_operation_complete >> transfer_archive_to_gcs
