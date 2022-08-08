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
from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glacier import GlacierCreateJobOperator
from airflow.providers.amazon.aws.sensors.glacier import GlacierJobOperationSensor
from airflow.providers.amazon.aws.transfers.glacier_to_gcs import GlacierToGCSOperator

VAULT_NAME = "airflow"
BUCKET_NAME = os.environ.get("GLACIER_GCS_BUCKET_NAME", "gs://INVALID BUCKET NAME")
OBJECT_NAME = os.environ.get("GLACIER_OBJECT", "example-text.txt")

with DAG(
    "example_glacier_to_gcs",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    catchup=False,
) as dag:
    # [START howto_operator_glacier_create_job]
    create_glacier_job = GlacierCreateJobOperator(task_id="create_glacier_job", vault_name=VAULT_NAME)
    JOB_ID = '{{ task_instance.xcom_pull("create_glacier_job")["jobId"] }}'
    # [END howto_operator_glacier_create_job]

    # [START howto_sensor_glacier_job_operation]
    wait_for_operation_complete = GlacierJobOperationSensor(
        vault_name=VAULT_NAME,
        job_id=JOB_ID,
        task_id="wait_for_operation_complete",
    )
    # [END howto_sensor_glacier_job_operation]

    # [START howto_transfer_glacier_to_gcs]
    transfer_archive_to_gcs = GlacierToGCSOperator(
        task_id="transfer_archive_to_gcs",
        vault_name=VAULT_NAME,
        bucket_name=BUCKET_NAME,
        object_name=OBJECT_NAME,
        gzip=False,
        # Override to match your needs
        # If chunk size is bigger than actual file size
        # then whole file will be downloaded
        chunk_size=1024,
    )
    # [END howto_transfer_glacier_to_gcs]

    create_glacier_job >> wait_for_operation_complete >> transfer_archive_to_gcs
