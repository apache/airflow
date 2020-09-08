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
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-id")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "test-gcs-example-bucket")

PATH_TO_REMOTE_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example-remote.txt")
PATH_TO_LOCAL_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "test-gcs-example-local.txt")

with models.DAG(
    "example_gcs_to_local",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_gcs_download_file_task]
    download_file = GCSToLocalFilesystemOperator(
        task_id="download_file",
        object_name=PATH_TO_REMOTE_FILE,
        bucket=BUCKET,
        filename=PATH_TO_LOCAL_FILE,
    )
    # [END howto_operator_gcs_download_file_task]
