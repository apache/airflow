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

from airflow import models
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils import dates

# [START howto_gcs_environment_variables]
BUCKET_NAME = os.environ.get('GCP_GCS_BUCKET', 'example-bucket-name')
PATH_TO_UPLOAD_FILE = os.environ.get('GCP_GCS_PATH_TO_UPLOAD_FILE', 'example-text.txt')
DESTINATION_FILE_LOCATION = os.environ.get('GCP_GCS_DESTINATION_FILE_LOCATION', 'example-text.txt')
# [END howto_gcs_environment_variables]

with models.DAG(
    'example_local_to_gcs',
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=['example'],
) as dag:
    # [START howto_operator_local_filesystem_to_gcs]
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=DESTINATION_FILE_LOCATION,
        bucket=BUCKET_NAME,
    )
    # [END howto_operator_local_filesystem_to_gcs]
