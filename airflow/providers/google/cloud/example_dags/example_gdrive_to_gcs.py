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

from airflow import models
from airflow.providers.google.cloud.transfers.gdrive_to_gcs import GoogleDriveToGCSOperator
from airflow.providers.google.suite.sensors.drive import GoogleDriveFileExistenceSensor

BUCKET = os.environ.get("GCP_GCS_BUCKET", "test28397yeo")
OBJECT = os.environ.get("GCP_GCS_OBJECT", "abc123xyz")
FOLDER_ID = os.environ.get("FILE_ID", "1234567890qwerty")
FILE_NAME = os.environ.get("FILE_NAME", "file.pdf")

with models.DAG(
    "example_gdrive_to_gcs_with_gdrive_sensor",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START detect_file]
    detect_file = GoogleDriveFileExistenceSensor(
        task_id="detect_file", folder_id=FOLDER_ID, file_name=FILE_NAME
    )
    # [END detect_file]
    # [START upload_gdrive_to_gcs]
    upload_gdrive_to_gcs = GoogleDriveToGCSOperator(
        task_id="upload_gdrive_object_to_gcs",
        folder_id=FOLDER_ID,
        file_name=FILE_NAME,
        bucket_name=BUCKET,
        object_name=OBJECT,
    )
    # [END upload_gdrive_to_gcs]
    detect_file >> upload_gdrive_to_gcs
