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
"""
Example DAG using GoogleCloudStorageToGoogleDriveOperator.
"""
import os

from airflow import models
from airflow.providers.google.suite.transfers.gcs_to_gdrive import GCSToGoogleDriveOperator
from airflow.utils.dates import days_ago

GCS_TO_GDRIVE_BUCKET = os.environ.get("GCS_TO_DRIVE_BUCKET", "example-object")

with models.DAG(
    "example_gcs_to_gdrive",
    schedule_interval=None,  # Override to match your needs,
    start_date=days_ago(1),
    tags=['example'],
) as dag:
    # [START howto_operator_gcs_to_gdrive_copy_single_file]
    copy_single_file = GCSToGoogleDriveOperator(
        task_id="copy_single_file",
        source_bucket=GCS_TO_GDRIVE_BUCKET,
        source_object="sales/january.avro",
        destination_object="copied_sales/january-backup.avro",
    )
    # [END howto_operator_gcs_to_gdrive_copy_single_file]
    # [START howto_operator_gcs_to_gdrive_copy_files]
    copy_files = GCSToGoogleDriveOperator(
        task_id="copy_files",
        source_bucket=GCS_TO_GDRIVE_BUCKET,
        source_object="sales/*",
        destination_object="copied_sales/",
    )
    # [END howto_operator_gcs_to_gdrive_copy_files]
    # [START howto_operator_gcs_to_gdrive_move_files]
    move_files = GCSToGoogleDriveOperator(
        task_id="move_files",
        source_bucket=GCS_TO_GDRIVE_BUCKET,
        source_object="sales/*.avro",
        move_object=True,
    )
    # [END howto_operator_gcs_to_gdrive_move_files]
