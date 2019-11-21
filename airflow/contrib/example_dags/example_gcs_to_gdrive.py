# -*- coding: utf-8 -*-
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

import airflow
from airflow import models
from airflow.contrib.operators.gcs_to_gdrive_operator import GcsToGDriveOperator

GCS_TO_GDRIVE_BUCKET = os.environ.get("GCS_TO_DRIVE_BUCKET", "example-object")

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

with models.DAG(
    "example_gcs_to_gdrive", default_args=default_args, schedule_interval=None  # Override to match your needs
) as dag:
    # [START howto_operator_gcs_to_gdrive_copy_single_file]
    copy_single_file = GcsToGDriveOperator(
        task_id="copy_single_file",
        source_bucket=GCS_TO_GDRIVE_BUCKET,
        source_object="sales/january.avro",
        destination_object="copied_sales/january-backup.avro",
    )
    # [END howto_operator_gcs_to_gdrive_copy_single_file]
    # [START howto_operator_gcs_to_gdrive_copy_files]
    copy_files = GcsToGDriveOperator(
        task_id="copy_files",
        source_bucket=GCS_TO_GDRIVE_BUCKET,
        source_object="sales/*",
        destination_object="copied_sales/",
    )
    # [END howto_operator_gcs_to_gdrive_copy_files]
    # [START howto_operator_gcs_to_gdrive_move_files]
    move_files = GcsToGDriveOperator(
        task_id="move_files",
        source_bucket=GCS_TO_GDRIVE_BUCKET,
        source_object="sales/*.avro",
        move_object=True,
    )
    # [END howto_operator_gcs_to_gdrive_move_files]
