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
Example Airflow DAG for Google Cloud Storage to SFTP transfer operators.
"""

import os

import airflow
from airflow import models
from airflow.providers.google.cloud.operators.sftp_to_gcs import SFTPToGoogleCloudStorageOperator

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

BUCKET_SRC = os.environ.get("GCP_GCS_BUCKET_1_SRC", "test-sftp-gcs")

TMP_PATH = "/tmp"
DIR = "tests_sftp_hook_dir"
SUBDIR = "subdir"

OBJECT_SRC_1 = "parent-1.bin"
OBJECT_SRC_2 = "parent-2.bin"
OBJECT_SRC_3 = "parent-3.txt"


with models.DAG(
    "example_sftp_to_gcs", default_args=default_args, schedule_interval=None
) as dag:
    # [START howto_operator_sftp_to_gcs_copy_single_file]
    copy_file_from_sftp_to_gcs = SFTPToGoogleCloudStorageOperator(
        task_id="file-copy-sftp-to-gcs",
        source_path=os.path.join(TMP_PATH, DIR, OBJECT_SRC_1),
        destination_bucket=BUCKET_SRC,
    )
    # [END howto_operator_sftp_to_gcs_copy_single_file]

    # [START howto_operator_sftp_to_gcs_move_single_file_destination]
    move_file_from_sftp_to_gcs_destination = SFTPToGoogleCloudStorageOperator(
        task_id="file-move-sftp-to-gcs-destination",
        source_path=os.path.join(TMP_PATH, DIR, OBJECT_SRC_2),
        destination_bucket=BUCKET_SRC,
        destination_path="destination_dir/destination_filename.bin",
        move_object=True,
    )
    # [END howto_operator_sftp_to_gcs_move_single_file_destination]

    # [START howto_operator_sftp_to_gcs_copy_directory]
    copy_directory_from_sftp_to_gcs = SFTPToGoogleCloudStorageOperator(
        task_id="dir-copy-sftp-to-gcs",
        source_path=os.path.join(TMP_PATH, DIR, SUBDIR, "*"),
        destination_bucket=BUCKET_SRC,
    )
    # [END howto_operator_sftp_to_gcs_copy_directory]

    # [START howto_operator_sftp_to_gcs_move_specific_files]
    move_specific_files_from_gcs_to_sftp = SFTPToGoogleCloudStorageOperator(
        task_id="dir-move-specific-files-sftp-to-gcs",
        source_path=os.path.join(TMP_PATH, DIR, SUBDIR, "*.bin"),
        destination_bucket=BUCKET_SRC,
        destination_path="specific_files/",
        move_object=True,
    )
    # [END howto_operator_sftp_to_gcs_move_specific_files]
