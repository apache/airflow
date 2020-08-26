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

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_sftp import GCSToSFTPOperator
from airflow.utils.dates import days_ago

BUCKET_SRC = os.environ.get("GCP_GCS_BUCKET_1_SRC", "test-gcs-sftp")
OBJECT_SRC_1 = "parent-1.bin"
OBJECT_SRC_2 = "parent-2.bin"
OBJECT_SRC_3 = "subdir-1/*"
DESTINATION_PATH_1 = "/tmp/single-file/"
DESTINATION_PATH_2 = "/tmp/dirs/"


with models.DAG(
    "example_gcs_to_sftp", start_date=days_ago(1), schedule_interval=None, tags=['example']
) as dag:
    # [START howto_operator_gcs_to_sftp_copy_single_file]
    copy_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-copy-gsc-to-sftp",
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_1,
        destination_path=DESTINATION_PATH_1,
    )
    # [END howto_operator_gcs_to_sftp_copy_single_file]

    # [START howto_operator_gcs_to_sftp_move_single_file_destination]
    move_file_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="file-move-gsc-to-sftp",
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_2,
        destination_path=DESTINATION_PATH_1,
        move_object=True,
    )
    # [END howto_operator_gcs_to_sftp_move_single_file_destination]

    # [START howto_operator_gcs_to_sftp_copy_directory]
    copy_dir_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-copy-gsc-to-sftp",
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_3,
        destination_path=DESTINATION_PATH_2,
    )
    # [END howto_operator_gcs_to_sftp_copy_directory]

    # [START howto_operator_gcs_to_sftp_move_specific_files]
    move_dir_from_gcs_to_sftp = GCSToSFTPOperator(
        task_id="dir-move-gsc-to-sftp",
        source_bucket=BUCKET_SRC,
        source_object=OBJECT_SRC_3,
        destination_path=DESTINATION_PATH_2,
    )
    # [END howto_operator_gcs_to_sftp_move_specific_files]
