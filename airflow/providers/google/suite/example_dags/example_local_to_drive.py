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
Example DAG using LocalFilesystemToGoogleDriveOperator.
"""

from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator

SINGLE_FILE_LOCAL_PATHS = [Path("test1")]
MULTIPLE_FILES_LOCAL_PATHS = [Path("test1"), Path("test2")]
DRIVE_FOLDER = Path("test-folder")

with models.DAG(
    "example_local_to_drive",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_operator_local_to_drive_upload_single_file]
    upload_single_file = LocalFilesystemToGoogleDriveOperator(
        task_id="upload_single_file",
        local_paths=SINGLE_FILE_LOCAL_PATHS,
        drive_folder=DRIVE_FOLDER,
    )
    # [END howto_operator_local_to_drive_upload_single_file]

    # [START howto_operator_local_to_drive_upload_multiple_files]
    upload_multiple_files = LocalFilesystemToGoogleDriveOperator(
        task_id="upload_multiple_files",
        local_paths=MULTIPLE_FILES_LOCAL_PATHS,
        drive_folder=DRIVE_FOLDER,
        ignore_if_missing=True,
    )
    # [END howto_operator_local_to_drive_upload_multiple_files]

    upload_single_file >> upload_multiple_files
