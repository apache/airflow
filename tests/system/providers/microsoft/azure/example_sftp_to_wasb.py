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

from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.transfers.sftp_to_wasb import SFTPToWasbOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator

AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "airflow")
BLOB_PREFIX = os.environ.get("AZURE_BLOB_PREFIX", "airflow")
SFTP_SRC_PATH = os.environ.get("SFTP_SRC_PATH", "/sftp")
LOCAL_FILE_PATH = os.environ.get("LOCAL_SRC_PATH", "/tmp")
SAMPLE_FILENAME = os.environ.get("SFTP_SAMPLE_FILENAME", "sftp_to_wasb_test.txt")
FILE_COMPLETE_PATH = os.path.join(LOCAL_FILE_PATH, SAMPLE_FILENAME)
SFTP_FILE_COMPLETE_PATH = os.path.join(SFTP_SRC_PATH, SAMPLE_FILENAME)
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_sftp_to_wasb"


@task
def delete_sftp_file():
    """Delete a file at SFTP SERVER"""
    SFTPHook().delete_file(SFTP_FILE_COMPLETE_PATH)


with DAG(
    DAG_ID,
    schedule=None,
    catchup=False,
    start_date=datetime(2021, 1, 1),  # Override to match your needs
) as dag:
    transfer_files_to_sftp_step = SFTPOperator(
        task_id="transfer_files_from_local_to_sftp",
        local_filepath=FILE_COMPLETE_PATH,
        remote_filepath=SFTP_FILE_COMPLETE_PATH,
    )

    # [START how_to_sftp_to_wasb]
    transfer_files_to_azure = SFTPToWasbOperator(
        task_id="transfer_files_from_sftp_to_wasb",
        # SFTP args
        sftp_source_path=SFTP_SRC_PATH,
        # AZURE args
        container_name=AZURE_CONTAINER_NAME,
        blob_prefix=BLOB_PREFIX,
    )
    # [END how_to_sftp_to_wasb]

    delete_blob_file_step = WasbDeleteBlobOperator(
        task_id="delete_blob_files",
        container_name=AZURE_CONTAINER_NAME,
        blob_name=BLOB_PREFIX + SAMPLE_FILENAME,
    )

    transfer_files_to_sftp_step >> transfer_files_to_azure >> delete_blob_file_step >> delete_sftp_file()

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
