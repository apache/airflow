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

from airflow.models import DAG
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator

PATH_TO_UPLOAD_FILE = os.environ.get("AZURE_PATH_TO_UPLOAD_FILE", "example-text.txt")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_local_to_wasb"
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "mycontainer")
AZURE_BLOB_NAME = os.environ.get("AZURE_BLOB_NAME", "myblob")

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    # [START howto_operator_local_to_wasb]
    upload = LocalFilesystemToWasbOperator(
        task_id="upload_file",
        file_path=PATH_TO_UPLOAD_FILE,
        container_name=AZURE_CONTAINER_NAME,
        blob_name=AZURE_BLOB_NAME,
    )
    # [END howto_operator_local_to_wasb]
    delete = WasbDeleteBlobOperator(
        task_id="delete_file", blob_name=AZURE_BLOB_NAME, container_name=AZURE_CONTAINER_NAME
    )

    upload >> delete

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
