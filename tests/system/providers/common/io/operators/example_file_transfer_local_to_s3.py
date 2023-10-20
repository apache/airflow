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
from airflow.io.store.path import ObjectStoragePath
from airflow.models.baseoperator import chain
from airflow.providers.common.io.operators.file_transfer import FileTransferOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_file_transfer_local_to_s3"

SAMPLE_TEXT = "This is some sample text."

FILENAME = "sample-txt.txt"
TEMP_FILE_PATH = ObjectStoragePath("file:///tmp") / FILENAME

AWS_BUCKET_NAME = f"bucket-aws-{DAG_ID}-{ENV_ID}".replace("_", "-")
AWS_BUCKET = ObjectStoragePath(f"s3://{AWS_BUCKET_NAME}")

AWS_FILE_PATH = AWS_BUCKET / FILENAME


@task
def create_temp_file():
    with TEMP_FILE_PATH.open("w") as file:
        file.write(SAMPLE_TEXT)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_temp_file():
    TEMP_FILE_PATH.unlink()


@task
def create_bucket():
    AWS_BUCKET.mkdir(exist_ok=True)


@task
def remove_bucket():
    AWS_BUCKET.unlink(recursive=True)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_transfer_local_to_s3]
    transfer = FileTransferOperator(src=TEMP_FILE_PATH, dst=AWS_FILE_PATH, task_id="transfer")
    # [END howto_transfer_local_to_s3]

    chain(
        # TEST SETUP
        create_temp_file(),
        create_bucket(),
        # TEST BODY
        transfer,
        # TEST TEARDOWN
        remove_bucket(),
        delete_temp_file(),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
