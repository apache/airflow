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
import uuid
from datetime import datetime
from typing import cast

from airflow import DAG
from airflow.decorators import task
from airflow.io.path import ObjectStoragePath
from airflow.providers.common.io.operators.file_transfer import FileTransferOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_file_transfer_local_to_s3"

SAMPLE_TEXT = "This is some sample text."

TEMP_FILE_PATH = ObjectStoragePath("file:///tmp")

AWS_BUCKET_NAME = f"bucket-aws-{DAG_ID}-{ENV_ID}".replace("_", "-")
AWS_BUCKET = ObjectStoragePath(f"s3://{AWS_BUCKET_NAME}")

AWS_FILE_PATH = AWS_BUCKET


@task
def create_temp_file() -> ObjectStoragePath:
    path = ObjectStoragePath(TEMP_FILE_PATH / str(uuid.uuid4()))

    with path.open("w") as file:
        file.write(SAMPLE_TEXT)

    return path


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_temp_file(path: ObjectStoragePath):
    path.unlink()


@task
def remove_bucket():
    AWS_BUCKET.rmdir(recursive=True)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    tags=["example"],
    catchup=False,
) as dag:
    temp_file = create_temp_file()
    temp_file_path = cast(ObjectStoragePath, temp_file)

    # [START howto_transfer_local_to_s3]
    transfer = FileTransferOperator(src=temp_file_path, dst=AWS_BUCKET, task_id="transfer")
    # [END howto_transfer_local_to_s3]

    temp_file >> transfer >> remove_bucket() >> delete_temp_file(temp_file_path)

    from dev.tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
