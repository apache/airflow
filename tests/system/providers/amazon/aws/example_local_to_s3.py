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
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()


DAG_ID = 'example_local_to_s3'
TEMP_FILE_PATH = '/tmp/sample-txt.txt'
SAMPLE_TEXT = 'This is some sample text.'


@task
def create_temp_file():
    file = open(TEMP_FILE_PATH, 'w')
    file.write(SAMPLE_TEXT)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_temp_file():
    if os.path.exists(TEMP_FILE_PATH):
        os.remove(TEMP_FILE_PATH)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context['ENV_ID']

    s3_bucket_name = f'{env_id}-bucket'
    s3_key = f'{env_id}/files/my-temp-file.txt'

    create_s3_bucket = S3CreateBucketOperator(task_id='create-s3-bucket', bucket_name=s3_bucket_name)
    # [START howto_transfer_local_to_s3]
    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename=TEMP_FILE_PATH,
        dest_key=s3_key,
        dest_bucket=s3_bucket_name,
        replace=True,
    )
    # [END howto_transfer_local_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id='delete_s3_bucket',
        bucket_name=s3_bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_temp_file(),
        create_s3_bucket,
        # TEST BODY
        create_local_to_s3_job,
        # TEST TEARDOWN
        delete_s3_bucket,
        delete_temp_file(),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
