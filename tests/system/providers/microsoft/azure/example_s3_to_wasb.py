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

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator,
    S3CreateBucketOperator,
    S3DeleteBucketOperator
)
from airflow.providers.microsoft.azure.transfers.s3_to_wasb import S3ToAzureBlobStorageOperator

from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

# Set constants
DAG_ID: str = "example_s3_to_wasb"
S3_KEY: str = "test/TEST1.csv"
BLOB_PREFIX: str = "test"


# Create tasks using the TaskFlow API to create and remove a container
@task
def create_wasb_container(container_name):
    return WasbHook().create_container(container_name)


@task
def remove_wasb_container(container_name):
    return WasbHook().delete_container(container_name)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    # Pull the task context, as well as the ENV_ID
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    # Create the bucket name and container name using the ENV_ID
    s3_bucket_name: str = f"{env_id}-s3-bucket"
    wasb_container_name: str = f"{env_id}-wasb-container"

    # Create an S3 bucket
    create_s3_bucket = S3CreateBucketOperator(task_id="create-s3-bucket", bucket_name=s3_bucket_name)

    # Add a file to S3
    create_s3_object = S3CreateObjectOperator(
        task_id="create-s3-object",
        s3_bucket=s3_bucket_name,
        s3_key=S3_KEY,
        data=b"Testing...",
        replace=True,
        encrypt=False
    )

    # Move a file from S3 to WASB
    s3_to_wasb = S3ToAzureBlobStorageOperator(
        task_id="s3-to-wasb",
        s3_bucket=s3_bucket_name,
        container_name=wasb_container_name,
        s3_key=S3_KEY,
        blob_prefix=BLOB_PREFIX,  # Using a prefix for this
        replace=True
    )

    # Remove an S3 bucket
    remove_s3_bucket = S3DeleteBucketOperator(task_id="remove-s3-bucket", bucket_name=s3_bucket_name)

    # Set dependencies
    chain(
        create_s3_bucket,
        create_wasb_container(wasb_container_name),
        create_s3_object,
        s3_to_wasb,
        remove_wasb_container(wasb_container_name),
        remove_s3_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure when "tearDown" task with trigger
    # rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
