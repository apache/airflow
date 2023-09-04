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
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteObjectsOperator,
)
from airflow.providers.microsoft.azure.transfers.s3_to_azure_blob import S3ToAzureBlobOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

BLOB_NAME = os.environ.get("AZURE_BLOB_NAME", "file.txt")
AZURE_CONTAINER_NAME = os.environ.get("AZURE_CONTAINER_NAME", "airflow")
DAG_ID = "example_s3_to_azure_blob"
PREFIX_NAME = os.environ.get("AZURE_PREFIX_NAME", "20230421")

sys_test_context_task = SystemTestContextBuilder().build()
SAMPLE_DATA = "some data"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    default_args={"container_name": AZURE_CONTAINER_NAME, "blob_name": BLOB_NAME, "prefix": PREFIX_NAME},
):
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket = f"{env_id}-s3-to-azure-bucket"
    s3_key = f"{env_id}-s3-to-azure-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        data=SAMPLE_DATA,
        replace=True,
    )

    # [START how_to_s3_to_azure_blob]
    transfer_s3_to_azure_blob = S3ToAzureBlobOperator(
        task_id="transfer_s3_to_azure_blob",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        blob_name=BLOB_NAME,
        container_name=AZURE_CONTAINER_NAME,
        create_container=True,
    )
    # [END how_to_s3_to_azure_blob]

    delete_s3_object = S3DeleteObjectsOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id="delete_objects",
        bucket=s3_bucket,
        keys=s3_key,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        create_s3_bucket,
        create_object,
        transfer_s3_to_azure_blob,
        delete_s3_object,
        delete_s3_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
