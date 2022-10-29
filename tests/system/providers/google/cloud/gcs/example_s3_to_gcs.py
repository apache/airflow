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

from airflow import models
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "example_s3_to_gcs"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
GCS_BUCKET_URL = f"gs://{BUCKET_NAME}/"
UPLOAD_FILE = "/tmp/example-file.txt"
PREFIX = "TESTS"


@task(task_id="upload_file_to_s3")
def upload_file():
    """A callable to upload file to AWS bucket"""
    s3_hook = S3Hook()
    s3_hook.load_file(filename=UPLOAD_FILE, key=PREFIX, bucket_name=BUCKET_NAME)


with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "s3"],
) as dag:
    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=BUCKET_NAME, region_name="us-east-1"
    )

    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=GCP_PROJECT_ID,
    )
    # [START howto_transfer_s3togcs_operator]
    transfer_to_gcs = S3ToGCSOperator(
        task_id="s3_to_gcs_task", bucket=BUCKET_NAME, prefix=PREFIX, dest_gcs=GCS_BUCKET_URL
    )
    # [END howto_transfer_s3togcs_operator]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=BUCKET_NAME,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_gcs_bucket = GCSDeleteBucketOperator(
        task_id="delete_gcs_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_gcs_bucket
        >> create_s3_bucket
        >> upload_file()
        # TEST BODY
        >> transfer_to_gcs
        # TEST TEARDOWN
        >> delete_s3_bucket
        >> delete_gcs_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
