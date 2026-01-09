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
from pathlib import Path

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from system.openlineage.operator import OpenLineageTestOperator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "example_s3_to_gcs"

RESOURCES_BUCKET_NAME = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
GCS_BUCKET_URL = f"gs://{BUCKET_NAME}/"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE = f"gcs/{FILE_NAME}"
PREFIX = "gcs"


@task(task_id="upload_file_to_s3")
def upload_file():
    """A callable to upload file from GCS to AWS bucket"""
    gcs_hook = GCSHook()
    s3_hook = S3Hook()
    with gcs_hook.provide_file(bucket_name=RESOURCES_BUCKET_NAME, object_name=UPLOAD_FILE) as gcs_file:
        s3_hook.load_file_obj(file_obj=gcs_file, key=UPLOAD_FILE, bucket_name=BUCKET_NAME)


with DAG(
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
        task_id="s3_to_gcs_task",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        dest_gcs=GCS_BUCKET_URL,
        apply_gcs_prefix=True,
    )
    # [END howto_transfer_s3togcs_operator]

    # [START howto_transfer_s3togcs_operator_async]
    transfer_to_gcs_def = S3ToGCSOperator(
        task_id="s3_to_gcs_task_def",
        bucket=BUCKET_NAME,
        prefix=PREFIX,
        dest_gcs=GCS_BUCKET_URL,
        deferrable=True,
    )
    # [END howto_transfer_s3togcs_operator_async]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=BUCKET_NAME,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_gcs_bucket = GCSDeleteBucketOperator(
        task_id="delete_gcs_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "s3_to_gcs.json"),
    )

    (
        # TEST SETUP
        create_gcs_bucket
        >> create_s3_bucket
        >> upload_file()
        # TEST BODY
        >> [transfer_to_gcs, transfer_to_gcs_def]
        # TEST TEARDOWN
        >> delete_s3_bucket
        >> delete_gcs_bucket
        >> check_openlineage_events
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
