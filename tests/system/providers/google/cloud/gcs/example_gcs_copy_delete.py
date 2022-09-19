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
Example Airflow DAG for Google Cloud Storage operators for listing, copying and deleting
bucket content.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSDeleteObjectsOperator,
    GCSListObjectsOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "gcs_copy_delete"

BUCKET_NAME_SRC = f"bucket_{DAG_ID}_{ENV_ID}"
BUCKET_NAME_DST = f"bucket_dst_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "example"],
) as dag:
    create_bucket_src = GCSCreateBucketOperator(
        task_id="create_bucket_src",
        bucket_name=BUCKET_NAME_SRC,
        project_id=PROJECT_ID,
    )

    create_bucket_dst = GCSCreateBucketOperator(
        task_id="create_bucket_dst",
        bucket_name=BUCKET_NAME_DST,
        project_id=PROJECT_ID,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME_SRC,
    )

    # [START howto_operator_gcs_list_bucket]
    list_buckets = GCSListObjectsOperator(task_id="list_buckets", bucket=BUCKET_NAME_SRC)
    # [END howto_operator_gcs_list_bucket]

    list_buckets_result = BashOperator(
        task_id="list_buckets_result",
        bash_command=f"echo {list_buckets.output}",
    )

    copy_file = GCSToGCSOperator(
        task_id="copy_file",
        source_bucket=BUCKET_NAME_SRC,
        source_object=FILE_NAME,
        destination_bucket=BUCKET_NAME_DST,
        destination_object=FILE_NAME,
    )

    # [START howto_operator_gcs_delete_object]
    delete_files = GCSDeleteObjectsOperator(
        task_id="delete_files", bucket_name=BUCKET_NAME_SRC, objects=[FILE_NAME]
    )
    # [END howto_operator_gcs_delete_object]

    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src", bucket_name=BUCKET_NAME_SRC, trigger_rule=TriggerRule.ALL_DONE
    )
    delete_bucket_dst = GCSDeleteBucketOperator(
        task_id="delete_bucket_dst", bucket_name=BUCKET_NAME_DST, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        [create_bucket_src, create_bucket_dst],
        upload_file,
        # TEST BODY
        list_buckets,
        list_buckets_result,
        copy_file,
        delete_files,
        # TEST TEARDOWN
        [delete_bucket_src, delete_bucket_dst],
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
