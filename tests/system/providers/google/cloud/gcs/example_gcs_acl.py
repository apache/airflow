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
Example Airflow DAG for Google Cloud Storage ACL (Access Control List) operators.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "gcs_acl"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)

GCS_ACL_ENTITY = "allUsers"
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "acl", "example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        resource={
            "iamConfiguration": {
                "uniformBucketLevelAccess": {
                    "enabled": False,
                },
            },
        },
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_gcs_bucket_create_acl_entry_task]
    gcs_bucket_create_acl_entry_task = GCSBucketCreateAclEntryOperator(
        bucket=BUCKET_NAME,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_BUCKET_ROLE,
        task_id="gcs_bucket_create_acl_entry_task",
    )
    # [END howto_operator_gcs_bucket_create_acl_entry_task]

    # [START howto_operator_gcs_object_create_acl_entry_task]
    gcs_object_create_acl_entry_task = GCSObjectCreateAclEntryOperator(
        bucket=BUCKET_NAME,
        object_name=FILE_NAME,
        entity=GCS_ACL_ENTITY,
        role=GCS_ACL_OBJECT_ROLE,
        task_id="gcs_object_create_acl_entry_task",
    )
    # [END howto_operator_gcs_object_create_acl_entry_task]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_file
        # TEST BODY
        >> gcs_bucket_create_acl_entry_task
        >> gcs_object_create_acl_entry_task
        # TEST TEARDOWN,
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
