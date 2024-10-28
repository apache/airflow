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

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GCSBucketCreateAclEntryOperator,
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSObjectCreateAclEntryOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)
GCS_ACL_ENTITY = os.environ.get("SYSTEM_TESTS_GCS_ACL_ENTITY", "allUsers")

DAG_ID = "gcs_acl"

RESOURCES_BUCKET_NAME = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "example_upload.txt"
UPLOAD_FILE_PATH = f"gcs/{FILE_NAME}"

GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "acl", "example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        resource={"predefined_acl": "public_read_write"},
    )

    copy_file = GCSToGCSOperator(
        task_id="copy_example_gcs_file",
        source_bucket=RESOURCES_BUCKET_NAME,
        source_object=UPLOAD_FILE_PATH,
        destination_bucket=BUCKET_NAME,
        destination_object=FILE_NAME,
        exact_match=True,
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
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket
        >> copy_file
        # TEST BODY
        >> gcs_bucket_create_acl_entry_task
        >> gcs_object_create_acl_entry_task
        # TEST TEARDOWN,
        >> delete_bucket
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
