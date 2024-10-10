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

from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.transfers.gcs_to_s3 import GCSToS3Operator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

# Externally fetched variables:
GCP_PROJECT_ID = "GCP_PROJECT_ID"

sys_test_context_task = SystemTestContextBuilder().add_variable(GCP_PROJECT_ID).build()

DAG_ID = "example_gcs_to_s3"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]
    gcp_user_project = test_context[GCP_PROJECT_ID]

    s3_bucket = f"{env_id}-gcs-to-s3-bucket"
    s3_key = f"{env_id}-gcs-to-s3-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    gcs_bucket = f"{env_id}-gcs-to-s3-bucket"
    gcs_key = f"{env_id}-gcs-to-s3-key"

    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=gcs_bucket,
        resource={"billing": {"requesterPays": True}},
        project_id=gcp_user_project,
    )

    @task
    def upload_gcs_file(bucket_name: str, object_name: str, user_project: str):
        hook = GCSHook()
        with hook.provide_file_and_upload(
            bucket_name=bucket_name,
            object_name=object_name,
            user_project=user_project,
        ) as temp_file:
            temp_file.write(b"test")

    # [START howto_transfer_gcs_to_s3]
    gcs_to_s3 = GCSToS3Operator(
        task_id="gcs_to_s3",
        gcs_bucket=gcs_bucket,
        dest_s3_key=f"s3://{s3_bucket}/{s3_key}",
        replace=True,
        gcp_user_project=gcp_user_project,
    )
    # [END howto_transfer_gcs_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_gcs_bucket = GCSDeleteBucketOperator(
        task_id="delete_gcs_bucket",
        bucket_name=gcs_bucket,
        trigger_rule=TriggerRule.ALL_DONE,
        user_project=gcp_user_project,
    )

    chain(
        # TEST SETUP
        test_context,
        create_gcs_bucket,
        upload_gcs_file(gcs_bucket, gcs_key, gcp_user_project),
        create_s3_bucket,
        # TEST BODY
        gcs_to_s3,
        # TEST TEARDOWN
        delete_s3_bucket,
        delete_gcs_bucket,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
