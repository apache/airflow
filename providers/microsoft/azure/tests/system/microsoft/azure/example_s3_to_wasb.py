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
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteObjectsOperator,
)
from airflow.providers.microsoft.azure.transfers.s3_to_wasb import S3ToAzureBlobStorageOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

# Set constants
DAG_ID: str = "example_s3_to_wasb"
S3_PREFIX: str = "TEST"
S3_KEY: str = "TEST/TEST1.csv"
BLOB_PREFIX: str = "TEST"
BLOB_NAME: str = "TEST/TEST1.csv"


with DAG(dag_id=DAG_ID, start_date=datetime(2024, 1, 1), schedule="@once", catchup=False) as dag:
    # Pull the task context, as well as the ENV_ID
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    # Create the bucket name and container name using the ENV_ID
    s3_bucket_name: str = f"{env_id}-s3-bucket"
    wasb_container_name: str = f"{env_id}-wasb-container"

    # Create an S3 bucket as part of testing set up, which will be removed by the remove_s3_bucket during
    # teardown
    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket_name)

    # Add a file to the S3 bucket created above. Part of testing set up, this file will eventually be removed
    # by the remove_s3_object task during teardown
    create_s3_object = S3CreateObjectOperator(
        task_id="create_s3_object",
        s3_bucket=s3_bucket_name,
        s3_key=S3_KEY,
        data=b"Testing...",
        replace=True,
        encrypt=False,
    )

    # [START howto_transfer_s3_to_wasb]
    s3_to_wasb = S3ToAzureBlobStorageOperator(
        task_id="s3_to_wasb",
        s3_bucket=s3_bucket_name,
        container_name=wasb_container_name,
        s3_key=S3_KEY,
        blob_prefix=BLOB_PREFIX,  # Using a prefix for this
        trigger_rule=TriggerRule.ALL_DONE,
        replace=True,
    )
    # [END howto_transfer_s3_to_wasb]

    # Part of tear down, remove all the objects at the S3_PREFIX
    remove_s3_object = S3DeleteObjectsOperator(
        task_id="remove_s3_object", bucket=s3_bucket_name, prefix=S3_PREFIX, trigger_rule=TriggerRule.ALL_DONE
    )

    # Remove the S3 bucket created as part of setup
    remove_s3_bucket = S3DeleteBucketOperator(
        task_id="remove_s3_bucket",
        bucket_name=s3_bucket_name,  # Force delete?
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Set dependencies
    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        create_s3_object,
        # TEST BODY
        s3_to_wasb,
        # TEST TEARDOWN
        remove_s3_object,
        remove_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure when "tearDown" task with trigger
    # rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
