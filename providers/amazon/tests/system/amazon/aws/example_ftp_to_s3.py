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

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.ftp_to_s3 import FTPToS3Operator
from airflow.providers.common.compat.sdk import DAG, chain

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_ftp_to_s3"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket = f"{env_id}-ftp-to-s3-bucket"
    s3_key = f"{env_id}-ftp-to-s3-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    # [START howto_transfer_ftp_to_s3]
    ftp_to_s3_task = FTPToS3Operator(
        task_id="ftp_to_s3_task",
        ftp_path="/tmp/ftp_path",
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        replace=True,
    )
    # [END howto_transfer_ftp_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        # TEST BODY
        ftp_to_s3_task,
        # TEST TEARDOWN
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
