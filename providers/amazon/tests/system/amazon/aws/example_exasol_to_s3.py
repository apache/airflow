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
Example DAG to test Exasol connection and S3 upload in Apache Airflow.

This DAG uses the ExasolToS3Operator to execute a simple SQL query on Exasol and
upload the result file to S3. It verifies that both the Exasol connection (`exasol_default`)
and S3 credential settings (`aws_default`) are working correctly.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.exasol_to_s3 import ExasolToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_exasol_to_s3"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 3, 11),
    schedule="@once",
    catchup=False,
    tags=["exasol", "s3", "test"],
) as dag:

    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket_name = f"{env_id}-bucket"
    s3_key = f"{env_id}/files/exasol-output.csv"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket",
        bucket_name=s3_bucket_name,
    )

    # [START howto_transfer_exasol_to_s3]
    exasol_to_s3 = ExasolToS3Operator(
        task_id="exasol_to_s3",
        query_or_table="SELECT 1 AS val",
        key=s3_key,
        bucket_name=s3_bucket_name,
        exasol_conn_id="exasol_default",
        aws_conn_id="aws_default",
        export_params={},
        query_params={},
        use_s3=True,
        replace=True,
        encrypt=False,
        gzip=False,
        acl_policy="private",
    )
    # [END howto_transfer_exasol_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_s3_bucket >> exasol_to_s3 >> delete_s3_bucket

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)