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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3ReadObjectOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_s3_read_object"

sys_test_context_task = SystemTestContextBuilder().build()

TEST_DATA = "hello world - system test content for S3ReadObjectOperator"

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    bucket_name = f"{env_id}-s3-read-object"
    s3_key = f"{env_id}-test-read.txt"

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=s3_key,
        data=TEST_DATA,
        replace=True,
    )

    # [START howto_operator_s3_read_object]
    read_object = S3ReadObjectOperator(
        task_id="read_object",
        s3_bucket=bucket_name,
        s3_key=s3_key,
    )
    # [END howto_operator_s3_read_object]

    # [START howto_operator_s3_read_object_url]
    read_object_url = S3ReadObjectOperator(
        task_id="read_object_url",
        s3_key=f"s3://{bucket_name}/{s3_key}",
    )
    # [END howto_operator_s3_read_object_url]

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
    )
    delete_bucket.trigger_rule = TriggerRule.ALL_DONE

    chain(
        test_context,
        create_bucket,
        create_object,
        [read_object, read_object_url],
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
