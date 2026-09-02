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

from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.transfers.exasol_to_s3 import ExasolToS3Operator
from airflow.providers.common.compat.sdk import DAG, chain
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.exasol.hooks.exasol import exasol_fetch_all_handler

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]
from airflow.utils.timezone import datetime

from system.amazon.aws.utils import SystemTestContextBuilder

DAG_ID = "example_exasol_to_s3"

sys_test_context_task = SystemTestContextBuilder().build()

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket = f"{env_id}-exasol-to-s3-bucket"
    s3_key = f"{env_id}-exasol-to-s3-key"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    create_table_exasol = SQLExecuteQueryOperator(
        task_id="create_table_exasol",
        conn_id="exasol_default",
        handler=exasol_fetch_all_handler,
        sql="""
            CREATE OR REPLACE TABLE exasol_to_s3_example (
                a VARCHAR(100),
                b DECIMAL(18,0)
            );
        """,
    )

    insert_data_exasol = SQLExecuteQueryOperator(
        task_id="insert_data_exasol",
        conn_id="exasol_default",
        handler=exasol_fetch_all_handler,
        sql="""
            INSERT INTO exasol_to_s3_example (a, b)
            VALUES ('a', 1), ('a', 2), ('b', 3);
        """,
    )

    # [START howto_transfer_exasol_to_s3]
    exasol_to_s3_job = ExasolToS3Operator(
        task_id="exasol_to_s3_job",
        query_or_table="exasol_to_s3_example",
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True,
    )
    # [END howto_transfer_exasol_to_s3]

    drop_table_exasol = SQLExecuteQueryOperator(
        task_id="drop_table_exasol",
        conn_id="exasol_default",
        handler=exasol_fetch_all_handler,
        sql="DROP TABLE exasol_to_s3_example;",
        trigger_rule=TriggerRule.ALL_DONE,
    )

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
        create_table_exasol,
        insert_data_exasol,
        # TEST BODY
        exasol_to_s3_job,
        # TEST TEARDOWN
        drop_table_exasol,
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
