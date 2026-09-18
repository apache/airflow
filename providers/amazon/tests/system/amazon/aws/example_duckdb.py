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

import boto3

from airflow.providers.amazon.aws.hooks.duckdb import AwsDuckDBHook
from airflow.providers.amazon.aws.operators.duckdb import AwsDuckDBOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import DAG, chain, task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import chain  # type: ignore[attr-defined,no-redef]
    from airflow.models.dag import DAG  # type: ignore[attr-defined,no-redef,assignment]

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_duckdb"

SAMPLE_DATA = """category,price,quantity
widgets,9.99,10
widgets,19.99,3
gadgets,4.50,20
"""
SAMPLE_FILENAME = "sales.csv"


@task
def await_bucket(bucket_name):
    # Avoid a race condition after creating the S3 Bucket.
    waiter = boto3.client("s3").get_waiter("bucket_exists")
    waiter.wait(Bucket=bucket_name)


@task
def verify_summary(bucket_name):
    """Read the summary back through the hook to prove the round trip to S3 worked."""
    hook = AwsDuckDBHook()
    with hook.get_conn() as conn:
        rows = conn.execute(
            f"SELECT category, revenue FROM read_parquet('s3://{bucket_name}/summary/revenue.parquet')"
            " ORDER BY revenue DESC"
        ).fetchall()

    if [row[0] for row in rows] != ["widgets", "gadgets"]:
        raise ValueError(f"Unexpected summary contents: {rows}")


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket = f"{env_id}-duckdb-bucket"

    create_s3_bucket = S3CreateBucketOperator(task_id="create_s3_bucket", bucket_name=s3_bucket)

    upload_sample_data = S3CreateObjectOperator(
        task_id="upload_sample_data",
        s3_bucket=s3_bucket,
        s3_key=f"sales/{SAMPLE_FILENAME}",
        data=SAMPLE_DATA,
        replace=True,
    )

    # [START howto_operator_aws_duckdb]
    summarize_sales = AwsDuckDBOperator(
        task_id="summarize_sales",
        sql=f"""
            COPY (
                SELECT category, SUM(price * quantity) AS revenue
                FROM read_csv('s3://{s3_bucket}/sales/*.csv')
                GROUP BY category
                ORDER BY revenue DESC
            ) TO 's3://{s3_bucket}/summary/revenue.parquet' (FORMAT PARQUET)
        """,
    )
    # [END howto_operator_aws_duckdb]

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
        await_bucket(s3_bucket),
        upload_sample_data,
        # TEST BODY
        summarize_sales,
        verify_summary(s3_bucket),
        # TEST TEARDOWN
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
