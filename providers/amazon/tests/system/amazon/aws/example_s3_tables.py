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

from airflow.providers.amazon.aws.operators.s3_tables import (
    S3TablesCreateNamespaceOperator,
    S3TablesCreateTableBucketOperator,
    S3TablesCreateTableOperator,
    S3TablesDeleteNamespaceOperator,
    S3TablesDeleteTableBucketOperator,
    S3TablesDeleteTableOperator,
)
from airflow.providers.common.compat.sdk import DAG, chain

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import TriggerRule, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_s3_tables"

sys_test_context_task = SystemTestContextBuilder().build()

SCHEMA = {
    "iceberg": {
        "schema": {
            "fields": [
                {"name": "id", "type": "int", "required": True},
                {"name": "name", "type": "string", "required": False},
            ]
        }
    }
}

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    bucket_name = f"{env_id}-s3tables"
    namespace = f"{env_id}_ns"
    table_name = f"{env_id}_tbl"

    @task
    def create_namespace(table_bucket_arn: str, namespace: str):
        """Create a namespace in the table bucket."""
        import boto3

        boto3.client("s3tables").create_namespace(tableBucketARN=table_bucket_arn, namespace=[namespace])

    # [START howto_operator_s3tables_create_table_bucket]
    create_table_bucket = S3TablesCreateTableBucketOperator(
        task_id="create_table_bucket",
        table_bucket_name=bucket_name,
    )
    # [END howto_operator_s3tables_create_table_bucket]
    # [START howto_operator_s3tables_create_namespace]
    setup_namespace = S3TablesCreateNamespaceOperator(
        task_id="create_namespace",
        table_bucket_arn=create_table_bucket.output,
        namespace=namespace,
    )
    # [END howto_operator_s3tables_create_namespace]

    # [START howto_operator_s3tables_create_table]
    create_table = S3TablesCreateTableOperator(
        task_id="create_table",
        table_bucket_arn=create_table_bucket.output,
        namespace=namespace,
        table_name=table_name,
        metadata=SCHEMA,
    )
    # [END howto_operator_s3tables_create_table]

    # [START howto_operator_s3tables_delete_table]
    delete_table = S3TablesDeleteTableOperator(
        task_id="delete_table",
        table_bucket_arn=create_table_bucket.output,
        namespace=namespace,
        table_name=table_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_s3tables_delete_table]

    # [START howto_operator_s3tables_delete_table_bucket]
    delete_table_bucket = S3TablesDeleteTableBucketOperator(
        task_id="delete_table_bucket",
        table_bucket_arn=create_table_bucket.output,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_s3tables_delete_table_bucket]

    # [START howto_operator_s3tables_delete_namespace]
    delete_namespace = S3TablesDeleteNamespaceOperator(
        task_id="delete_namespace",
        table_bucket_arn=create_table_bucket.output,
        namespace=namespace,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_s3tables_delete_namespace]

    chain(
        # TEST SETUP
        test_context,
        create_table_bucket,
        setup_namespace,
        # TEST BODY
        create_table,
        # TEST TEARDOWN
        delete_table,
        delete_namespace,
        delete_table_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
