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

import logging
from datetime import datetime

import boto3

from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.transfers.s3_to_dynamodb import S3ToDynamoDBOperator

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

from system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

log = logging.getLogger(__name__)

DAG_ID = "example_s3_to_dynamodb"

sys_test_context_task = SystemTestContextBuilder().build()

TABLE_ATTRIBUTES = [
    {"AttributeName": "cocktail_id", "AttributeType": "S"},
]
TABLE_KEY_SCHEMA = [
    {"AttributeName": "cocktail_id", "KeyType": "HASH"},
]
TABLE_THROUGHPUT = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}

SAMPLE_DATA = r"""cocktail_id,cocktail_name,base_spirit
1,Caipirinha,Cachaca
2,Bramble,Gin
3,Daiquiri,Rum
"""


@task
def set_up_table(table_name: str):
    dynamo_resource = boto3.resource("dynamodb")
    dynamo_resource.create_table(
        AttributeDefinitions=TABLE_ATTRIBUTES,
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        ProvisionedThroughput=TABLE_THROUGHPUT,
    )
    boto3.client("dynamodb").get_waiter("table_exists").wait(
        TableName=table_name, WaiterConfig={"Delay": 10, "MaxAttempts": 10}
    )


@task
def wait_for_bucket(s3_bucket_name):
    waiter = boto3.client("s3").get_waiter("bucket_exists")
    waiter.wait(Bucket=s3_bucket_name)


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_dynamodb_table(table_name: str):
    boto3.resource("dynamodb").Table(table_name).delete()
    boto3.client("dynamodb").get_waiter("table_not_exists").wait(
        TableName=table_name, WaiterConfig={"Delay": 10, "MaxAttempts": 30}
    )


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    existing_table_name = f"{env_id}-ex-dynamodb-table"
    new_table_name = f"{env_id}-new-dynamodb-table"
    bucket_name = f"{env_id}-dynamodb-bucket"
    s3_key = f"{env_id}/files/cocktail_list.csv"

    create_table = set_up_table(table_name=existing_table_name)

    create_bucket = S3CreateBucketOperator(task_id="create_bucket", bucket_name=bucket_name)

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=s3_key,
        data=SAMPLE_DATA,
        replace=True,
    )

    # [START howto_transfer_s3_to_dynamodb]
    transfer_1 = S3ToDynamoDBOperator(
        task_id="s3_to_dynamodb",
        s3_bucket=bucket_name,
        s3_key=s3_key,
        dynamodb_table_name=new_table_name,
        input_format="CSV",
        import_table_kwargs={
            "InputFormatOptions": {
                "Csv": {
                    "Delimiter": ",",
                }
            }
        },
        dynamodb_attributes=[
            {"AttributeName": "cocktail_id", "AttributeType": "S"},
        ],
        dynamodb_key_schema=[
            {"AttributeName": "cocktail_id", "KeyType": "HASH"},
        ],
    )
    # [END howto_transfer_s3_to_dynamodb]

    # [START howto_transfer_s3_to_dynamodb_existing_table]
    transfer_2 = S3ToDynamoDBOperator(
        task_id="s3_to_dynamodb_new_table",
        s3_bucket=bucket_name,
        s3_key=s3_key,
        dynamodb_table_name=existing_table_name,
        use_existing_table=True,
        input_format="CSV",
        import_table_kwargs={
            "InputFormatOptions": {
                "Csv": {
                    "Delimiter": ",",
                }
            }
        },
        dynamodb_attributes=[
            {"AttributeName": "cocktail_id", "AttributeType": "S"},
        ],
        dynamodb_key_schema=[
            {"AttributeName": "cocktail_id", "KeyType": "HASH"},
        ],
    )
    # [END howto_transfer_s3_to_dynamodb_existing_table]

    delete_existing_table = delete_dynamodb_table(table_name=existing_table_name)
    delete_new_table = delete_dynamodb_table(table_name=new_table_name)

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        trigger_rule=TriggerRule.ALL_DONE,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        create_table,
        create_bucket,
        wait_for_bucket(s3_bucket_name=bucket_name),
        create_object,
        # TEST BODY
        transfer_1,
        transfer_2,
        # TEST TEARDOWN
        delete_existing_table,
        delete_new_table,
        delete_bucket,
    )
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
