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

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_dynamodbvaluesensor"
sys_test_context_task = SystemTestContextBuilder().build()

TABLE_ATTRIBUTES = [
    {"AttributeName": "PK", "AttributeType": "S"},
    {"AttributeName": "SK", "AttributeType": "S"},
]
TABLE_KEY_SCHEMA = [
    {"AttributeName": "PK", "KeyType": "HASH"},
    {"AttributeName": "SK", "KeyType": "RANGE"},
]
TABLE_THROUGHPUT = {"ReadCapacityUnits": 10, "WriteCapacityUnits": 10}


@task
def create_table(table_name: str):
    ddb = boto3.resource("dynamodb")
    table = ddb.create_table(
        AttributeDefinitions=TABLE_ATTRIBUTES,
        TableName=table_name,
        KeySchema=TABLE_KEY_SCHEMA,
        ProvisionedThroughput=TABLE_THROUGHPUT,
    )
    boto3.client("dynamodb").get_waiter("table_exists").wait()
    table.put_item(Item={"PK": "Test", "SK": "2022-07-12T11:11:25-0400", "Value": "Testing"})


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_table(table_name: str):
    ddb = boto3.resource("dynamodb")
    ddb.delete_table(TableName=table_name)
    boto3.client("dynamodb").get_waiter("table_not_exists").wait()


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    table_name = f"{env_id}-dynamodb-table"
    create_table = create_table(table_name=table_name)

    # [START howto_sensor_dynamodb]
    dynamodb_sensor = DynamoDBValueSensor(
        task_id="waiting_for_dynamodb_item_value",
        poke_interval=30,
        timeout=120,
        soft_fail=False,
        retries=10,
        table_name="AirflowSensorTest",
        partition_key_name="PK",
        partition_key_value="Test",
        sort_key_name="SK",
        sort_key_value="2022-07-12T11:11:25-0400",
        attribute_name="Value",
        attribute_value="Testing",
    )
    # [END howto_sensor_dynamodb]

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_table,
        dynamodb_sensor,
        # TEST TEARDOWN
        delete_table,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
