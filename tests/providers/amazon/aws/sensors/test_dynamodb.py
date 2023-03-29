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

from unittest import mock
from unittest.mock import MagicMock
from moto import mock_dynamodb

from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor

AWS_CONN_ID = "aws_default"
REGION_NAME = "us-east-1"
TABLE_NAME = "test_airflow"
TASK_ID = "dynamodb_value_sensor"


class TestDynamoDBValueSensor:
    def setup_method(self):
        self.mock_context = MagicMock()

    def test_init(self):
        sensor = DynamoDBValueSensor(
            task_id=TASK_ID,
            table_name=TABLE_NAME,
            partition_key_name="PK",
            partition_key_value="Test",
            sort_key_name="SK",
            sort_key_value="2022-07-12T11:11:25-0400",
            attribute_name="Foo",
            attribute_value="Bar",
            aws_conn_id=AWS_CONN_ID,
            region_name=REGION_NAME,
        )

        assert TASK_ID == sensor.task_id
        assert AWS_CONN_ID == sensor.aws_conn_id
        assert REGION_NAME == sensor.region_name

    @mock_dynamodb
    def test_conn_returns_a_boto3_connection(self):
        hook = DynamoDBHook(aws_conn_id=AWS_CONN_ID)
        assert hook.conn is not None

    @mock_dynamodb
    @mock.patch("airflow.providers.amazon.aws.sensors.dynamodb.DynamoDBHook")
    def test_sensor_with_pk_and_sk(self, ddb_mock):

        hook = DynamoDBHook(
            aws_conn_id=AWS_CONN_ID, table_name=TABLE_NAME, table_keys=["PK"], region_name=REGION_NAME
        )

        hook.conn.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "PK", "KeyType": "HASH"},
                {"AttributeName": "SK", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "PK", "AttributeType": "S"},
                {"AttributeName": "SK", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        table = hook.conn.Table(TABLE_NAME)
        table.meta.client.get_waiter("table_exists").wait(TableName=TABLE_NAME)

        assert table.table_status == "ACTIVE"

        sensor = DynamoDBValueSensor(
            task_id=TASK_ID,
            poke_interval=30,
            timeout=120,
            soft_fail=False,
            retries=10,
            table_name=TABLE_NAME,  # replace with your table name
            partition_key_name="PK",  # replace with your partition key name
            partition_key_value="Test",  # replace with your partition key value
            sort_key_name="SK",  # replace with your sort key name (if applicable)
            sort_key_value="2023-03-28T11:11:25-0400",  # replace with your sort key value (if applicable)
            attribute_name="Foo",  # replace with the attribute name to wait for
            attribute_value="Bar",  # replace with the attribute value to wait for (sensor will return true when this value matches the attribute value in the item)
        )

        assert not sensor.poke(None)

        table.put_item(Item={"PK": "123", "SK": "2023-03-28T11:11:25-0400", "Foo": "Bar"})

        assert sensor.poke(None)
