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

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor

pytestmark = pytest.mark.db_test


class TestDynamoDBValueSensor:
    def setup_method(self):
        self.table_name = "test_airflow"
        self.pk_name = "PK"
        self.pk_value = "PKTest"
        self.sk_name = "SK"
        self.sk_value = "SKTest"
        self.attribute_name = "Foo"
        self.attribute_value = "Bar"

        self.sensor_pk_sk = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor",
            table_name=self.table_name,
            partition_key_name=self.pk_name,
            partition_key_value=self.pk_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
            sort_key_name=self.sk_name,
            sort_key_value=self.sk_value,
        )

        self.sensor_pk = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor",
            table_name=self.table_name,
            partition_key_name=self.pk_name,
            partition_key_value=self.pk_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
        )

    @mock_aws
    def test_sensor_with_pk(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.pk_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.pk_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": self.pk_name, "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        assert not self.sensor_pk.poke(None)

        items = [{self.pk_name: self.pk_value, self.attribute_name: self.attribute_value}]
        hook.write_batch_data(items)

        assert self.sensor_pk.poke(None)

    @mock_aws
    def test_sensor_with_pk_and_sk(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.pk_name, self.sk_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": self.pk_name, "KeyType": "HASH"},
                {"AttributeName": self.sk_name, "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": self.pk_name, "AttributeType": "S"},
                {"AttributeName": self.sk_name, "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        assert not self.sensor_pk_sk.poke(None)

        items = [
            {
                self.pk_name: self.pk_value,
                self.sk_name: self.sk_value,
                self.attribute_name: self.attribute_value,
            }
        ]
        hook.write_batch_data(items)

        assert self.sensor_pk_sk.poke(None)

    @mock_aws
    def test_sensor_with_client_error(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.pk_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.pk_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": self.pk_name, "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        items = [{self.pk_name: self.pk_value, self.attribute_name: self.attribute_value}]
        hook.write_batch_data(items)

        self.sensor_pk.partition_key_name = "no such key"
        assert self.sensor_pk.poke(None) is False


class TestDynamoDBMultipleValuesSensor:
    def setup_method(self):
        self.table_name = "test_airflow"
        self.pk_name = "PK"
        self.pk_value = "PKTest"
        self.sk_name = "SK"
        self.sk_value = "SKTest"
        self.attribute_name = "Foo"
        self.attribute_value = ["Bar1", "Bar2", "Bar3"]

        self.sensor = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor",
            table_name=self.table_name,
            partition_key_name=self.pk_name,
            partition_key_value=self.pk_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
            sort_key_name=self.sk_name,
            sort_key_value=self.sk_value,
        )
        self.sensor_pk = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor",
            table_name=self.table_name,
            partition_key_name=self.pk_name,
            partition_key_value=self.pk_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
        )

    def test_init(self):
        sensor = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor_init",
            table_name=self.table_name,
            partition_key_name=self.pk_name,
            partition_key_value=self.pk_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
            sort_key_name=self.sk_name,
            sort_key_value=self.sk_value,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="cn-north-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert sensor.hook.client_type is None
        assert sensor.hook.resource_type == "dynamodb"
        assert sensor.hook.aws_conn_id == "fake-conn-id"
        assert sensor.hook._region_name == "cn-north-1"
        assert sensor.hook._verify is False
        assert sensor.hook._config is not None
        assert sensor.hook._config.read_timeout == 42

        sensor = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor_init",
            table_name=self.table_name,
            partition_key_name=self.pk_name,
            partition_key_value=self.pk_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
            sort_key_name=self.sk_name,
            sort_key_value=self.sk_value,
        )
        assert sensor.hook.aws_conn_id == "aws_default"
        assert sensor.hook._region_name is None
        assert sensor.hook._verify is None
        assert sensor.hook._config is None

    @mock_aws
    def test_sensor_with_pk(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.pk_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.pk_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": self.pk_name, "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        assert not self.sensor_pk.poke(None)

        items = [{self.pk_name: self.pk_value, self.attribute_name: self.attribute_value[1]}]
        hook.write_batch_data(items)

        assert self.sensor_pk.poke(None)

    @mock_aws
    def test_sensor_with_pk_and_sk(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.pk_name, self.sk_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[
                {"AttributeName": self.pk_name, "KeyType": "HASH"},
                {"AttributeName": self.sk_name, "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": self.pk_name, "AttributeType": "S"},
                {"AttributeName": self.sk_name, "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        assert not self.sensor.poke(None)

        items = [
            {
                self.pk_name: self.pk_value,
                self.sk_name: self.sk_value,
                self.attribute_name: self.attribute_value[1],
            }
        ]
        hook.write_batch_data(items)

        assert self.sensor.poke(None)
