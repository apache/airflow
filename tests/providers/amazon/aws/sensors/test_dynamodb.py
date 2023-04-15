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

from moto import mock_dynamodb

from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor


class TestDynamoDBValueSensor:
    def setup_method(self):
        self.table_name = "test_airflow"
        self.pk_name = "PK"
        self.pk_value = "PKTest"
        self.sk_name = "SK"
        self.sk_value = "SKTest"
        self.attribute_name = "Foo"
        self.attribute_value = "Bar"

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

    @mock_dynamodb
    def test_sensor_with_pk(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.pk_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.pk_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": self.pk_name, "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        assert not self.sensor.poke(None)

        items = [{self.pk_name: self.pk_value, self.attribute_name: self.attribute_value}]
        hook.write_batch_data(items)

        assert self.sensor.poke(None)

    @mock_dynamodb
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
                self.attribute_name: self.attribute_value,
            }
        ]
        hook.write_batch_data(items)

        assert self.sensor.poke(None)
