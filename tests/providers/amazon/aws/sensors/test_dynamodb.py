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

from unittest.mock import Mock, patch, PropertyMock

from airflow.providers.amazon.aws.sensors.dynamodb import DynamoDBValueSensor
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from moto import mock_dynamodb


class TestDynamoDBValueSensor:
    def setup_method(self):
        self.table_name = "test_airflow"
        self.key_name = "PK"
        self.key_value = "Test"
        self.attribute_name = "Foo"
        self.attribute_value = "Bar"

        self.sensor = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor",
            table_name=self.table_name,
            partition_key_name=self.key_name,
            partition_key_value=self.key_value,
            attribute_name=self.attribute_name,
            attribute_value=self.attribute_value,
        )

    @mock_dynamodb
    def test_sensor_with_pk(self):
        hook = DynamoDBHook(table_name=self.table_name, table_keys=[self.key_name])

        hook.conn.create_table(
            TableName=self.table_name,
            KeySchema=[{"AttributeName": self.key_name, "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": self.key_name, "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        items = [{self.key_name: self.key_value, self.attribute_name: self.attribute_value}]
        hook.write_batch_data(items)

        assert self.sensor.poke({})

        mock_conn.Table = Mock()
        mock_conn.Table.get_item = Mock(return_value=response)

        assert self.sensor.poke(None)
        
