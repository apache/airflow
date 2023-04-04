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
        self.sensor = DynamoDBValueSensor(
            task_id="dynamodb_value_sensor",
            poke_interval=30,
            timeout=120,
            soft_fail=False,
            retries=10,
            table_name="test_airflow",
            partition_key_name="PK",
            partition_key_value="Test",
            sort_key_name="SK",
            sort_key_value="2023-03-28T11:11:25-0400",
            attribute_name="Foo",
            attribute_value="Bar",
        )

        self.ddb_hook = DynamoDBHook()
        self.sensor.hook = self.ddb_hook

    @mock_dynamodb
    @patch("airflow.providers.amazon.aws.hooks.dynamodb.DynamoDBHook.conn", new_callable=PropertyMock)
    def test_sensor_with_pk_and_sk(self, mock_conn):
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "Item": {"PK": "Test", "SK": "2023-03-28T11:11:25-0400", "Foo": "Bar"},
        }

        mock_conn.Table = Mock()
        mock_conn.Table.get_item = Mock(return_value=response)

        assert self.sensor.poke(None)
