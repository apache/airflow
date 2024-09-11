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

from unittest.mock import MagicMock, patch

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mongo.sensors.mongo import MongoSensor
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestMongoSensor:
    def setup_method(self, method):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.mock_context = MagicMock()

    @patch.object(MongoHook, "_create_uri", return_value="mocked_uri")
    @patch("airflow.providers.mongo.hooks.mongo.MongoHook.find")
    @patch("airflow.providers.mongo.hooks.mongo.MongoHook.get_connection")
    def test_execute_operator(self, mock_mongo_conn, mock_mongo_find, mock_create_uri):
        mock_mongo_find.return_value = {"test_key": "test"}
        mock_connection = MagicMock(spec=Connection)
        mock_connection.conn_type = "mongo"

        mock_extra_dejson = {"ssl": "false", "srv": "false"}
        mock_connection.extra_dejson = MagicMock(spec=dict)
        mock_connection.extra_dejson.copy.return_value = mock_extra_dejson
        mock_mongo_conn.return_value = mock_connection

        sensor = MongoSensor(
            collection="coll",
            query={"test_key": "test"},
            mongo_conn_id="mongo_default",
            mongo_db="test_db",
            task_id="test_task",
            dag=self.dag,
        )

        result = sensor.poke(self.mock_context)

        mock_mongo_conn.assert_called_once_with("mongo_default")
        assert result is True
        mock_mongo_find.assert_called_once_with(
            "coll", {"test_key": "test"}, mongo_db="test_db", find_one=True
        )
