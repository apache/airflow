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

from unittest.mock import patch

from airflow.models.dag import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.mongo.sensors.mongo import MongoSensor
from airflow.utils import timezone

from tests_common.test_utils.compat import Context

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestMongoSensor:
    def setup_method(self, method):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}

        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.context = Context()

    @patch("airflow.providers.mongo.sensors.mongo.MongoHook", spec=MongoHook)
    def test_execute_operator(self, mock_mongo_hook_constructor):
        test_collection = "coll"
        test_query = {"test_key": "test"}
        test_conn_id = "mongo_default"
        test_db = "test_db"

        mock_mongo_hook_find = mock_mongo_hook_constructor.return_value.find
        mock_mongo_hook_find.return_value = {"document_key": "document_val"}

        sensor = MongoSensor(
            collection=test_collection,
            query=test_query,
            mongo_conn_id=test_conn_id,
            mongo_db=test_db,
            task_id="test_task",
            dag=self.dag,
        )

        result = sensor.poke(self.context)

        mock_mongo_hook_constructor.assert_called_once_with(mongo_conn_id=test_conn_id)
        assert result is True
        mock_mongo_hook_find.assert_called_once_with(
            test_collection, test_query, mongo_db=test_db, find_one=True
        )
