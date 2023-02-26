#
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
#
from __future__ import annotations

import unittest
from datetime import datetime, timedelta
from unittest import mock

from airflow import AirflowException
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor

TASK_ID = "db-sensor"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"
DEFAULT_SQL_ENDPOINT = "sql_warehouse_default"

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)

sql_sensor = DatabricksSqlSensor(
    databricks_conn_id=DEFAULT_CONN_ID,
    sql_endpoint_name=DEFAULT_SQL_ENDPOINT,
    task_id=TASK_ID,
    schema=DEFAULT_SCHEMA,
    catalog=DEFAULT_CATALOG,
    sql="select 1 from catalog1.schema1.table1",
)


class TestDatabricksSqlSensor(unittest.TestCase):
    @mock.patch.object(DatabricksSqlSensor, "_sql_sensor")
    def test_poke_changes_success(self, mock_sql_sensor):
        mock_sql_sensor.return_value = [1]
        assert sql_sensor.poke({}) is True

    @mock.patch.object(DatabricksSqlSensor, "_sql_sensor")
    def test_poke_changes_failure(self, mock_sql_sensor):
        mock_sql_sensor.return_value = []
        assert sql_sensor.poke({}) is False
