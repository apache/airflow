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

from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.databricks.sensors.sql import DatabricksSqlSensor
from airflow.utils import timezone

TASK_ID = "db-sensor"
DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
HOST_WITH_SCHEME = "https://xx.cloud.databricks.com"
TOKEN = "token"

DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"
DEFAULT_SQL_ENDPOINT = "sql_warehouse_default"
DEFAULT_CALLER = "TestDatabricksSqlSensor"
DEFAULT_SQL = f"select 1 from {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{DEFAULT_TABLE} LIMIT 1"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)


class TestDatabricksSqlSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)

        self.sensor = DatabricksSqlSensor(
            task_id=TASK_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            sql_endpoint_name=DEFAULT_SQL_ENDPOINT,
            dag=self.dag,
            sql=DEFAULT_SQL,
            schema=DEFAULT_SCHEMA,
            catalog=DEFAULT_CATALOG,
            timeout=30,
            poke_interval=15,
        )

    def test_init(self):
        assert self.sensor.databricks_conn_id == "databricks_default"
        assert self.sensor.task_id == "db-sensor"
        assert self.sensor._sql_endpoint_name == "sql_warehouse_default"
        assert self.sensor.poke_interval == 15

    @pytest.mark.parametrize(
        argnames=("sensor_poke_result", "expected_poke_result"), argvalues=[(True, True), (False, False)]
    )
    @patch.object(DatabricksSqlSensor, "_get_results")
    def test_poke(self, mock_get_results, sensor_poke_result, expected_poke_result):
        mock_get_results.return_value = sensor_poke_result
        assert self.sensor.poke({}) == expected_poke_result

    @pytest.mark.parametrize(argnames=("query", "query_return"), argvalues=[(DEFAULT_SQL, True), ("", False)])
    @patch.object(DatabricksSqlSensor, "_sql_sensor")
    def test_query(self, mock_sql_sensor, query, query_return):
        mock_sql_sensor.return_value = query
        assert self.sensor.poke({}) == query_return

    def test_unsupported_conn_type(self):
        with pytest.raises(AirflowException):
            self.sensor.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
