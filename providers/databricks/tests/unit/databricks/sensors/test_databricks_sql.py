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
from unittest import mock
from unittest.mock import patch

import pytest

from airflow.models import DAG
from airflow.providers.common.compat.sdk import AirflowException, timezone
from airflow.providers.databricks.sensors.databricks_sql import DatabricksSqlSensor

TASK_ID = "db-sensor"
DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
HOST_WITH_SCHEME = "https://xx.cloud.databricks.com"
PERSONAL_ACCESS_TOKEN = "token"

DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"
DEFAULT_HTTP_PATH = "/sql/1.0/warehouses/xxxxx"
DEFAULT_SQL_WAREHOUSE = "sql_warehouse_default"
DEFAULT_CALLER = "TestDatabricksSqlSensor"
DEFAULT_SQL = f"select 1 from {DEFAULT_CATALOG}.{DEFAULT_SCHEMA}.{DEFAULT_TABLE} LIMIT 1"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)


class TestDatabricksSqlSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.sensor = DatabricksSqlSensor(
            task_id=TASK_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            sql_warehouse_name=DEFAULT_SQL_WAREHOUSE,
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
        assert self.sensor._sql_warehouse_name == "sql_warehouse_default"
        assert self.sensor.poke_interval == 15

    @pytest.mark.parametrize(
        argnames=("sensor_poke_result", "expected_poke_result"), argvalues=[(True, True), (False, False)]
    )
    @patch.object(DatabricksSqlSensor, "poke")
    def test_poke(self, mock_poke, sensor_poke_result, expected_poke_result):
        mock_poke.return_value = sensor_poke_result
        assert self.sensor.poke({}) == expected_poke_result

    def test_sql_warehouse_http_path(self):
        """Neither SQL warehouse name not HTTP path has been specified."""
        _sensor_without_sql_warehouse_http = DatabricksSqlSensor(
            task_id="task2",
            databricks_conn_id=DEFAULT_CONN_ID,
            dag=self.dag,
            sql=DEFAULT_SQL,
            schema=DEFAULT_SCHEMA,
            catalog=DEFAULT_CATALOG,
            timeout=30,
            poke_interval=15,
        )
        with pytest.raises(AirflowException):
            _sensor_without_sql_warehouse_http._get_results()

    def test_fail__get_results(self):
        self.sensor._http_path = None
        self.sensor._sql_warehouse_name = None
        with pytest.raises(
            AirflowException,
            match="Databricks SQL warehouse/cluster configuration missing."
            " Please specify either http_path or sql_warehouse_name.",
        ):
            self.sensor._get_results()

    def _make_sensor(self, **kwargs):
        return DatabricksSqlSensor(
            task_id=TASK_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            sql_warehouse_name=DEFAULT_SQL_WAREHOUSE,
            sql=DEFAULT_SQL,
            **kwargs,
        )

    def test_query_tags_defaults(self):
        sensor = self._make_sensor()
        assert sensor.query_tags == {}
        assert sensor.include_airflow_query_tags is True

    def test_query_tags_in_template_fields(self):
        assert "query_tags" in DatabricksSqlSensor.template_fields

    def test_query_tags_stored(self):
        sensor = self._make_sensor(query_tags={"env": "prod"})
        assert sensor.query_tags == {"env": "prod"}

    def test_poke_sets_query_tags_on_hook(self):
        sensor = self._make_sensor(query_tags={"env": "test"}, include_airflow_query_tags=False)
        with patch.object(sensor, "_get_results", return_value=True) as mock_results:
            sensor.poke(context=None)
        assert sensor.hook.query_tags == {"env": "test"}
        mock_results.assert_called_once()

    def test_poke_no_tags_when_disabled_and_no_custom(self):
        sensor = self._make_sensor(include_airflow_query_tags=False)
        with patch.object(sensor, "_get_results", return_value=True):
            sensor.poke(context=None)
        assert sensor.hook.query_tags is None

    def test_poke_includes_airflow_tags_from_context(self):
        sensor = self._make_sensor(query_tags={"custom": "value"})
        mock_ti = mock.MagicMock(spec=["dag_id", "task_id", "run_id", "try_number", "map_index"])
        mock_ti.dag_id = "my_dag"
        mock_ti.task_id = "my_task"
        mock_ti.run_id = "run_1"
        mock_ti.try_number = 1
        mock_ti.map_index = -1
        mock_context = {"ti": mock_ti}

        with patch.object(sensor, "_get_results", return_value=True):
            sensor.poke(context=mock_context)

        tags = sensor.hook.query_tags
        assert tags is not None
        assert tags["airflow_dag_id"] == "my_dag"
        assert tags["airflow_task_id"] == "my_task"
        assert tags["custom"] == "value"

    def test_custom_tags_override_airflow_tags(self):
        sensor = self._make_sensor(query_tags={"airflow_dag_id": "overridden"})
        mock_ti = mock.MagicMock(spec=["dag_id", "task_id", "run_id", "try_number", "map_index"])
        mock_ti.dag_id = "original"
        mock_ti.task_id = "t"
        mock_ti.run_id = "r"
        mock_ti.try_number = 1
        mock_ti.map_index = -1
        mock_context = {"ti": mock_ti}

        with patch.object(sensor, "_get_results", return_value=True):
            sensor.poke(context=mock_context)

        assert sensor.hook.query_tags["airflow_dag_id"] == "overridden"
