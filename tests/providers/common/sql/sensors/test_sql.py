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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.dag import DAG
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.timezone import datetime

DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_sql_dag"


class TestSqlSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=args)

    @pytest.mark.db_test
    def test_unsupported_conn_type(self):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="redis_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag,
        )

        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.backend("mysql")
    def test_sql_sensor_mysql(self):
        op1 = SqlSensor(
            task_id="sql_sensor_check_1",
            conn_id="mysql_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        op2 = SqlSensor(
            task_id="sql_sensor_check_2",
            conn_id="mysql_default",
            sql="SELECT count(%s) FROM INFORMATION_SCHEMA.TABLES",
            parameters=["table_name"],
            dag=self.dag,
        )
        op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.backend("postgres")
    def test_sql_sensor_postgres(self):
        op1 = SqlSensor(
            task_id="sql_sensor_check_1",
            conn_id="postgres_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag,
        )
        op1.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        op2 = SqlSensor(
            task_id="sql_sensor_check_2",
            conn_id="postgres_default",
            sql="SELECT count(%s) FROM INFORMATION_SCHEMA.TABLES",
            parameters=["table_name"],
            dag=self.dag,
        )
        op2.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke(self, mock_hook):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke(None)

        mock_get_records.return_value = [[None]]
        assert not op.poke(None)

        mock_get_records.return_value = [["None"]]
        assert op.poke(None)

        mock_get_records.return_value = [[0.0]]
        assert not op.poke(None)

        mock_get_records.return_value = [[0]]
        assert not op.poke(None)

        mock_get_records.return_value = [["0"]]
        assert op.poke(None)

        mock_get_records.return_value = [["1"]]
        assert op.poke(None)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_fail_on_empty(
        self, mock_hook, soft_fail: bool, expected_exception: type[AirflowException]
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            fail_on_empty=True,
            soft_fail=soft_fail,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        with pytest.raises(expected_exception):
            op.poke(None)

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_success(self, mock_hook):
        op = SqlSensor(
            task_id="sql_sensor_check", conn_id="postgres_default", sql="SELECT 1", success=lambda x: x in [1]
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke(None)

        mock_get_records.return_value = [[1]]
        assert op.poke(None)

        mock_get_records.return_value = [["1"]]
        assert not op.poke(None)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_failure(
        self, mock_hook, soft_fail: bool, expected_exception: type[AirflowException]
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=lambda x: x in [1],
            soft_fail=soft_fail,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke(None)

        mock_get_records.return_value = [[1]]
        with pytest.raises(expected_exception):
            op.poke(None)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_failure_success(
        self, mock_hook, soft_fail: bool, expected_exception: type[AirflowException]
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=lambda x: x in [1],
            success=lambda x: x in [2],
            soft_fail=soft_fail,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke(None)

        mock_get_records.return_value = [[1]]
        with pytest.raises(expected_exception):
            op.poke(None)

        mock_get_records.return_value = [[2]]
        assert op.poke(None)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_failure_success_same(
        self, mock_hook, soft_fail: bool, expected_exception: type[AirflowException]
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=lambda x: x in [1],
            success=lambda x: x in [1],
            soft_fail=soft_fail,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke(None)

        mock_get_records.return_value = [[1]]
        with pytest.raises(expected_exception):
            op.poke(None)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_invalid_failure(
        self, mock_hook, soft_fail: bool, expected_exception: type[AirflowException]
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=[1],
            soft_fail=soft_fail,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[1]]
        with pytest.raises(expected_exception) as ctx:
            op.poke(None)
        assert "self.failure is present, but not callable -> [1]" == str(ctx.value)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_invalid_success(
        self, mock_hook, soft_fail: bool, expected_exception: type[AirflowException]
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            success=[1],
            soft_fail=soft_fail,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[1]]
        with pytest.raises(expected_exception) as ctx:
            op.poke(None)
        assert "self.success is present, but not callable -> [1]" == str(ctx.value)

    @pytest.mark.db_test
    def test_sql_sensor_hook_params(self):
        op = SqlSensor(
            task_id="sql_sensor_hook_params",
            conn_id="postgres_default",
            sql="SELECT 1",
            hook_params={
                "log_sql": False,
            },
        )
        hook = op._get_hook()
        assert hook.log_sql == op.hook_params["log_sql"]
