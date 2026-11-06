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

import copy
from datetime import date, datetime, timedelta
from decimal import Decimal
from operator import itemgetter
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowSensorTimeout,
    AirflowSkipException,
    TaskDeferred,
    timezone,
)
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.sensors.sql import SqlSensor, SqlSensorTriggerError
from airflow.providers.common.sql.triggers.sql import SqlSensorTrigger

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_2_PLUS

if AIRFLOW_V_3_2_2_PLUS:
    from airflow.sdk.bases.operator import TriggerFailureReason

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
TEST_DAG_ID = "unit_test_sql_dag"


class TestSqlSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, schedule=None, default_args=args)

    @pytest.mark.db_test
    def test_unsupported_conn_type(self):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="redis_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag,
        )

        with pytest.raises(AirflowException):
            op.execute({})

    @pytest.mark.backend("mysql")
    def test_sql_sensor_mysql(self):
        op1 = SqlSensor(
            task_id="sql_sensor_check_1",
            conn_id="mysql_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag,
        )
        op1.execute({})

        op2 = SqlSensor(
            task_id="sql_sensor_check_2",
            conn_id="mysql_default",
            sql="SELECT count(%s) FROM INFORMATION_SCHEMA.TABLES",
            parameters=["table_name"],
            dag=self.dag,
        )
        op2.execute({})

    @pytest.mark.backend("postgres")
    def test_sql_sensor_postgres(self):
        op1 = SqlSensor(
            task_id="sql_sensor_check_1",
            conn_id="postgres_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            dag=self.dag,
        )
        op1.execute({})

        op2 = SqlSensor(
            task_id="sql_sensor_check_2",
            conn_id="postgres_default",
            sql="SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s",
            parameters=["information_schema"],
            dag=self.dag,
        )
        op2.execute({})

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
        assert not op.poke({})

        mock_get_records.return_value = [[None]]
        assert not op.poke({})

        mock_get_records.return_value = [["None"]]
        assert op.poke({})

        mock_get_records.return_value = [[0.0]]
        assert not op.poke({})

        mock_get_records.return_value = [[0]]
        assert not op.poke({})

        mock_get_records.return_value = [["0"]]
        assert op.poke({})

        mock_get_records.return_value = [["1"]]
        assert op.poke({})

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_fail_on_empty(self, mock_hook):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            fail_on_empty=True,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        with pytest.raises(AirflowException):
            op.poke({})

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_success(self, mock_hook):
        op = SqlSensor(
            task_id="sql_sensor_check", conn_id="postgres_default", sql="SELECT 1", success=lambda x: x in [1]
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke({})

        mock_get_records.return_value = [[1]]
        assert op.poke({})

        mock_get_records.return_value = [["1"]]
        assert not op.poke({})

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_failure(
        self,
        mock_hook,
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=lambda x: x in [1],
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke({})

        mock_get_records.return_value = [[1]]
        with pytest.raises(AirflowException):
            op.poke({})

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_failure_success(
        self,
        mock_hook,
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=lambda x: x in [1],
            success=lambda x: x in [2],
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke({})

        mock_get_records.return_value = [[1]]
        with pytest.raises(AirflowException):
            op.poke({})

        mock_get_records.return_value = [[2]]
        assert op.poke({})

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_failure_success_same(self, mock_hook):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=lambda x: x in [1],
            success=lambda x: x in [1],
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []
        assert not op.poke({})

        mock_get_records.return_value = [[1]]
        with pytest.raises(AirflowException):
            op.poke({})

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_invalid_failure(self, mock_hook):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            failure=[1],
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[1]]
        with pytest.raises(AirflowException) as ctx:
            op.poke({})
        assert str(ctx.value) == "self.failure is present, but not callable -> [1]"

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_postgres_poke_invalid_success(
        self,
        mock_hook,
    ):
        op = SqlSensor(
            task_id="sql_sensor_check",
            conn_id="postgres_default",
            sql="SELECT 1",
            success=[1],
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = [[1]]
        with pytest.raises(AirflowException) as ctx:
            op.poke({})
        assert str(ctx.value) == "self.success is present, but not callable -> [1]"

    @pytest.mark.backend("postgres")
    def test_sql_sensor_postgres_with_selector(self):
        op1 = SqlSensor(
            task_id="sql_sensor_check_1",
            conn_id="postgres_default",
            sql="SELECT 0, 1",
            dag=self.dag,
            success=lambda x: x in [1],
            failure=lambda x: x in [0],
            selector=lambda x: x[1],
        )
        op1.execute({})

        op2 = SqlSensor(
            task_id="sql_sensor_check_2",
            conn_id="postgres_default",
            sql="SELECT 0, 1",
            dag=self.dag,
            success=lambda x: x in [1],
            failure=lambda x: x in [0],
            selector=lambda x: x[0],
        )
        with pytest.raises(AirflowException):
            op2.poke({})

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

    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_sql_sensor_templated_parameters(self, mock_base_hook):
        op = SqlSensor(
            task_id="sql_sensor_templated_parameters",
            conn_id="snowflake_default",
            sql="SELECT %(something)s",
            parameters={"something": "{{ logical_date }}"},
        )
        op.render_template_fields(context={"logical_date": "1970-01-01"})
        mock_base_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)
        mock_get_records = mock_base_hook.get_connection.return_value.get_hook.return_value.get_records
        op.execute(context=mock.MagicMock())

        mock_get_records.assert_called_once_with("SELECT %(something)s", {"something": "1970-01-01"})


@pytest.fixture
def mock_get_records():
    with mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook") as mock_base_hook:
        hook = mock.MagicMock(spec=DbApiHook)
        mock_base_hook.get_connection.return_value.get_hook.return_value = hook
        yield hook.get_records


def make_sensor(deferrable: bool = True, sensor_class: type[SqlSensor] = SqlSensor, **kwargs) -> SqlSensor:
    defaults = {
        "task_id": "sql_sensor_deferrable",
        "conn_id": "postgres_default",
        "sql": "SELECT count(*) FROM orders WHERE ds = %(ds)s",
        "parameters": {"ds": "2026-01-01"},
        "hook_params": {"log_sql": False},
        "poke_interval": 0,
        "timeout": 600,
    }
    return sensor_class(**{**defaults, **kwargs}, deferrable=deferrable)


class PokeOverridingSensor(SqlSensor):
    def poke(self, context):
        return super().poke(context)


class HookOverridingSensor(SqlSensor):
    def _get_hook(self):
        return super()._get_hook()


class TestSqlSensorDeferrable:
    @pytest.fixture(autouse=True)
    def _airflow_version_that_can_defer(self):
        # The provider's compat jobs run these tests on Airflow 3 releases before 3.2.2, where the
        # sensor never defers; pin a version that can, and let the version tests override it.
        with (
            mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_0_PLUS", True),
            mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_2_2_PLUS", True),
        ):
            yield

    def test_returns_without_deferring_when_first_poke_succeeds(self, mock_get_records):
        mock_get_records.return_value = [(5,)]

        assert make_sensor().execute(context={}) is None
        assert mock_get_records.call_count == 1

    def test_defers_to_trigger_when_first_poke_fails(self, mock_get_records):
        mock_get_records.side_effect = [[(0,)], [(1,)]]

        with pytest.raises(TaskDeferred) as deferred:
            make_sensor(fail_on_empty=True).execute(context={})

        assert isinstance(deferred.value.trigger, SqlSensorTrigger)
        assert deferred.value.trigger.serialize()[1] == {
            "sql": "SELECT count(*) FROM orders WHERE ds = %(ds)s",
            "conn_id": "postgres_default",
            "hook_params": {"log_sql": False},
            "parameters": {"ds": "2026-01-01"},
            "poke_interval": 0,
            "fail_on_empty": True,
        }
        assert deferred.value.method_name == "execute_complete"
        assert deferred.value.timeout == timedelta(seconds=600)

    def test_does_not_warn_when_not_deferrable(self, mock_get_records, caplog):
        mock_get_records.side_effect = [[(0,)], [(1,)]]

        assert make_sensor(deferrable=False).execute(context={}) is None
        assert mock_get_records.call_count == 2
        assert "Cannot defer" not in caplog.text

    @pytest.mark.parametrize(
        ("sensor_kwargs", "reason"),
        [
            pytest.param(
                {"success": lambda value: value > 10}, "success, failure and selector", id="success"
            ),
            pytest.param({"failure": lambda value: value < 0}, "success, failure and selector", id="failure"),
            pytest.param({"selector": itemgetter(1)}, "success, failure and selector", id="selector"),
            pytest.param({"silent_fail": True}, "silent_fail, never_fail", id="silent_fail"),
            pytest.param({"never_fail": True}, "silent_fail, never_fail", id="never_fail"),
            pytest.param({"exponential_backoff": True}, "silent_fail, never_fail", id="exponential_backoff"),
            pytest.param(
                {"sensor_class": PokeOverridingSensor},
                "PokeOverridingSensor overrides poke or _get_hook",
                id="poke",
            ),
            pytest.param(
                {"sensor_class": HookOverridingSensor},
                "HookOverridingSensor overrides poke or _get_hook",
                id="get_hook",
            ),
        ],
    )
    def test_pokes_on_the_worker_when_the_triggerer_cannot_check(
        self, mock_get_records, caplog, sensor_kwargs, reason
    ):
        mock_get_records.side_effect = [[(0, 0)], [(20, 20)]]

        assert make_sensor(**sensor_kwargs).execute(context={}) is None
        assert mock_get_records.call_count == 2
        assert f"Cannot defer because {reason}" in caplog.text

    @pytest.mark.parametrize(
        "build_sensor",
        [
            pytest.param(lambda: make_sensor(selector=itemgetter(0)), id="explicit-first-cell-selector"),
            pytest.param(lambda: copy.deepcopy(make_sensor()), id="deep-copied-sensor"),
        ],
    )
    def test_default_condition_defers_however_the_selector_was_built(self, mock_get_records, build_sensor):
        # The second result meets the condition, so a wrong fallback to poking returns instead of hanging.
        mock_get_records.side_effect = [[(0,)], [(1,)]]

        with pytest.raises(TaskDeferred):
            build_sensor().execute(context={})

    @mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_2_2_PLUS", False)
    @mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_0_PLUS", True)
    def test_pokes_on_the_worker_on_airflow_3_before_3_2_2(self, mock_get_records, caplog):
        mock_get_records.side_effect = [[(0,)], [(1,)]]

        assert make_sensor().execute(context={}) is None
        assert mock_get_records.call_count == 2
        assert "Cannot defer because the triggerer before Airflow 3.2.2" in caplog.text

    @mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_2_2_PLUS", False)
    @mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_0_PLUS", False)
    def test_defers_on_airflow_2(self, mock_get_records):
        mock_get_records.side_effect = [[(0,)], [(1,)]]

        with pytest.raises(TaskDeferred):
            make_sensor(parameters=("2026-01-01", 3, 1.5, True, None)).execute(context={})

    @mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_2_2_PLUS", False)
    @mock.patch("airflow.providers.common.sql.sensors.sql.AIRFLOW_V_3_0_PLUS", False)
    @pytest.mark.parametrize(
        "value",
        [date(2026, 1, 1), datetime(2026, 1, 1, 12), Decimal("1.5")],
        ids=["date", "datetime", "decimal"],
    )
    def test_pokes_on_the_worker_on_airflow_2_when_parameters_would_change_in_json(
        self, mock_get_records, caplog, value
    ):
        mock_get_records.side_effect = [[(0,)], [(1,)]]

        assert make_sensor(parameters={"ds": value}).execute(context={}) is None
        assert mock_get_records.call_count == 2
        assert "Cannot defer because Airflow 2 stores trigger arguments as JSON" in caplog.text

    def test_success_event_completes(self):
        assert make_sensor().execute_complete(context={}, event={"status": "success"}) is None

    def test_failure_event_raises(self):
        with pytest.raises(SqlSensorTriggerError, match="connection refused"):
            make_sensor().resume_execution(
                "execute_complete", {"event": {"status": "failure", "message": "connection refused"}}, {}
            )

    def test_failure_event_skips_with_soft_fail(self):
        with pytest.raises(AirflowSkipException):
            make_sensor(soft_fail=True).resume_execution(
                "execute_complete", {"event": {"status": "failure", "message": "connection refused"}}, {}
            )

    @pytest.mark.skipif(not AIRFLOW_V_3_2_2_PLUS, reason="SqlSensor only defers on Airflow 3 from 3.2.2")
    @pytest.mark.parametrize(
        ("soft_fail", "expected"), [(False, AirflowSensorTimeout), (True, AirflowSkipException)]
    )
    def test_trigger_timeout_fails_like_a_sensor_timeout(self, soft_fail, expected):
        with pytest.raises(expected) as excinfo:
            make_sensor(soft_fail=soft_fail).resume_execution(
                "__fail__", {"error": TriggerFailureReason.TRIGGER_TIMEOUT}, {}
            )

        assert excinfo.type is expected
