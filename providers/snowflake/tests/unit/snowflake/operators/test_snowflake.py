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
from unittest.mock import MagicMock, call

import pendulum
import pytest
import requests

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeCheckOperator,
    SnowflakeIntervalCheckOperator,
    SnowflakeSqlApiOperator,
    SnowflakeValueCheckOperator,
)
from airflow.providers.snowflake.triggers.snowflake_trigger import SnowflakeSqlApiTrigger
from airflow.utils.types import DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
from tests_common.test_utils.taskinstance import create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_3_PLUS, timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"

TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
TEST_SQL = "select * from any;"

SQL_MULTIPLE_STMTS = (
    "create or replace table user_test (i int); insert into user_test (i) "
    "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
)

SINGLE_STMT = "select i from user_test order by i;"


@pytest.mark.db_test
class TestSnowflakeOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_snowflake_operator(self, mock_get_db_hook, dag_maker):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """

        with dag_maker(TEST_DAG_ID):
            operator = SQLExecuteQueryOperator(
                task_id="basic_snowflake", sql=sql, do_xcom_push=False, conn_id="snowflake_default"
            )
        # do_xcom_push=False because otherwise the XCom test will fail due to the mocking (it actually works)
        dag_maker.run_ti(operator.task_id)


class TestSnowflakeOperatorForParams:
    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.__init__")
    def test_overwrite_params(self, mock_base_op):
        sql = "Select * from test_table"
        SQLExecuteQueryOperator(
            sql=sql,
            task_id="snowflake_params_check",
            conn_id="snowflake_default",
            hook_params={
                "warehouse": "test_warehouse",
                "database": "test_database",
                "role": "test_role",
                "schema": "test_schema",
                "authenticator": "oath",
                "session_parameters": {"QUERY_TAG": "test_tag"},
            },
        )
        mock_base_op.assert_called_once_with(
            conn_id="snowflake_default",
            task_id="snowflake_params_check",
            database=None,
            hook_params={
                "warehouse": "test_warehouse",
                "database": "test_database",
                "role": "test_role",
                "schema": "test_schema",
                "authenticator": "oath",
                "session_parameters": {"QUERY_TAG": "test_tag"},
            },
            default_args={},
        )


@pytest.fixture(autouse=True)
def setup_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id="snowflake_default",
            conn_type="snowflake",
            host="test_host",
            port=443,
            schema="test_schema",
            login="test_user",
            password="test_password",
        )
    )


class TestSnowflakeCheckOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLCheckOperator.get_db_hook")
    def test_get_db_hook(
        self,
        mock_get_db_hook,
    ):
        operator = SnowflakeCheckOperator(
            task_id="snowflake_check",
            snowflake_conn_id="snowflake_default",
            sql="Select * from test_table",
            parameters={"param1": "value1"},
        )
        operator.execute({})
        mock_get_db_hook.assert_has_calls(
            [call().get_first("Select * from test_table", {"param1": "value1"})]
        )


class TestSnowflakeValueCheckOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLValueCheckOperator.get_db_hook")
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLValueCheckOperator.check_value")
    def test_get_db_hook(
        self,
        mock_check_value,
        mock_get_db_hook,
    ):
        mock_get_db_hook.return_value.get_first.return_value = ["test_value"]

        operator = SnowflakeValueCheckOperator(
            task_id="snowflake_check",
            sql="Select * from test_table",
            pass_value=95,
            parameters={"param1": "value1"},
        )
        operator.execute({})
        mock_get_db_hook.assert_has_calls(
            [call().get_first("Select * from test_table", {"param1": "value1"})]
        )
        assert mock_check_value.call_args == call(["test_value"])


class TestSnowflakeIntervalCheckOperator:
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLIntervalCheckOperator.__init__")
    def test_get_db_hook(
        self,
        mock_snowflake_interval_check_operator,
    ):
        SnowflakeIntervalCheckOperator(
            task_id="snowflake_check", table="test-table-id", metrics_thresholds={"COUNT(*)": 1.5}
        )
        assert mock_snowflake_interval_check_operator.call_args == mock.call(
            table="test-table-id",
            metrics_thresholds={"COUNT(*)": 1.5},
            date_filter_column="ds",
            days_back=-7,
            conn_id="snowflake_default",
            task_id="snowflake_check",
            default_args={},
        )


@pytest.mark.parametrize(
    ("operator_class", "kwargs"),
    [
        (SnowflakeCheckOperator, dict(sql="Select * from test_table")),
        (SnowflakeValueCheckOperator, dict(sql="Select * from test_table", pass_value=95)),
        (SnowflakeIntervalCheckOperator, dict(table="test-table-id", metrics_thresholds={"COUNT(*)": 1.5})),
    ],
)
class TestSnowflakeCheckOperatorsForParams:
    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.__init__")
    def test_overwrite_params(
        self,
        mock_base_op,
        operator_class,
        kwargs,
    ):
        operator_class(
            task_id="snowflake_params_check",
            snowflake_conn_id="snowflake_default",
            warehouse="test_warehouse",
            database="test_database",
            role="test_role",
            schema="test_schema",
            authenticator="oath",
            session_parameters={"QUERY_TAG": "test_tag"},
            **kwargs,
        )
        mock_base_op.assert_called_once_with(
            conn_id="snowflake_default",
            database=None,
            task_id="snowflake_params_check",
            hook_params={
                "warehouse": "test_warehouse",
                "database": "test_database",
                "role": "test_role",
                "schema": "test_schema",
                "authenticator": "oath",
                "session_parameters": {"QUERY_TAG": "test_tag"},
            },
            default_args={},
        )


def create_context(task, dag=None):
    if dag is None:
        dag = DAG(dag_id="dag", schedule=None)
    tzinfo = pendulum.timezone("UTC")
    logical_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    if AIRFLOW_V_3_0_PLUS:
        from airflow.models.dag_version import DagVersion

        sync_dag_to_db(dag)
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        task_instance = create_task_instance(task=task, run_id="test_run_id", dag_version_id=dag_version.id)
        dag_run = DagRun(
            dag_id=dag.dag_id,
            logical_date=logical_date,
            run_id=DagRun.generate_run_id(
                run_type=DagRunType.MANUAL, logical_date=logical_date, run_after=logical_date
            ),
        )
    else:
        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=logical_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
        )

        task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock()
    date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
    return {
        "dag": dag,
        "ts": logical_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": dag_run.run_id,
        "dag_run": dag_run,
        "data_interval_end": logical_date,
        date_key: logical_date,
    }


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiHook.execute_query"
    ) as execute_query:
        yield execute_query


@pytest.fixture
def mock_get_sql_api_query_status():
    with mock.patch(
        "airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiHook.get_sql_api_query_status"
    ) as get_sql_api_query_status:
        yield get_sql_api_query_status


@pytest.fixture
def mock_check_query_output():
    with mock.patch(
        "airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiHook.check_query_output"
    ) as check_query_output:
        yield check_query_output


@pytest.mark.db_test
class TestSnowflakeSqlApiOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self):
        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

        yield

        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

    def test_snowflake_sql_api_to_succeed_when_no_query_fails(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        """Tests SnowflakeSqlApiOperator passed if poll_on_queries method gives no error"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            durable=False,
        )
        mock_execute_query.return_value = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}, {"status": "success"}]
        operator.execute(context=None)

    def test_snowflake_sql_api_durable_true_submits_fresh_missing_task_state_store(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        """Default durable=True with no task_state_store in context still submits and succeeds."""
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
        )
        mock_execute_query.return_value = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}, {"status": "success"}]

        operator.execute(context={})

        mock_execute_query.assert_called_once()
        mock_check_query_output.assert_called_once_with(["uuid1", "uuid2"])

    def test_snowflake_sql_api_to_fails_when_one_query_fails(
        self, mock_execute_query, mock_get_sql_api_query_status
    ):
        """Tests SnowflakeSqlApiOperator passed if poll_on_queries method gives one or more error"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            durable=False,
        )
        mock_execute_query.return_value = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = [{"status": "error"}, {"status": "success"}]
        with pytest.raises(RuntimeError):
            operator.execute(context=None)

    def test_poll_on_queries_raises_runtime_error_on_status_check_failure(
        self, mock_execute_query, mock_get_sql_api_query_status
    ):
        """Tests poll_on_queries raises RuntimeError when status check fails."""
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
        )
        operator.query_ids = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = RuntimeError("connection timeout")

        with pytest.raises(RuntimeError, match="Failed to get status for query uuid1"):
            operator.poll_on_queries()

    def test_poll_on_queries_no_sleep_when_all_resolved(
        self, mock_execute_query, mock_get_sql_api_query_status
    ):
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
        )
        operator.query_ids = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}, {"status": "error"}]

        with mock.patch("time.sleep") as mock_sleep:
            result = operator.poll_on_queries()

        mock_sleep.assert_not_called()
        assert result["success"] == {"uuid1": {"status": "success"}}
        assert result["error"] == {"uuid2": {"status": "error"}}
        assert result["running"] == {}

    def test_poll_on_queries_sleeps_once_per_cycle(self, mock_execute_query, mock_get_sql_api_query_status):
        """One handle is still running, so the cycle sleeps -- but only once, not per handle."""
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
        )
        operator.query_ids = ["uuid1", "uuid2", "uuid3"]
        mock_get_sql_api_query_status.side_effect = [
            {"status": "success"},
            {"status": "running"},
            {"status": "success"},
        ]

        with mock.patch("time.sleep") as mock_sleep:
            result = operator.poll_on_queries()

        mock_sleep.assert_called_once_with(operator.poll_interval)
        assert result["running"] == {"uuid2": {"status": "running"}}

    @pytest.mark.parametrize(
        ("mock_sql", "statement_count"),
        [pytest.param(SQL_MULTIPLE_STMTS, 4, id="multi"), pytest.param(SINGLE_STMT, 1, id="single")],
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.execute_query")
    def test_snowflake_sql_api_execute_operator_async(
        self, mock_execute_query, mock_sql, statement_count, mock_get_sql_api_query_status
    ):
        """
        Asserts that a task is deferred and an SnowflakeSqlApiTrigger will be fired
        when the SnowflakeSqlApiOperator is executed.
        """
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=mock_sql,
            statement_count=statement_count,
            deferrable=True,
        )

        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "running"}]

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(exc.value.trigger, SnowflakeSqlApiTrigger), (
            "Trigger is not a SnowflakeSqlApiTrigger"
        )

    def test_snowflake_sql_api_pushes_query_ids_to_xcom(
        self,
        mock_execute_query,
        mock_get_sql_api_query_status,
        mock_check_query_output,
    ):
        """
        Tests that query IDs returned by the Snowflake SQL API are pushed to XCom
        when ``do_xcom_push=True``.
        """
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=True,
            deferrable=False,
            durable=False,
        )

        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.return_value = {"status": "success"}

        context = create_context(operator)

        operator.execute(context)

        context["ti"].xcom_push.assert_called_once_with(
            key="query_ids",
            value=["uuid1"],
        )

    def test_snowflake_sql_api_execute_complete_failure(self):
        """Test SnowflakeSqlApiOperator raise RuntimeError of error event"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            deferrable=True,
        )
        with pytest.raises(RuntimeError):
            operator.execute_complete(
                context=None,
                event={"status": "error", "message": "Test failure message", "type": "FAILED_WITH_ERROR"},
            )

    @pytest.mark.parametrize(
        "mock_event",
        [
            None,
            ({"status": "success", "statement_query_ids": ["uuid", "uuid"]}),
        ],
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.check_query_output")
    def test_snowflake_sql_api_execute_complete(self, mock_conn, mock_event):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            deferrable=True,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)

    @pytest.mark.parametrize(
        "mock_event",
        [
            None,
            ({"status": "success", "statement_query_ids": ["uuid", "uuid"]}),
        ],
    )
    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.check_query_output")
    def test_snowflake_sql_api_execute_complete_reassigns_query_ids(self, mock_conn, mock_event):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            deferrable=True,
        )
        expected_query_ids = mock_event["statement_query_ids"] if mock_event else []

        assert operator.query_ids == []
        assert operator._hook.query_ids == []

        operator.execute_complete(context=None, event=mock_event)

        assert operator.query_ids == expected_query_ids
        assert operator._hook.query_ids == expected_query_ids

    def test_snowflake_sql_api_caches_hook(self):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            deferrable=True,
        )
        hook1 = operator._hook
        hook2 = operator._hook
        assert hook1 is hook2

    @mock.patch("airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperator.defer")
    def test_snowflake_sql_api_execute_operator_failed_before_defer(
        self, mock_defer, mock_execute_query, mock_get_sql_api_query_status
    ):
        """Asserts that a task is not deferred when its failed"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            deferrable=True,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "error"}]
        with pytest.raises(RuntimeError):
            operator.execute(create_context(operator))
        assert not mock_defer.called

    @mock.patch("airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperator.defer")
    def test_snowflake_sql_api_execute_operator_succeeded_before_defer(
        self, mock_defer, mock_execute_query, mock_get_sql_api_query_status
    ):
        """Asserts that a task is not deferred when its succeeded"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            deferrable=True,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}]
        operator.execute(create_context(operator))

        assert not mock_defer.called

    @mock.patch("airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiOperator.defer")
    def test_snowflake_sql_api_execute_operator_running_before_defer(
        self, mock_defer, mock_execute_query, mock_get_sql_api_query_status
    ):
        """Asserts that a task is deferred when its running"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            deferrable=True,
        )
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.side_effect = [{"status": "running"}]
        operator.execute(create_context(operator))

        assert mock_defer.called

    def test_snowflake_sql_api_execute_operator_polling_running(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        """
        Tests that the execute method correctly loops and waits until all queries complete
        when ``deferrable=False``
        """
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            deferrable=False,
            durable=False,
        )

        mock_execute_query.return_value = ["uuid1"]

        mock_get_sql_api_query_status.side_effect = [
            # 1st poll_on_queries check (poll_interval: 5s) -- still running, sleeps
            {"status": "running"},
            # 2nd poll_on_queries check (poll_interval: 5s) -- still running, sleeps
            {"status": "running"},
            # 3rd poll_on_queries check (poll_interval: 5s) -- still running, sleeps
            {"status": "running"},
            # 4th poll_on_queries check -- resolves to success, no sleep needed
            {"status": "success"},
        ]

        with mock.patch("time.sleep") as mock_sleep:
            operator.execute(context=None)
            mock_check_query_output.assert_called_once_with(["uuid1"])
            # 3 sleeps: durable=False routes straight into poll_until_complete with no
            # separate pre-check, so every "running" cycle (including the first) sleeps;
            # only the cycle that resolves to success skips it.
            assert mock_sleep.call_count == 3

    def test_snowflake_sql_api_execute_operator_polling_failed(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        """
        Tests that the execute method raises RuntimeError if any query fails during polling
        when ``deferrable=False``
        """
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            deferrable=False,
            durable=False,
        )

        mock_execute_query.return_value = ["uuid1"]

        mock_get_sql_api_query_status.side_effect = [
            # 1st poll_on_queries check -- still running, sleeps
            {"status": "running"},
            # 2nd poll_on_queries check -- resolves to error, raises immediately
            {"status": "error"},
        ]

        with mock.patch("time.sleep") as mock_sleep:
            with pytest.raises(RuntimeError):
                operator.execute(context=None)
            # 1 sleep: the first cycle finds "running" and sleeps before the second
            # cycle finds "error" and raises without sleeping again.
            assert mock_sleep.call_count == 1
        mock_check_query_output.assert_not_called()

    def test_poll_until_complete_pushes_query_ids_to_xcom_even_on_failure(
        self, mock_get_sql_api_query_status
    ):
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=True,
        )
        mock_get_sql_api_query_status.side_effect = [{"status": "error"}]
        context = create_context(operator)

        with pytest.raises(RuntimeError):
            operator.poll_until_complete(["uuid1"], context)

        context["ti"].xcom_push.assert_called_once_with(key="query_ids", value=["uuid1"])

    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.cancel_queries")
    def test_snowflake_sql_api_on_kill_cancels_queries(self, mock_cancel_queries):
        """Test that on_kill cancels running queries."""
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
        )
        operator.query_ids = ["uuid1", "uuid2"]

        operator.on_kill()

        mock_cancel_queries.assert_called_once_with(["uuid1", "uuid2"])

    @mock.patch("airflow.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHook.cancel_queries")
    def test_snowflake_sql_api_on_kill_no_queries(self, mock_cancel_queries):
        """Test that on_kill does nothing when no query ids exist."""
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
        )
        operator.query_ids = []

        operator.on_kill()

        mock_cancel_queries.assert_not_called()


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="task_state_store (durable execution) requires Airflow 3.3+"
)
class TestSnowflakeSqlApiOperatorDurable:
    @staticmethod
    def _context(task_store=None):
        ctx: dict = {"ti": MagicMock(stats_tags={})}
        if task_store is not None:
            ctx["task_state_store"] = task_store
        return ctx

    @staticmethod
    def _make_operator(**kwargs):
        return SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            **kwargs,
        )

    def test_persists_query_ids_to_task_state_store_on_fresh_submit(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator()
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.return_value = {"status": "success"}
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = None

        operator.execute(self._context(task_store))

        mock_execute_query.assert_called_once()
        task_store.set.assert_called_once_with("snowflake_query_ids", ["uuid1"])

    def test_reconnects_to_running_query_without_resubmitting(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator()
        # get_job_status sees it still running, then poll_on_queries sees it finish.
        mock_get_sql_api_query_status.side_effect = [{"status": "running"}, {"status": "success"}]
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = ["uuid1"]

        operator.execute(self._context(task_store))

        mock_execute_query.assert_not_called()
        task_store.set.assert_not_called()
        # execute_query never ran on this hook instance, so nothing else would populate this --
        # OpenLineage's get_openlineage_database_specific_lineage reads hook.query_ids directly.
        assert operator._hook.query_ids == ["uuid1"]

    def test_partial_progress_reconnect_waits_only_on_running_handle(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator()
        mock_get_sql_api_query_status.side_effect = [
            {"status": "success"},  # get_job_status check: uuid1 already done
            {"status": "running"},  # get_job_status check: uuid2 -> aggregate "running" -> reconnect
            {"status": "success"},  # poll cycle 1: uuid1 re-confirmed
            {"status": "running"},  # poll cycle 1: uuid2 still running -> sleep
            {"status": "success"},  # poll cycle 2: uuid1 re-confirmed
            {"status": "success"},  # poll cycle 2: uuid2 finishes
        ]
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = ["uuid1", "uuid2"]

        with mock.patch("time.sleep"):
            operator.execute(self._context(task_store))

        mock_execute_query.assert_not_called()
        task_store.set.assert_not_called()
        mock_check_query_output.assert_called_once_with(["uuid1", "uuid2"])
        assert operator._hook.query_ids == ["uuid1", "uuid2"]

    def test_already_succeeded_returns_result_without_polling(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator()
        mock_get_sql_api_query_status.return_value = {"status": "success"}
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = ["uuid1"]

        operator.execute(self._context(task_store))

        mock_execute_query.assert_not_called()
        mock_check_query_output.assert_called_once_with(["uuid1"])
        # Only the get_job_status check ran; poll_on_queries must never have been entered.
        assert mock_get_sql_api_query_status.call_count == 1
        assert operator._hook.query_ids == ["uuid1"]

    def test_already_succeeded_pushes_query_ids_to_xcom(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id="snowflake_default",
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=True,
        )
        mock_get_sql_api_query_status.return_value = {"status": "success"}
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = ["uuid1"]
        context = self._context(task_store)

        operator.execute(context)

        mock_execute_query.assert_not_called()
        context["ti"].xcom_push.assert_called_once_with(key="query_ids", value=["uuid1"])

    def test_resubmits_when_stored_query_in_terminal_error(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator()
        mock_execute_query.return_value = ["uuid2"]
        # get_job_status sees the stored handle failed; after fresh resubmit, polling succeeds.
        mock_get_sql_api_query_status.side_effect = [{"status": "error"}, {"status": "success"}]
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = ["uuid1"]

        operator.execute(self._context(task_store))

        mock_execute_query.assert_called_once()
        task_store.set.assert_called_once_with("snowflake_query_ids", ["uuid2"])

    def test_resubmits_when_stored_query_not_found(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator()
        mock_execute_query.return_value = ["uuid2"]
        not_found_response = MagicMock()
        not_found_response.status_code = 404
        # get_job_status sees the stored handle expired (404); after fresh resubmit, polling succeeds.
        mock_get_sql_api_query_status.side_effect = [
            requests.exceptions.HTTPError(response=not_found_response),
            {"status": "success"},
        ]
        task_store = MagicMock(spec_set=["get", "set"])
        task_store.get.return_value = ["uuid1"]

        operator.execute(self._context(task_store))

        mock_execute_query.assert_called_once()
        task_store.set.assert_called_once_with("snowflake_query_ids", ["uuid2"])

    def test_durable_false_never_touches_task_state_store(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        operator = self._make_operator(durable=False)
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.return_value = {"status": "success"}
        task_store = MagicMock(spec_set=["get", "set"])

        operator.execute(self._context(task_store))

        mock_execute_query.assert_called_once()
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    def test_deferrable_unaffected_by_durable(self, mock_execute_query, mock_get_sql_api_query_status):
        operator = self._make_operator(deferrable=True)
        mock_execute_query.return_value = ["uuid1"]
        mock_get_sql_api_query_status.return_value = {"status": "running"}
        task_store = MagicMock(spec_set=["get", "set"])

        with mock.patch.object(SnowflakeSqlApiOperator, "defer") as mock_defer:
            operator.execute(self._context(task_store))

        assert mock_defer.called
        task_store.get.assert_not_called()
        task_store.set.assert_not_called()

    def test_get_job_status_error_takes_priority_over_running(self, mock_get_sql_api_query_status):
        operator = self._make_operator()
        mock_get_sql_api_query_status.side_effect = [{"status": "running"}, {"status": "error"}]

        assert operator.get_job_status(["uuid1", "uuid2"], context={}) == "error"

    def test_get_job_status_running_when_none_error(self, mock_get_sql_api_query_status):
        operator = self._make_operator()
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}, {"status": "running"}]

        assert operator.get_job_status(["uuid1", "uuid2"], context={}) == "running"

    def test_get_job_status_success_when_all_succeed(self, mock_get_sql_api_query_status):
        operator = self._make_operator()
        mock_get_sql_api_query_status.return_value = {"status": "success"}

        assert operator.get_job_status(["uuid1", "uuid2"], context={}) == "success"

    def test_get_job_status_not_found_on_404(self, mock_get_sql_api_query_status):
        operator = self._make_operator()
        response = MagicMock()
        response.status_code = 404
        mock_get_sql_api_query_status.side_effect = requests.exceptions.HTTPError(response=response)

        assert operator.get_job_status(["uuid1"], context={}) == "not_found"

    def test_get_job_status_reraises_non_404_http_error(self, mock_get_sql_api_query_status):
        operator = self._make_operator()
        response = MagicMock()
        response.status_code = 500
        mock_get_sql_api_query_status.side_effect = requests.exceptions.HTTPError(response=response)

        with pytest.raises(requests.exceptions.HTTPError):
            operator.get_job_status(["uuid1"], context={})

    def test_is_job_active_and_is_job_succeeded_predicates(self):
        operator = self._make_operator()

        assert operator.is_job_active("running") is True
        assert operator.is_job_active("success") is False
        assert operator.is_job_succeeded("success") is True
        assert operator.is_job_succeeded("running") is False
