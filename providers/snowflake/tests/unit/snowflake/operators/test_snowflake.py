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
from unittest.mock import call

import pendulum
import pytest

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
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
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, timezone

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
        task_instance = TaskInstance(task=task, run_id="test_run_id", dag_version_id=dag_version.id)
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

    @pytest.fixture
    def mock_execute_query(self):
        with mock.patch(
            "airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiHook.execute_query"
        ) as execute_query:
            yield execute_query

    @pytest.fixture
    def mock_get_sql_api_query_status(self):
        with mock.patch(
            "airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiHook.get_sql_api_query_status"
        ) as get_sql_api_query_status:
            yield get_sql_api_query_status

    @pytest.fixture
    def mock_check_query_output(self):
        with mock.patch(
            "airflow.providers.snowflake.operators.snowflake.SnowflakeSqlApiHook.check_query_output"
        ) as check_query_output:
            yield check_query_output

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
        )
        mock_execute_query.return_value = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = [{"status": "success"}, {"status": "success"}]
        operator.execute(context=None)

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
        )
        mock_execute_query.return_value = ["uuid1", "uuid2"]
        mock_get_sql_api_query_status.side_effect = [{"status": "error"}, {"status": "success"}]
        with pytest.raises(AirflowException):
            operator.execute(context=None)

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

    def test_snowflake_sql_api_execute_complete_failure(self):
        """Test SnowflakeSqlApiOperator raise AirflowException of error event"""

        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
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
        with pytest.raises(AirflowException):
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
        )

        mock_execute_query.return_value = ["uuid1"]

        mock_get_sql_api_query_status.side_effect = [
            # Initial get_sql_api_query_status check
            {"status": "running"},
            # 1st poll_on_queries check (poll_interval: 5s)
            {"status": "running"},
            # 2nd poll_on_queries check (poll_interval: 5s)
            {"status": "running"},
            # 3rd poll_on_queries check (poll_interval: 5s)
            {"status": "success"},
        ]

        with mock.patch("time.sleep") as mock_sleep:
            operator.execute(context=None)
            mock_check_query_output.assert_called_once_with(["uuid1"])
            assert mock_sleep.call_count == 3

    def test_snowflake_sql_api_execute_operator_polling_failed(
        self, mock_execute_query, mock_get_sql_api_query_status, mock_check_query_output
    ):
        """
        Tests that the execute method raises AirflowException if any query fails during polling
        when ``deferrable=False``
        """
        operator = SnowflakeSqlApiOperator(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
            do_xcom_push=False,
            deferrable=False,
        )

        mock_execute_query.return_value = ["uuid1"]

        mock_get_sql_api_query_status.side_effect = [
            # Initial get_sql_api_query_status check
            {"status": "running"},
            # 1st poll_on_queries check
            {"status": "error"},
        ]

        with pytest.raises(AirflowException):
            operator.execute(context=None)
        mock_check_query_output.assert_not_called()

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
