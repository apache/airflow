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

import pendulum
import pytest

from airflow.exceptions import AirflowException, TaskDeferred
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
from airflow.utils import timezone
from airflow.utils.types import DagRunType

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
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, schedule=None, default_args=args)
        self.dag = dag

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_snowflake_operator(self, mock_get_db_hook):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        operator = SQLExecuteQueryOperator(
            task_id="basic_snowflake", sql=sql, dag=self.dag, do_xcom_push=False, conn_id="snowflake_default"
        )
        # do_xcom_push=False because otherwise the XCom test will fail due to the mocking (it actually works)
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


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


@pytest.mark.parametrize(
    "operator_class, kwargs",
    [
        (SnowflakeCheckOperator, dict(sql="Select * from test_table")),
        (SnowflakeValueCheckOperator, dict(sql="Select * from test_table", pass_value=95)),
        (SnowflakeIntervalCheckOperator, dict(table="test-table-id", metrics_thresholds={"COUNT(*)": 1.5})),
    ],
)
class TestSnowflakeCheckOperators:
    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_get_db_hook(
        self,
        mock_get_db_hook,
        operator_class,
        kwargs,
    ):
        operator = operator_class(task_id="snowflake_check", snowflake_conn_id="snowflake_default", **kwargs)
        operator.get_db_hook()
        mock_get_db_hook.assert_called_once()


@pytest.mark.parametrize(
    "operator_class, kwargs",
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
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )

    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "ts": execution_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": dag_run.run_id,
        "dag_run": dag_run,
        "execution_date": execution_date,
        "data_interval_end": execution_date,
        "logical_date": execution_date,
    }


class TestSnowflakeSqlApiOperator:
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

    @pytest.mark.parametrize("mock_sql, statement_count", [(SQL_MULTIPLE_STMTS, 4), (SINGLE_STMT, 1)])
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

        assert isinstance(
            exc.value.trigger, SnowflakeSqlApiTrigger
        ), "Trigger is not a SnowflakeSqlApiTrigger"

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
