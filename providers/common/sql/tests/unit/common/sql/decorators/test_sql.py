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

import json
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.sql.hooks.handlers import fetch_all_handler

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import Connection, task
    from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models import Connection  # type: ignore[assignment]
    from airflow.utils.types import NOTSET as SET_DURING_EXECUTION  # type: ignore[attr-defined,no-redef]

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import timezone
else:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

DEFAULT_DATE = timezone.datetime(2023, 1, 1)
conn_id: str = "my_conn_id"


@pytest.mark.db_test
class TestSqlDecorator:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.dag_maker = dag_maker

        with dag_maker(dag_id="sql_deco_dag") as dag:
            ...

        self.dag = dag

    def execute_task(self, task):
        session = self.dag_maker.session
        dag_run = self.dag_maker.create_dagrun(run_id=f"sql_deco_test_{DEFAULT_DATE.date()}", session=session)
        ti = dag_run.get_task_instance(task.operator.task_id, session=session)
        return_val = task.operator.execute(context={"ti": ti})

        return ti, return_val

    def test_sql_decorator_init(self):
        """Test the initialization of the @task.sql decorator."""
        with self.dag_maker:

            @task.sql(conn_id=conn_id, database="my_database")
            def sql(): ...

            sql_task = sql()

        assert sql_task.operator.task_id == "sql"
        assert sql_task.operator.conn_id == "my_conn_id"
        assert sql_task.operator.database == "my_database"

    def test_templated_fields(self):
        """Test that templated fields are properly rendered."""
        with self.dag_maker(render_template_as_native_obj=True):

            @task.sql(
                conn_id="{{ conn_id }}",
                database="{{ database }}",
                hook_params="{{ hook_params }}",
            )
            def sql():
                return "SELECT 1"

            sql_task = sql()

        # Render template fields with context
        sql_task.operator.render_template_fields(
            {
                "conn_id": "my_conn_id",
                "database": "my_database",
                "hook_params": {"key": "value"},
            }
        )

        # Assert that the templated fields were rendered correctly
        assert sql_task.operator.conn_id == "my_conn_id"
        assert sql_task.operator.database == "my_database"
        assert sql_task.operator.hook_params == {"key": "value"}
        assert sql_task.operator.sql == SET_DURING_EXECUTION

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook", MagicMock())
    def test_sql_query(self):
        """Test the value returned by function matches the .sql parameter."""
        with self.dag_maker:

            @task.sql(conn_id=conn_id)
            def sql():
                return "SELECT 1;"

            sql_task = sql()

        assert sql_task.operator.sql == SET_DURING_EXECUTION

        # We're not testing the return value here; we'll test that later
        _, _ = self.execute_task(sql_task)

        assert sql_task.operator.sql == "SELECT 1;"

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook", MagicMock())
    @pytest.mark.parametrize("sql_query", ["", 1, None, [""], ["SELECT 1;", ""]])
    def test_invalid_sql_query(self, sql_query):
        """Test that an exception is thrown when invalid SQL is returned"""
        with self.dag_maker:

            @task.sql(conn_id=conn_id)
            def sql():
                return sql_query

            sql_task = sql()

        assert sql_task.operator.sql == SET_DURING_EXECUTION

        with pytest.raises(
            TypeError,
            match=(
                "The returned value from the TaskFlow callable must be a non-empty string "
                "or a non-empty list of non-empty strings."
            ),
        ):
            self.execute_task(sql_task)

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_sql_query_return(self, mock_get_db_hook):
        """Test the value returned when executing the task."""
        mock_hook = MagicMock()
        mock_hook.run.return_value = [(1,)]
        mock_get_db_hook.return_value = mock_hook

        with self.dag_maker:

            @task.sql(conn_id=conn_id)
            def sql():
                return "SELECT 1;"

            sql_task = sql()

        _, return_value = self.execute_task(sql_task)

        assert isinstance(return_value, list)
        assert len(return_value) == 1
        assert return_value[0] == (1,)

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator._process_output")
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    @pytest.mark.parametrize("requires_result_fetch", [True, False])
    def test_do_xcom_push(self, mock_get_db_hook, mock_process_output, requires_result_fetch):
        """Test that do_xcom_push properly configures handler and calls _process_output."""
        with self.dag_maker:

            @task.sql(conn_id=conn_id, do_xcom_push=True, requires_result_fetch=requires_result_fetch)
            def sql():
                return "SELECT 1;"

            sql_task = sql()

        # Execute the task (discarding the results)
        _, _ = self.execute_task(sql_task)

        # Verify the hook.run was called with correct parameters
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1;",
            autocommit=False,
            handler=fetch_all_handler,
            parameters=None,
            return_last=True,
        )

        # Verify _process_output was called when do_xcom_push=True
        mock_process_output.assert_called()

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator._process_output")
    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_dont_xcom_push(self, mock_get_db_hook, mock_process_output):
        """Test that do_xcom_push = False properly configures handler and does not call _process_output."""
        with self.dag_maker:

            @task.sql(conn_id=conn_id, do_xcom_push=False)
            def sql():
                return "SELECT 1;"

            sql_task = sql()

        # Execute the task (discarding the results)
        _, _ = self.execute_task(sql_task)

        # Verify the hook.run was called with correct parameters
        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1;",
            autocommit=False,
            handler=None,
            parameters=None,
            return_last=True,
        )

        # Verify _process_output was called when do_xcom_push=True
        mock_process_output.assert_not_called()

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_output_processor(self, mock_get_db_hook):
        """Test that output_processor can transform query results."""
        data = [(1, "Alice"), (2, "Bob")]

        mock_hook = MagicMock()
        mock_hook.run.return_value = data
        mock_hook.descriptions = ("id", "name")
        mock_get_db_hook.return_value = mock_hook

        with self.dag_maker:

            @task.sql(
                conn_id=conn_id,
                output_processor=lambda results, descriptions: (descriptions, results),
                return_last=False,
            )
            def sql():
                return "SELECT * FROM users;"

            sql_task = sql()

        # Execute the task and get the return value
        _, return_value = self.execute_task(sql_task)

        descriptions, result = return_value

        assert descriptions == ("id", "name")
        assert result == [(1, "Alice"), (2, "Bob")]

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook", MagicMock())
    @mock.patch("airflow.providers.common.sql.operators.sql.BaseHook.get_connection")
    def test_operator_extra_dejson_to_hook_params(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id=conn_id, conn_type="postgres", extra=json.dumps({"database": "prod"})
        )

        with self.dag_maker:

            @task.sql(conn_id=conn_id, hook_params={"database": "dev"})
            def sql():
                return "SELECT 1;"

            sql_task = sql()

        hook = sql_task.operator._hook

        assert hook.conn_type == "postgres"
        assert hook.database == "dev"
