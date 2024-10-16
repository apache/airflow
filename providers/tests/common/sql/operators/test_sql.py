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

import datetime
import inspect
from unittest import mock
from unittest.mock import MagicMock

import pytest
from tests_common.test_utils.compat import AIRFLOW_V_2_8_PLUS, AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.providers import get_provider_min_airflow_version

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import Connection, DagRun, TaskInstance as TI, XCom
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.common.sql.operators.sql import (
    BaseSQLOperator,
    BranchSQLOperator,
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLIntervalCheckOperator,
    SQLTableCheckOperator,
    SQLThresholdCheckOperator,
    SQLValueCheckOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skipif(not AIRFLOW_V_2_8_PLUS, reason="Tests for Airflow 2.8.0+ only"),
    pytest.mark.skip_if_database_isolation_mode,
]


class MockHook:
    def get_records(self):
        return


def _get_mock_db_hook():
    return MockHook()


class TestBaseSQLOperator:
    def _construct_operator(self, **kwargs):
        dag = DAG(
            "test_dag",
            schedule=None,
            start_date=datetime.datetime(2017, 1, 1),
            render_template_as_native_obj=True,
        )
        return BaseSQLOperator(
            task_id="test_task",
            conn_id="{{ conn_id }}",
            database="{{ database }}",
            hook_params="{{ hook_params }}",
            **kwargs,
            dag=dag,
        )

    def test_templated_fields(self):
        operator = self._construct_operator()
        operator.render_template_fields(
            {"conn_id": "my_conn_id", "database": "my_database", "hook_params": {"key": "value"}}
        )
        assert operator.conn_id == "my_conn_id"
        assert operator.database == "my_database"
        assert operator.hook_params == {"key": "value"}

    def test_when_provider_min_airflow_version_is_3_0_or_higher_remove_obsolete_get_hook_method(self):
        """
        Once this test starts failing due to the fact that the minimum Airflow version is now 3.0.0 or higher
        for this provider, you should remove the obsolete get_hook method in the BaseSQLOperator operator
        and remove this test.  This test was added to make sure to not forget to remove the fallback code
        for backward compatibility with Airflow 2.8.x which isn't need anymore once this provider depends on
        Airflow 3.0.0 or higher.
        """
        min_airflow_version = get_provider_min_airflow_version("apache-airflow-providers-common-sql")

        # Check if the current Airflow version is 3.0.0 or higher
        if min_airflow_version[0] >= 3:
            method_source = inspect.getsource(BaseSQLOperator.get_hook)
            raise AirflowProviderDeprecationWarning(
                f"Check TODO's to remove obsolete get_hook method in BaseSQLOperator:\n\r\n\r\t\t\t{method_source}"
            )


class TestSQLExecuteQueryOperator:
    def _construct_operator(self, sql, **kwargs):
        dag = DAG("test_dag", schedule=None, start_date=datetime.datetime(2017, 1, 1))
        return SQLExecuteQueryOperator(
            task_id="test_task",
            conn_id="default_conn",
            sql=sql,
            **kwargs,
            dag=dag,
        )

    @mock.patch.object(SQLExecuteQueryOperator, "_process_output")
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_do_xcom_push(self, mock_get_db_hook, mock_process_output):
        operator = self._construct_operator("SELECT 1;", do_xcom_push=True)
        operator.execute(context=MagicMock())

        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1;",
            autocommit=False,
            handler=fetch_all_handler,
            parameters=None,
            return_last=True,
        )
        mock_process_output.assert_called()

    @mock.patch.object(SQLExecuteQueryOperator, "_process_output")
    @mock.patch.object(SQLExecuteQueryOperator, "get_db_hook")
    def test_dont_xcom_push(self, mock_get_db_hook, mock_process_output):
        operator = self._construct_operator("SELECT 1;", do_xcom_push=False)
        operator.execute(context=MagicMock())

        mock_get_db_hook.return_value.run.assert_called_once_with(
            sql="SELECT 1;",
            autocommit=False,
            parameters=None,
            handler=None,
            return_last=True,
        )
        mock_process_output.assert_not_called()


class TestColumnCheckOperator:
    valid_column_mapping = {
        "X": {
            "null_check": {"equal_to": 0},
            "distinct_check": {"equal_to": 10, "tolerance": 0.1},
            "unique_check": {"geq_to": 10},
            "min": {"leq_to": 1},
            "max": {"less_than": 20, "greater_than": 10},
        }
    }

    short_valid_column_mapping = {
        "X": {
            "null_check": {"equal_to": 0},
            "distinct_check": {"equal_to": 10, "tolerance": 0.1},
        }
    }

    invalid_column_mapping = {"Y": {"invalid_check_name": {"expectation": 5}}}

    correct_generate_sql_query_no_partitions = """
        SELECT 'X' AS col_name, 'null_check' AS check_type, X_null_check AS check_result
        FROM (SELECT SUM(CASE WHEN X IS NULL THEN 1 ELSE 0 END) AS X_null_check FROM test_table ) AS sq
    UNION ALL
        SELECT 'X' AS col_name, 'distinct_check' AS check_type, X_distinct_check AS check_result
        FROM (SELECT COUNT(DISTINCT(X)) AS X_distinct_check FROM test_table ) AS sq
    """

    correct_generate_sql_query_with_partition = """
        SELECT 'X' AS col_name, 'null_check' AS check_type, X_null_check AS check_result
        FROM (SELECT SUM(CASE WHEN X IS NULL THEN 1 ELSE 0 END) AS X_null_check FROM test_table WHERE Y > 1) AS sq
    UNION ALL
        SELECT 'X' AS col_name, 'distinct_check' AS check_type, X_distinct_check AS check_result
        FROM (SELECT COUNT(DISTINCT(X)) AS X_distinct_check FROM test_table WHERE Y > 1) AS sq
    """

    correct_generate_sql_query_with_partition_and_where = """
        SELECT 'X' AS col_name, 'null_check' AS check_type, X_null_check AS check_result
        FROM (SELECT SUM(CASE WHEN X IS NULL THEN 1 ELSE 0 END) AS X_null_check FROM test_table WHERE Y > 1 AND Z < 100) AS sq
    UNION ALL
        SELECT 'X' AS col_name, 'distinct_check' AS check_type, X_distinct_check AS check_result
        FROM (SELECT COUNT(DISTINCT(X)) AS X_distinct_check FROM test_table WHERE Y > 1) AS sq
    """

    correct_generate_sql_query_with_where = """
        SELECT 'X' AS col_name, 'null_check' AS check_type, X_null_check AS check_result
        FROM (SELECT SUM(CASE WHEN X IS NULL THEN 1 ELSE 0 END) AS X_null_check FROM test_table ) AS sq
    UNION ALL
        SELECT 'X' AS col_name, 'distinct_check' AS check_type, X_distinct_check AS check_result
        FROM (SELECT COUNT(DISTINCT(X)) AS X_distinct_check FROM test_table WHERE Z < 100) AS sq
    """

    def _construct_operator(self, monkeypatch, column_mapping, records):
        def get_records(*arg):
            return records

        operator = SQLColumnCheckOperator(
            task_id="test_task", table="test_table", column_mapping=column_mapping
        )
        monkeypatch.setattr(operator, "get_db_hook", _get_mock_db_hook)
        monkeypatch.setattr(MockHook, "get_records", get_records)
        return operator

    def _full_check_sql(self, sql: str) -> str:
        """
        Wraps the check fragment in the outer parts of the sql query
        """
        return f"SELECT col_name, check_type, check_result FROM ({sql}) AS check_columns"

    def test_check_not_in_column_checks(self, monkeypatch):
        with pytest.raises(AirflowException, match="Invalid column check: invalid_check_name."):
            self._construct_operator(monkeypatch, self.invalid_column_mapping, ())

    def test_pass_all_checks_exact_check(self, monkeypatch):
        records = [
            ("X", "null_check", 0),
            ("X", "distinct_check", 10),
            ("X", "unique_check", 10),
            ("X", "min", 1),
            ("X", "max", 19),
        ]
        operator = self._construct_operator(monkeypatch, self.valid_column_mapping, records)
        operator.execute(context=MagicMock())
        assert [
            operator.column_mapping["X"][check]["success"] is True
            for check in [*operator.column_mapping["X"]]
        ]

    def test_max_less_than_fails_check(self, monkeypatch):
        records = [
            ("X", "null_check", 1),
            ("X", "distinct_check", 10),
            ("X", "unique_check", 10),
            ("X", "min", 1),
            ("X", "max", 21),
        ]
        operator = self._construct_operator(monkeypatch, self.valid_column_mapping, records)
        with pytest.raises(AirflowException, match="Test failed") as err_ctx:
            operator.execute(context=MagicMock())
        assert "Check: max" in str(err_ctx.value)
        assert "{'less_than': 20, 'greater_than': 10, 'result': 21, 'success': False}" in str(err_ctx.value)
        assert operator.column_mapping["X"]["max"]["success"] is False

    def test_max_greater_than_fails_check(self, monkeypatch):
        records = [
            ("X", "null_check", 1),
            ("X", "distinct_check", 10),
            ("X", "unique_check", 10),
            ("X", "min", 1),
            ("X", "max", 9),
        ]
        operator = self._construct_operator(monkeypatch, self.valid_column_mapping, records)
        with pytest.raises(AirflowException, match="Test failed") as err_ctx:
            operator.execute(context=MagicMock())
        assert "Check: max" in str(err_ctx.value)
        assert "{'less_than': 20, 'greater_than': 10, 'result': 9, 'success': False}" in str(err_ctx.value)
        assert operator.column_mapping["X"]["max"]["success"] is False

    def test_pass_all_checks_inexact_check(self, monkeypatch):
        records = [
            ("X", "null_check", 0),
            ("X", "distinct_check", 9),
            ("X", "unique_check", 12),
            ("X", "min", 0),
            ("X", "max", 15),
        ]
        operator = self._construct_operator(monkeypatch, self.valid_column_mapping, records)
        operator.execute(context=MagicMock())
        assert [
            operator.column_mapping["X"][check]["success"] is True
            for check in [*operator.column_mapping["X"]]
        ]

    def test_fail_all_checks_check(self, monkeypatch):
        records = [
            ("X", "null_check", 1),
            ("X", "distinct_check", 12),
            ("X", "unique_check", 11),
            ("X", "min", -1),
            ("X", "max", 20),
        ]
        operator = operator = self._construct_operator(monkeypatch, self.valid_column_mapping, records)
        with pytest.raises(AirflowException):
            operator.execute(context=MagicMock())

    def test_generate_sql_query_no_partitions(self, monkeypatch):
        checks = self.short_valid_column_mapping["X"]
        operator = self._construct_operator(monkeypatch, self.short_valid_column_mapping, ())
        assert (
            operator._generate_sql_query("X", checks).lstrip()
            == self.correct_generate_sql_query_no_partitions.lstrip()
        )

    def test_generate_sql_query_with_partitions(self, monkeypatch):
        checks = self.short_valid_column_mapping["X"]
        operator = self._construct_operator(monkeypatch, self.short_valid_column_mapping, ())
        operator.partition_clause = "Y > 1"
        assert (
            operator._generate_sql_query("X", checks).lstrip()
            == self.correct_generate_sql_query_with_partition.lstrip()
        )

    @pytest.mark.db_test
    def test_generate_sql_query_with_templated_partitions(self, monkeypatch):
        checks = self.short_valid_column_mapping["X"]
        operator = self._construct_operator(monkeypatch, self.short_valid_column_mapping, ())
        operator.partition_clause = "{{ params.col }} > 1"
        operator.render_template_fields({"params": {"col": "Y"}})
        assert (
            operator._generate_sql_query("X", checks).lstrip()
            == self.correct_generate_sql_query_with_partition.lstrip()
        )

    def test_generate_sql_query_with_partitions_and_check_partition(self, monkeypatch):
        self.short_valid_column_mapping["X"]["null_check"]["partition_clause"] = "Z < 100"
        checks = self.short_valid_column_mapping["X"]
        operator = self._construct_operator(monkeypatch, self.short_valid_column_mapping, ())
        operator.partition_clause = "Y > 1"
        assert (
            operator._generate_sql_query("X", checks).lstrip()
            == self.correct_generate_sql_query_with_partition_and_where.lstrip()
        )
        del self.short_valid_column_mapping["X"]["null_check"]["partition_clause"]

    def test_generate_sql_query_with_check_partition(self, monkeypatch):
        self.short_valid_column_mapping["X"]["distinct_check"]["partition_clause"] = "Z < 100"
        checks = self.short_valid_column_mapping["X"]
        operator = self._construct_operator(monkeypatch, self.short_valid_column_mapping, ())
        assert (
            operator._generate_sql_query("X", checks).lstrip()
            == self.correct_generate_sql_query_with_where.lstrip()
        )
        del self.short_valid_column_mapping["X"]["distinct_check"]["partition_clause"]

    @pytest.mark.db_test
    @mock.patch.object(SQLColumnCheckOperator, "get_db_hook")
    def test_generated_sql_respects_templated_partitions(self, mock_get_db_hook):
        records = [
            ("X", "null_check", 0),
            ("X", "distinct_check", 10),
        ]

        mock_hook = mock.Mock()
        mock_hook.get_records.return_value = records
        mock_get_db_hook.return_value = mock_hook

        operator = SQLColumnCheckOperator(
            task_id="test_task",
            table="test_table",
            column_mapping=self.short_valid_column_mapping,
            partition_clause="{{ params.col }} > 1",
        )
        operator.render_template_fields({"params": {"col": "Y"}})

        operator.execute(context=MagicMock())

        mock_get_db_hook.return_value.get_records.assert_called_once_with(
            self._full_check_sql(self.correct_generate_sql_query_with_partition),
        )

    @pytest.mark.db_test
    @mock.patch.object(SQLColumnCheckOperator, "get_db_hook")
    def test_generated_sql_respects_templated_table(self, mock_get_db_hook):
        records = [
            ("X", "null_check", 0),
            ("X", "distinct_check", 10),
        ]

        mock_hook = mock.Mock()
        mock_hook.get_records.return_value = records
        mock_get_db_hook.return_value = mock_hook

        operator = SQLColumnCheckOperator(
            task_id="test_task",
            table="{{ params.table }}",
            column_mapping=self.short_valid_column_mapping,
        )
        operator.render_template_fields({"params": {"table": "test_table"}})

        operator.execute(context=MagicMock())

        mock_get_db_hook.return_value.get_records.assert_called_once_with(
            self._full_check_sql(self.correct_generate_sql_query_no_partitions),
        )


class TestTableCheckOperator:
    count_check = "COUNT(*) == 1000"
    sum_check = "col_a + col_b < col_c"
    checks = {
        "row_count_check": {"check_statement": f"{count_check}"},
        "column_sum_check": {"check_statement": f"{sum_check}"},
    }

    correct_generate_sql_query_no_partitions = f"""
    SELECT 'row_count_check' AS check_name, MIN(row_count_check) AS check_result
    FROM (SELECT CASE WHEN {count_check} THEN 1 ELSE 0 END AS row_count_check
          FROM test_table ) AS sq
    UNION ALL
    SELECT 'column_sum_check' AS check_name, MIN(column_sum_check) AS check_result
    FROM (SELECT CASE WHEN {sum_check} THEN 1 ELSE 0 END AS column_sum_check
          FROM test_table ) AS sq
    """

    correct_generate_sql_query_with_partition = f"""
    SELECT 'row_count_check' AS check_name, MIN(row_count_check) AS check_result
    FROM (SELECT CASE WHEN {count_check} THEN 1 ELSE 0 END AS row_count_check
          FROM test_table WHERE col_a > 10) AS sq
    UNION ALL
    SELECT 'column_sum_check' AS check_name, MIN(column_sum_check) AS check_result
    FROM (SELECT CASE WHEN {sum_check} THEN 1 ELSE 0 END AS column_sum_check
          FROM test_table WHERE col_a > 10) AS sq
    """

    correct_generate_sql_query_with_partition_and_where = f"""
    SELECT 'row_count_check' AS check_name, MIN(row_count_check) AS check_result
    FROM (SELECT CASE WHEN {count_check} THEN 1 ELSE 0 END AS row_count_check
          FROM test_table WHERE col_a > 10 AND id = 100) AS sq
    UNION ALL
    SELECT 'column_sum_check' AS check_name, MIN(column_sum_check) AS check_result
    FROM (SELECT CASE WHEN {sum_check} THEN 1 ELSE 0 END AS column_sum_check
          FROM test_table WHERE col_a > 10) AS sq
    """

    correct_generate_sql_query_with_where = f"""
    SELECT 'row_count_check' AS check_name, MIN(row_count_check) AS check_result
    FROM (SELECT CASE WHEN {count_check} THEN 1 ELSE 0 END AS row_count_check
          FROM test_table ) AS sq
    UNION ALL
    SELECT 'column_sum_check' AS check_name, MIN(column_sum_check) AS check_result
    FROM (SELECT CASE WHEN {sum_check} THEN 1 ELSE 0 END AS column_sum_check
          FROM test_table WHERE id = 100) AS sq
    """

    def _construct_operator(self, monkeypatch, checks, records):
        def get_records(*arg):
            return records

        operator = SQLTableCheckOperator(task_id="test_task", table="test_table", checks=checks)
        monkeypatch.setattr(operator, "get_db_hook", _get_mock_db_hook)
        monkeypatch.setattr(MockHook, "get_records", get_records)
        return operator

    @pytest.mark.parametrize(
        ["conn_id"],
        [
            pytest.param("postgres_default", marks=[pytest.mark.backend("postgres")]),
            pytest.param("mysql_default", marks=[pytest.mark.backend("mysql")]),
        ],
    )
    def test_sql_check(self, conn_id):
        operator = SQLTableCheckOperator(
            task_id="test_task",
            table="employees",
            checks={"row_count_check": {"check_statement": "COUNT(*) >= 3"}},
            conn_id=conn_id,
        )

        hook = operator.get_db_hook()
        hook.run(
            [
                """
                CREATE TABLE IF NOT EXISTS employees (
                    employee_name VARCHAR(50) NOT NULL,
                    employment_year INT NOT NULL
                );
                """,
                "INSERT INTO employees VALUES ('Adam', 2021)",
                "INSERT INTO employees VALUES ('Chris', 2021)",
                "INSERT INTO employees VALUES ('Frank', 2021)",
                "INSERT INTO employees VALUES ('Fritz', 2021)",
                "INSERT INTO employees VALUES ('Magda', 2022)",
                "INSERT INTO employees VALUES ('Phil', 2021)",
            ]
        )
        try:
            operator.execute({})
        finally:
            hook.run(["DROP TABLE employees"])

    @pytest.mark.parametrize(
        ["conn_id"],
        [
            pytest.param("postgres_default", marks=[pytest.mark.backend("postgres")]),
            pytest.param("mysql_default", marks=[pytest.mark.backend("mysql")]),
        ],
    )
    def test_sql_check_partition_clause_templating(self, conn_id):
        """
        Checks that the generated sql respects a templated partition clause
        """
        operator = SQLTableCheckOperator(
            task_id="test_task",
            table="employees",
            checks={"row_count_check": {"check_statement": "COUNT(*) = 5"}},
            conn_id=conn_id,
            partition_clause="employment_year = {{ params.year }}",
        )

        hook = operator.get_db_hook()
        hook.run(
            [
                """
                CREATE TABLE IF NOT EXISTS employees (
                    employee_name VARCHAR(50) NOT NULL,
                    employment_year INT NOT NULL
                );
                """,
                "INSERT INTO employees VALUES ('Adam', 2021)",
                "INSERT INTO employees VALUES ('Chris', 2021)",
                "INSERT INTO employees VALUES ('Frank', 2021)",
                "INSERT INTO employees VALUES ('Fritz', 2021)",
                "INSERT INTO employees VALUES ('Magda', 2022)",
                "INSERT INTO employees VALUES ('Phil', 2021)",
            ]
        )
        try:
            operator.render_template_fields({"params": {"year": 2021}})
            operator.execute({})
        finally:
            hook.run(["DROP TABLE employees"])

    def test_pass_all_checks_check(self, monkeypatch):
        records = [("row_count_check", 1), ("column_sum_check", "y")]
        operator = self._construct_operator(monkeypatch, self.checks, records)
        operator.execute(context=MagicMock())
        assert [operator.checks[check]["success"] is True for check in operator.checks.keys()]

    def test_fail_all_checks_check(self, monkeypatch):
        records = [("row_count_check", 0), ("column_sum_check", "n")]
        operator = self._construct_operator(monkeypatch, self.checks, records)
        with pytest.raises(AirflowException):
            operator.execute(context=MagicMock())

    def test_generate_sql_query_no_partitions(self, monkeypatch):
        operator = self._construct_operator(monkeypatch, self.checks, ())
        assert (
            operator._generate_sql_query().lstrip() == self.correct_generate_sql_query_no_partitions.lstrip()
        )

    def test_generate_sql_query_with_partitions(self, monkeypatch):
        operator = self._construct_operator(monkeypatch, self.checks, ())
        operator.partition_clause = "col_a > 10"
        assert (
            operator._generate_sql_query().lstrip() == self.correct_generate_sql_query_with_partition.lstrip()
        )

    @pytest.mark.db_test
    def test_generate_sql_query_with_templated_partitions(self, monkeypatch):
        operator = self._construct_operator(monkeypatch, self.checks, ())
        operator.partition_clause = "{{ params.col }} > 10"
        operator.render_template_fields({"params": {"col": "col_a"}})
        assert (
            operator._generate_sql_query().lstrip() == self.correct_generate_sql_query_with_partition.lstrip()
        )

    @pytest.mark.db_test
    def test_generate_sql_query_with_templated_table(self, monkeypatch):
        operator = self._construct_operator(monkeypatch, self.checks, ())
        operator.table = "{{ params.table }}"
        operator.render_template_fields({"params": {"table": "test_table"}})
        assert (
            operator._generate_sql_query().lstrip() == self.correct_generate_sql_query_no_partitions.lstrip()
        )

    def test_generate_sql_query_with_partitions_and_check_partition(self, monkeypatch):
        self.checks["row_count_check"]["partition_clause"] = "id = 100"
        operator = self._construct_operator(monkeypatch, self.checks, ())
        operator.partition_clause = "col_a > 10"
        assert (
            operator._generate_sql_query().lstrip()
            == self.correct_generate_sql_query_with_partition_and_where.lstrip()
        )
        del self.checks["row_count_check"]["partition_clause"]

    def test_generate_sql_query_with_check_partition(self, monkeypatch):
        self.checks["column_sum_check"]["partition_clause"] = "id = 100"
        operator = self._construct_operator(monkeypatch, self.checks, ())
        assert operator._generate_sql_query().lstrip() == self.correct_generate_sql_query_with_where.lstrip()
        del self.checks["column_sum_check"]["partition_clause"]


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)
SUPPORTED_TRUE_VALUES = [
    ["True"],
    ["true"],
    ["1"],
    ["on"],
    [1],
    True,
    "true",
    "1",
    "on",
    1,
]
SUPPORTED_FALSE_VALUES = [
    ["False"],
    ["false"],
    ["0"],
    ["off"],
    [0],
    False,
    "false",
    "0",
    "off",
    0,
]


class TestSQLCheckOperatorDbHook:
    def setup_method(self):
        self.task_id = "test_task"
        self.conn_id = "sql_default"
        self._operator = SQLCheckOperator(task_id=self.task_id, conn_id=self.conn_id, sql="sql")

    @pytest.mark.parametrize("database", [None, "test-db"])
    def test_get_hook(self, database):
        with mock.patch(
            "airflow.providers.common.sql.operators.sql.BaseHook.get_connection",
            return_value=Connection(conn_id="sql_default", conn_type="postgres"),
        ) as mock_get_conn:
            if database:
                self._operator.database = database
            assert isinstance(self._operator._hook, PostgresHook)
            mock_get_conn.assert_called_once_with(self.conn_id)

    def test_not_allowed_conn_type(self):
        with mock.patch(
            "airflow.providers.common.sql.operators.sql.BaseHook.get_connection",
            return_value=Connection(conn_id="sql_default", conn_type="postgres"),
        ) as mock_get_conn:
            mock_get_conn.return_value = Connection(conn_id="sql_default", conn_type="airbyte")
            with pytest.raises(AirflowException, match=r"You are trying to use `common-sql`"):
                self._operator._hook

    def test_sql_operator_hook_params_snowflake(self):
        with mock.patch(
            "airflow.providers.common.sql.operators.sql.BaseHook.get_connection",
            return_value=Connection(conn_id="sql_default", conn_type="postgres"),
        ) as mock_get_conn:
            mock_get_conn.return_value = Connection(conn_id="snowflake_default", conn_type="snowflake")
            self._operator.hook_params = {
                "warehouse": "warehouse",
                "database": "database",
                "role": "role",
                "schema": "schema",
                "log_sql": False,
            }
            assert self._operator._hook.conn_type == "snowflake"
            assert self._operator._hook.warehouse == "warehouse"
            assert self._operator._hook.database == "database"
            assert self._operator._hook.role == "role"
            assert self._operator._hook.schema == "schema"
            assert not self._operator._hook.log_sql

    def test_sql_operator_hook_params_biguery(self):
        with mock.patch(
            "airflow.providers.common.sql.operators.sql.BaseHook.get_connection",
            return_value=Connection(conn_id="sql_default", conn_type="postgres"),
        ) as mock_get_conn:
            mock_get_conn.return_value = Connection(
                conn_id="google_cloud_bigquery_default", conn_type="gcpbigquery"
            )
            self._operator.hook_params = {"use_legacy_sql": True, "location": "us-east1"}
            assert self._operator._hook.conn_type == "gcpbigquery"
            assert self._operator._hook.use_legacy_sql
            assert self._operator._hook.location == "us-east1"

    def test_sql_operator_hook_params_templated(self):
        with mock.patch(
            "airflow.providers.common.sql.operators.sql.BaseHook.get_connection",
            return_value=Connection(conn_id="sql_default", conn_type="postgres"),
        ) as mock_get_conn:
            mock_get_conn.return_value = Connection(conn_id="snowflake_default", conn_type="snowflake")
            self._operator.hook_params = {"session_parameters": {"query_tag": "{{ ds }}"}}
            logical_date = "2024-04-02"
            self._operator.render_template_fields({"ds": logical_date})

            assert self._operator._hook.conn_type == "snowflake"
            assert self._operator._hook.session_parameters == {"query_tag": logical_date}


class TestCheckOperator:
    def setup_method(self):
        self._operator = SQLCheckOperator(task_id="test_task", sql="sql", parameters="parameters")

    @mock.patch.object(SQLCheckOperator, "get_db_hook")
    def test_execute_no_records(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = []

        with pytest.raises(AirflowException, match=r"The following query returned zero rows: sql"):
            self._operator.execute({})

    @mock.patch.object(SQLCheckOperator, "get_db_hook")
    def test_execute_not_all_records_are_true(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = ["data", ""]

        with pytest.raises(AirflowException, match=r"Test failed."):
            self._operator.execute({})

    @mock.patch.object(SQLCheckOperator, "get_db_hook")
    def test_execute_records_dict_not_all_values_are_true(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = {
            "DUPLICATE_ID_CHECK": False,
            "NULL_VALUES_CHECK": True,
        }

        with pytest.raises(AirflowException, match=r"Test failed."):
            self._operator.execute({})

    @mock.patch.object(SQLCheckOperator, "get_db_hook")
    def test_sqlcheckoperator_parameters(self, mock_get_db_hook):
        self._operator.execute({})
        mock_get_db_hook.return_value.get_first.assert_called_once_with("sql", "parameters")


class TestValueCheckOperator:
    def setup_method(self):
        self.task_id = "test_task"
        self.conn_id = "default_conn"

    def _construct_operator(self, sql, pass_value, tolerance=None):
        dag = DAG("test_dag", schedule=None, start_date=datetime.datetime(2017, 1, 1))

        return SQLValueCheckOperator(
            dag=dag,
            task_id=self.task_id,
            conn_id=self.conn_id,
            sql=sql,
            pass_value=pass_value,
            tolerance=tolerance,
        )

    @pytest.mark.db_test
    def test_pass_value_template_string(self):
        pass_value_str = "2018-03-22"
        operator = self._construct_operator("select date from tab1;", "{{ ds }}")

        operator.render_template_fields({"ds": pass_value_str})

        assert operator.task_id == self.task_id
        assert operator.pass_value == pass_value_str

    @pytest.mark.db_test
    def test_pass_value_template_string_float(self):
        pass_value_float = 4.0
        operator = self._construct_operator("select date from tab1;", pass_value_float)

        operator.render_template_fields({})

        assert operator.task_id == self.task_id
        assert operator.pass_value == str(pass_value_float)

    @mock.patch.object(SQLValueCheckOperator, "get_db_hook")
    def test_execute_pass(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [10]
        mock_get_db_hook.return_value = mock_hook
        sql = "select value from tab1 limit 1;"
        operator = self._construct_operator(sql, 5, 1)

        operator.execute(None)

        mock_hook.get_first.assert_called_once_with(sql)

    @mock.patch.object(SQLValueCheckOperator, "get_db_hook")
    def test_execute_fail(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("select value from tab1 limit 1;", 5, 1)

        with pytest.raises(AirflowException, match="Tolerance:100.0%"):
            operator.execute(context=MagicMock())


class TestIntervalCheckOperator:
    def _construct_operator(self, table, metric_thresholds, ratio_formula, ignore_zero):
        return SQLIntervalCheckOperator(
            task_id="test_task",
            table=table,
            metrics_thresholds=metric_thresholds,
            ratio_formula=ratio_formula,
            ignore_zero=ignore_zero,
        )

    def test_invalid_ratio_formula(self):
        with pytest.raises(AirflowException, match="Invalid diff_method"):
            self._construct_operator(
                table="test_table",
                metric_thresholds={
                    "f1": 1,
                },
                ratio_formula="abs",
                ignore_zero=False,
            )

    @mock.patch.object(SQLIntervalCheckOperator, "get_db_hook")
    def test_execute_not_ignore_zero(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [0]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={
                "f1": 1,
            },
            ratio_formula="max_over_min",
            ignore_zero=False,
        )

        with pytest.raises(AirflowException):
            operator.execute(context=MagicMock())

    @mock.patch.object(SQLIntervalCheckOperator, "get_db_hook")
    def test_execute_ignore_zero(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [0]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={
                "f1": 1,
            },
            ratio_formula="max_over_min",
            ignore_zero=True,
        )

        operator.execute(context=MagicMock())

    @mock.patch.object(SQLIntervalCheckOperator, "get_db_hook")
    def test_execute_min_max(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [2, 2, 2, 2],  # reference
                [1, 1, 1, 1],  # current
            ]

            yield from rows

        mock_hook.get_first.side_effect = returned_row()
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={
                "f0": 1.0,
                "f1": 1.5,
                "f2": 2.0,
                "f3": 2.5,
            },
            ratio_formula="max_over_min",
            ignore_zero=True,
        )

        with pytest.raises(AirflowException, match="f0, f1, f2"):
            operator.execute(context=MagicMock())

    @mock.patch.object(SQLIntervalCheckOperator, "get_db_hook")
    def test_execute_diff(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [3, 3, 3, 3],  # reference
                [1, 1, 1, 1],  # current
            ]

            yield from rows

        mock_hook.get_first.side_effect = returned_row()
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={
                "f0": 0.5,
                "f1": 0.6,
                "f2": 0.7,
                "f3": 0.8,
            },
            ratio_formula="relative_diff",
            ignore_zero=True,
        )

        with pytest.raises(AirflowException, match="f0, f1"):
            operator.execute(context=MagicMock())


class TestThresholdCheckOperator:
    def _construct_operator(self, sql, min_threshold, max_threshold):
        dag = DAG("test_dag", schedule=None, start_date=datetime.datetime(2017, 1, 1))

        return SQLThresholdCheckOperator(
            task_id="test_task",
            sql=sql,
            min_threshold=min_threshold,
            max_threshold=max_threshold,
            dag=dag,
        )

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_pass_min_value_max_value(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = (10,)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select avg(val) from table1 limit 1", 1, 100)

        operator.execute(context=MagicMock())

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_pass_min_value_max_value_templated(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = (10,)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select avg(val) from table1 limit 1", "{{ params.min }}", 100)
        operator.render_template_fields({"params": {"min": 1}})
        operator.execute(context=MagicMock())
        mock_hook.get_first.assert_called_once_with("Select avg(val) from table1 limit 1")

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_fail_min_value_max_value(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = (10,)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select avg(val) from table1 limit 1", 20, 100)

        with pytest.raises(AirflowException, match="10.*20.0.*100.0"):
            operator.execute(context=MagicMock())

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_pass_min_sql_max_sql(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: (int(x.split()[1]),)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select 10", "Select 1", "Select 100")

        operator.execute(context=MagicMock())

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_fail_min_sql_max_sql(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: (int(x.split()[1]),)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select 10", "Select 20", "Select 100")

        with pytest.raises(AirflowException, match="10.*20.*100"):
            operator.execute(context=MagicMock())

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    @pytest.mark.parametrize(
        ("sql", "min_threshold", "max_threshold"),
        (
            ("Select 75", 45, "Select 100"),
            # check corner-case if result of query is "falsey" does not raise error
            ("Select 0", 0, 1),
            ("Select 1", 0, 1),
        ),
    )
    def test_pass_min_value_max_sql(self, mock_get_db_hook, sql, min_threshold, max_threshold):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: (int(x.split()[1]),)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(sql, min_threshold, max_threshold)

        operator.execute(context=MagicMock())

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_fail_min_sql_max_value(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: (int(x.split()[1]),)
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select 155", "Select 45", 100)

        with pytest.raises(AirflowException, match="155.*45.*100.0"):
            operator.execute(context=MagicMock())

    @mock.patch.object(SQLThresholdCheckOperator, "get_db_hook")
    def test_fail_if_query_returns_no_rows(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = None
        mock_get_db_hook.return_value = mock_hook

        sql = "Select val from table1 where val = 'val not in table'"
        operator = self._construct_operator(sql, 20, 100)

        with pytest.raises(AirflowException, match=f"The following query returned zero rows: {sql}"):
            operator.execute(context=MagicMock())


@pytest.mark.db_test
class TestSqlBranch:
    """
    Test for SQL Branch Operator
    """

    @classmethod
    def setup_class(cls):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(XCom).delete()

    def setup_method(self):
        self.dag = DAG(
            "sql_branch_operator_test",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
        )
        self.branch_1 = EmptyOperator(task_id="branch_1", dag=self.dag)
        self.branch_2 = EmptyOperator(task_id="branch_2", dag=self.dag)
        self.branch_3 = None

    def teardown_method(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(XCom).delete()

    @pytest.mark.db_test
    def test_unsupported_conn_type(self):
        """Check if BranchSQLOperator throws an exception for unsupported connection type"""
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="redis_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_conn(self):
        """Check if BranchSQLOperator throws an exception for invalid connection"""
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_follow_task_true(self):
        """Check if BranchSQLOperator throws an exception for invalid connection"""
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true=None,
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_follow_task_false(self):
        """Check if BranchSQLOperator throws an exception for invalid connection"""
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false=None,
            dag=self.dag,
        )

        with pytest.raises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.backend("mysql")
    def test_sql_branch_operator_mysql(self):
        """Check if BranchSQLOperator works with backend"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.backend("postgres")
    def test_sql_branch_operator_postgres(self):
        """Check if BranchSQLOperator works with backend"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="postgres_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_branch_single_value_with_dag_run(self, mock_get_db_hook):
        """Check BranchSQLOperator branch operation"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first

        mock_get_records.return_value = 1

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                assert ti.state == State.NONE
            elif ti.task_id == "branch_2":
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_branch_true_with_dag_run(self, mock_get_db_hook):
        """Check BranchSQLOperator branch operation"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first

        for true_value in SUPPORTED_TRUE_VALUES:
            mock_get_records.return_value = true_value

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    assert ti.state == State.SUCCESS
                elif ti.task_id == "branch_1":
                    assert ti.state == State.NONE
                elif ti.task_id == "branch_2":
                    assert ti.state == State.SKIPPED
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_branch_false_with_dag_run(self, mock_get_db_hook):
        """Check BranchSQLOperator branch operation"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first

        for false_value in SUPPORTED_FALSE_VALUES:
            mock_get_records.return_value = false_value
            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    assert ti.state == State.SUCCESS
                elif ti.task_id == "branch_1":
                    assert ti.state == State.SKIPPED
                elif ti.task_id == "branch_2":
                    assert ti.state == State.NONE
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_branch_list_with_dag_run(self, mock_get_db_hook):
        """Checks if the BranchSQLOperator supports branching off to a list of tasks."""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true=["branch_1", "branch_2"],
            follow_task_ids_if_false="branch_3",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.branch_3 = EmptyOperator(task_id="branch_3", dag=self.dag)
        self.branch_3.set_upstream(branch_op)
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first
        mock_get_records.return_value = [["1"]]

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id in ("branch_1", "branch_2"):
                assert ti.state == State.NONE
            elif ti.task_id == "branch_3":
                assert ti.state == State.SKIPPED
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_invalid_query_result_with_dag_run(self, mock_get_db_hook):
        """Check BranchSQLOperator branch operation"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first

        mock_get_records.return_value = ["Invalid Value"]

        with pytest.raises(AirflowException):
            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_with_skip_in_branch_downstream_dependencies(self, mock_get_db_hook):
        """Test SQL Branch with skipping all downstream dependencies"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first

        for true_value in SUPPORTED_TRUE_VALUES:
            mock_get_records.return_value = [true_value]

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    assert ti.state == State.SUCCESS
                elif ti.task_id in ("branch_1", "branch_2"):
                    assert ti.state == State.NONE
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_with_skip_in_branch_downstream_dependencies2(self, mock_get_db_hook):
        """Test skipping downstream dependency for false condition"""
        branch_op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        mock_get_records = mock_get_db_hook.return_value.get_first

        for false_value in SUPPORTED_FALSE_VALUES:
            mock_get_records.return_value = [false_value]

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    assert ti.state == State.SUCCESS
                elif ti.task_id == "branch_1":
                    assert ti.state == State.SKIPPED
                elif ti.task_id == "branch_2":
                    assert ti.state == State.NONE
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")


class TestBaseSQLOperatorSubClass:
    from airflow.providers.common.sql.operators.sql import BaseSQLOperator

    class NewStyleBaseSQLOperatorSubClass(BaseSQLOperator):
        """New style subclass of BaseSQLOperator"""

        conn_id_field = "custom_conn_id_field"

        def __init__(self, custom_conn_id_field="test_conn", **kwargs):
            super().__init__(**kwargs)
            self.custom_conn_id_field = custom_conn_id_field

    class OldStyleBaseSQLOperatorSubClass(BaseSQLOperator):
        """Old style subclass of BaseSQLOperator"""

        def __init__(self, custom_conn_id_field="test_conn", **kwargs):
            super().__init__(conn_id=custom_conn_id_field, **kwargs)

    @pytest.mark.parametrize(
        "operator_class", [NewStyleBaseSQLOperatorSubClass, OldStyleBaseSQLOperatorSubClass]
    )
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_new_style_subclass(self, mock_get_connection, operator_class):
        from airflow.providers.common.sql.hooks.sql import DbApiHook

        op = operator_class(task_id="test_task")
        mock_get_connection.return_value.get_hook.return_value = MagicMock(spec=DbApiHook)
        op.get_db_hook()
        mock_get_connection.assert_called_once_with("test_conn")
