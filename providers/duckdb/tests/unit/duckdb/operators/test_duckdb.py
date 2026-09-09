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

from airflow.providers.duckdb.hooks.duckdb import DuckDBHook
from airflow.providers.duckdb.operators.duckdb import DuckDBExecuteQueryOperator
from airflow.providers.duckdb.version_compat import AirflowNotFoundException

GET_CONNECTION = "airflow.providers.duckdb.hooks.duckdb.DuckDBHook.get_connection"


class TestDuckDBExecuteQueryOperator:
    def test_defaults_to_the_duckdb_connection_id(self):
        operator = DuckDBExecuteQueryOperator(task_id="t", sql="SELECT 1")
        assert operator.conn_id == DuckDBHook.default_conn_name

    def test_get_db_hook_returns_a_duckdb_hook(self):
        operator = DuckDBExecuteQueryOperator(task_id="t", sql="SELECT 1")
        hook = operator.get_db_hook()
        assert isinstance(hook, DuckDBHook)
        assert hook.get_conn_id() == DuckDBHook.default_conn_name

    def test_get_db_hook_bypasses_the_connection_lookup(self):
        """The whole point of the override: no Airflow connection row is required."""
        operator = DuckDBExecuteQueryOperator(task_id="t", sql="SELECT 1", conn_id="does_not_exist")
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            hook = operator.get_db_hook()
            assert hook.get_database() == ":memory:"

    def test_hook_params_are_forwarded_to_the_hook(self):
        operator = DuckDBExecuteQueryOperator(
            task_id="t",
            sql="SELECT 1",
            hook_params={"extensions": ["json"], "memory_limit": "1GB", "threads": 2},
        )
        hook = operator.get_db_hook()
        assert hook.memory_limit == "1GB"
        assert hook.threads == 2
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            assert hook.get_extensions() == ["json"]

    def test_hook_params_is_a_template_field(self):
        """Inherited from BaseSQLOperator, so hook params can be rendered per run."""
        assert "hook_params" in DuckDBExecuteQueryOperator.template_fields

    def test_database_parameter_reaches_the_hook(self):
        """
        ``BaseSQLOperator`` applies ``database`` inside ``_hook``, which ``get_db_hook`` bypasses.

        Without the explicit hand-off the parameter is silently dropped and the task writes to a
        throwaway in-memory database instead of the file the author named.
        """
        operator = DuckDBExecuteQueryOperator(
            task_id="t", sql="SELECT 1", database="/tmp/explicit.duckdb", conn_id="does_not_exist"
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            assert operator.get_db_hook().get_database() == "/tmp/explicit.duckdb"

    def test_database_parameter_wins_over_hook_params(self):
        operator = DuckDBExecuteQueryOperator(
            task_id="t",
            sql="SELECT 1",
            database="/tmp/explicit.duckdb",
            hook_params={"database": "/tmp/from_hook_params.duckdb"},
            conn_id="does_not_exist",
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            assert operator.get_db_hook().get_database() == "/tmp/explicit.duckdb"

    def test_hook_params_are_not_mutated(self):
        """The operator must not write back into the dict the Dag author passed in."""
        hook_params: dict = {}
        operator = DuckDBExecuteQueryOperator(
            task_id="t", sql="SELECT 1", database="/tmp/x.duckdb", hook_params=hook_params
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            operator.get_db_hook()
        assert hook_params == {}

    def test_writes_to_the_named_database_file(self, tmp_path):
        """End to end: the file the author named is the file that ends up on disk with the data."""
        database = tmp_path / "standalone.duckdb"
        operator = DuckDBExecuteQueryOperator(
            task_id="t",
            sql=["CREATE TABLE t AS SELECT 42 AS answer", "SELECT answer FROM t"],
            database=str(database),
            conn_id="does_not_exist",
            do_xcom_push=True,
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            # A list of statements yields one result set per statement.
            assert operator.execute({})[-1] == [(42,)]
        assert database.exists()

    def test_executes_sql_against_an_in_memory_database(self):
        operator = DuckDBExecuteQueryOperator(
            task_id="t",
            sql="SELECT 21 * 2 AS answer",
            conn_id="does_not_exist",
            do_xcom_push=True,
        )
        with mock.patch(GET_CONNECTION, side_effect=AirflowNotFoundException("nope")):
            assert operator.execute({}) == [(42,)]
