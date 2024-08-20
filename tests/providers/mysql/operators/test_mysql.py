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

import os
from contextlib import closing
from unittest.mock import MagicMock

import pytest

from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SQLJobFacet,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone
from tests.providers.mysql.hooks.test_mysql import MySqlContext

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"
MYSQL_DEFAULT = "mysql_default"


@pytest.mark.backend("mysql")
class TestMySql:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, schedule=None, default_args=args)
        self.dag = dag

    def teardown_method(self):
        drop_tables = {"test_mysql_to_mysql", "test_airflow"}
        with closing(MySqlHook().get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                for table in drop_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")

    @pytest.mark.parametrize("client", ["mysqlclient", "mysql-connector-python"])
    def test_mysql_operator_test(self, client):
        with MySqlContext(client):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            op = SQLExecuteQueryOperator(task_id="basic_mysql", sql=sql, dag=self.dag, conn_id=MYSQL_DEFAULT)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.parametrize("client", ["mysqlclient", "mysql-connector-python"])
    def test_mysql_operator_test_multi(self, client):
        with MySqlContext(client):
            sql = [
                "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
                "TRUNCATE TABLE test_airflow",
                "INSERT INTO test_airflow VALUES ('X')",
            ]
            op = SQLExecuteQueryOperator(
                task_id="mysql_operator_test_multi", sql=sql, dag=self.dag, conn_id=MYSQL_DEFAULT
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.parametrize("client", ["mysqlclient", "mysql-connector-python"])
    def test_overwrite_schema(self, client):
        """
        Verifies option to overwrite connection schema
        """
        with MySqlContext(client):
            sql = "SELECT 1;"
            op = SQLExecuteQueryOperator(
                task_id="test_mysql_operator_test_schema_overwrite",
                sql=sql,
                dag=self.dag,
                database="foobar",
                conn_id=MYSQL_DEFAULT,
            )

            from MySQLdb import OperationalError

            with pytest.raises(OperationalError, match="Unknown database 'foobar'"):
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_mysql_operator_resolve_parameters_template_json_file(self, tmp_path):
        path = tmp_path / "testfile.json"
        path.write_text('{\n "foo": "{{ ds }}"}')

        with DAG(
            dag_id="test-dag",
            schedule=None,
            start_date=DEFAULT_DATE,
            template_searchpath=os.fspath(path.parent),
        ):
            task = SQLExecuteQueryOperator(
                task_id="op1", parameters=path.name, sql="SELECT 1", conn_id=MYSQL_DEFAULT
            )

        task.resolve_template_files()

        assert isinstance(task.parameters, dict)
        assert task.parameters["foo"] == "{{ ds }}"

    @pytest.mark.parametrize("client", ["mysqlclient", "mysql-connector-python"])
    def test_mysql_operator_openlineage(self, client):
        with MySqlContext(client):
            sql = """
            CREATE TABLE IF NOT EXISTS test_airflow (
                dummy VARCHAR(50)
            );
            """
            op = SQLExecuteQueryOperator(task_id="basic_mysql", sql=sql, dag=self.dag, conn_id=MYSQL_DEFAULT)

            lineage = op.get_openlineage_facets_on_start()
            assert len(lineage.inputs) == 0
            assert len(lineage.outputs) == 0
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

            # OpenLineage provider runs same method on complete by default
            lineage_on_complete = op.get_openlineage_facets_on_start()
            assert len(lineage_on_complete.inputs) == 0
            assert len(lineage_on_complete.outputs) == 1


@pytest.mark.parametrize("connection_port", [None, 1234])
def test_execute_openlineage_events(connection_port):
    class MySqlHookForTests(MySqlHook):
        conn_name_attr = "sql_default"
        get_conn = MagicMock(name="conn")
        get_connection = MagicMock()

    dbapi_hook = MySqlHookForTests()

    sql = """CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
        order_day_of_week VARCHAR(64) NOT NULL,
        order_placed_on   TIMESTAMP NOT NULL,
        orders_placed     INTEGER NOT NULL
    );
FORGOT TO COMMENT"""
    op = SQLExecuteQueryOperator(task_id="mysql-operator", sql=sql)
    op._hook = dbapi_hook
    DB_SCHEMA_NAME = "PUBLIC"
    rows = [
        (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_day_of_week", 1, "varchar"),
        (DB_SCHEMA_NAME, "popular_orders_day_of_week", "order_placed_on", 2, "timestamp"),
        (DB_SCHEMA_NAME, "popular_orders_day_of_week", "orders_placed", 3, "int4"),
    ]
    dbapi_hook.get_connection.return_value = Connection(
        conn_id="mysql_default", conn_type="mysql", host="host", port=connection_port
    )
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    lineage = op.get_openlineage_facets_on_start()
    assert len(lineage.inputs) == 0
    assert lineage.outputs == [
        Dataset(
            namespace=f"mysql://host:{connection_port or 3306}",
            name="PUBLIC.popular_orders_day_of_week",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="order_day_of_week", type="varchar"),
                        SchemaDatasetFacetFields(name="order_placed_on", type="timestamp"),
                        SchemaDatasetFacetFields(name="orders_placed", type="int4"),
                    ]
                )
            },
        )
    ]

    assert lineage.job_facets == {"sql": SQLJobFacet(query=sql)}

    assert lineage.run_facets["extractionError"].failedTasks == 1
