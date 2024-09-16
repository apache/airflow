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

import pytest

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"
POSTGRES_DEFAULT = "postgres_default"


@pytest.mark.backend("postgres")
class TestPostgres:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, schedule=None, default_args=args)
        self.dag = dag

    def teardown_method(self):
        tables_to_drop = ["test_postgres_to_postgres", "test_airflow"]

        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    def test_postgres_operator_test(self):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        op = SQLExecuteQueryOperator(
            task_id="basic_postgres", sql=sql, dag=self.dag, conn_id=POSTGRES_DEFAULT
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        autocommit_task = SQLExecuteQueryOperator(
            task_id="basic_postgres_with_autocommit",
            sql=sql,
            dag=self.dag,
            autocommit=True,
            conn_id=POSTGRES_DEFAULT,
        )
        autocommit_task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_postgres_operator_test_multi(self):
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            "TRUNCATE TABLE test_airflow",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        op = SQLExecuteQueryOperator(
            task_id="postgres_operator_test_multi", sql=sql, dag=self.dag, conn_id=POSTGRES_DEFAULT
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_vacuum(self):
        """
        Verifies the VACUUM operation runs well with the PostgresOperator
        """

        sql = "VACUUM ANALYZE;"
        op = SQLExecuteQueryOperator(
            task_id="postgres_operator_test_vacuum",
            sql=sql,
            dag=self.dag,
            autocommit=True,
            conn_id=POSTGRES_DEFAULT,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_overwrite_database(self):
        """
        Verifies option to overwrite connection database
        """

        sql = "SELECT 1;"
        op = SQLExecuteQueryOperator(
            task_id="postgres_operator_test_database_overwrite",
            sql=sql,
            dag=self.dag,
            autocommit=True,
            database="foobar",
            conn_id=POSTGRES_DEFAULT,
        )

        from psycopg2 import OperationalError

        with pytest.raises(OperationalError, match='database "foobar" does not exist'):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_runtime_parameter_setting(self):
        """
        Verifies ability to pass server configuration parameters to
        PostgresOperator
        """

        sql = "SELECT 1;"
        op = SQLExecuteQueryOperator(
            task_id="postgres_operator_test_runtime_parameter_setting",
            sql=sql,
            dag=self.dag,
            hook_params={"options": "-c statement_timeout=3000ms"},
            conn_id=POSTGRES_DEFAULT,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert op.get_db_hook().get_first("SHOW statement_timeout;")[0] == "3s"


@pytest.mark.backend("postgres")
class TestPostgresOpenLineage:
    custom_schemas = ["another_schema"]

    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, schedule=None, default_args=args)
        self.dag = dag

        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                for schema in self.custom_schemas:
                    cur.execute(f"CREATE SCHEMA {schema}")

    def teardown_method(self):
        tables_to_drop = ["test_postgres_to_postgres", "test_airflow"]

        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")
                for schema in self.custom_schemas:
                    cur.execute(f"DROP SCHEMA {schema} CASCADE")

    def test_postgres_operator_openlineage_implicit_schema(self):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        op = SQLExecuteQueryOperator(
            task_id="basic_postgres",
            sql=sql,
            dag=self.dag,
            hook_params={"options": "-c search_path=another_schema"},
            conn_id=POSTGRES_DEFAULT,
        )

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # OpenLineage provider runs same method on complete by default
        lineage_on_complete = op.get_openlineage_facets_on_start()
        assert len(lineage_on_complete.inputs) == 0
        assert len(lineage_on_complete.outputs) == 1
        assert lineage_on_complete.outputs[0].namespace == "postgres://postgres:5432"
        assert lineage_on_complete.outputs[0].name == "airflow.another_schema.test_airflow"
        assert "schema" in lineage_on_complete.outputs[0].facets

    def test_postgres_operator_openlineage_explicit_schema(self):
        sql = """
        CREATE TABLE IF NOT EXISTS public.test_airflow (
            dummy VARCHAR(50)
        );
        """
        op = SQLExecuteQueryOperator(
            task_id="basic_postgres",
            sql=sql,
            dag=self.dag,
            hook_params={"options": "-c search_path=another_schema"},
            conn_id=POSTGRES_DEFAULT,
        )

        lineage = op.get_openlineage_facets_on_start()
        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        # OpenLineage provider runs same method on complete by default
        lineage_on_complete = op.get_openlineage_facets_on_start()
        assert len(lineage_on_complete.inputs) == 0
        assert len(lineage_on_complete.outputs) == 1
        assert lineage_on_complete.outputs[0].namespace == "postgres://postgres:5432"
        assert lineage_on_complete.outputs[0].name == "airflow.public.test_airflow"
        assert "schema" in lineage_on_complete.outputs[0].facets


@pytest.mark.db_test
def test_parameters_are_templatized(create_task_instance_of_operator):
    """Test that PostgreSQL operator could template the same fields as SQLExecuteQueryOperator"""
    ti = create_task_instance_of_operator(
        SQLExecuteQueryOperator,
        conn_id="{{ param.conn_id }}",
        sql="SELECT * FROM {{ param.table }} WHERE spam = %(spam)s;",
        parameters={"spam": "{{ param.bar }}"},
        dag_id="test-postgres-op-parameters-are-templatized",
        task_id="test-task",
    )
    task: SQLExecuteQueryOperator = ti.render_templates(
        {
            "param": {"conn_id": "pg", "table": "foo", "bar": "egg"},
            "ti": ti,
        }
    )
    assert task.conn_id == "pg"
    assert task.sql == "SELECT * FROM foo WHERE spam = %(spam)s;"
    assert task.parameters == {"spam": "egg"}
