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
from tempfile import NamedTemporaryFile

import pytest

from airflow.models.dag import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils import timezone
from tests.providers.mysql.hooks.test_mysql import MySqlContext

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"


@pytest.mark.backend("mysql")
class TestMySql:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
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
            op = MySqlOperator(task_id="basic_mysql", sql=sql, dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.parametrize("client", ["mysqlclient", "mysql-connector-python"])
    def test_mysql_operator_test_multi(self, client):
        with MySqlContext(client):
            sql = [
                "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
                "TRUNCATE TABLE test_airflow",
                "INSERT INTO test_airflow VALUES ('X')",
            ]
            op = MySqlOperator(
                task_id="mysql_operator_test_multi",
                sql=sql,
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.parametrize("client", ["mysqlclient", "mysql-connector-python"])
    def test_overwrite_schema(self, client):
        """
        Verifies option to overwrite connection schema
        """
        with MySqlContext(client):
            sql = "SELECT 1;"
            op = MySqlOperator(
                task_id="test_mysql_operator_test_schema_overwrite",
                sql=sql,
                dag=self.dag,
                database="foobar",
            )

            from MySQLdb import OperationalError

            try:
                op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
            except OperationalError as e:
                assert "Unknown database 'foobar'" in str(e)

    def test_mysql_operator_resolve_parameters_template_json_file(self):

        with NamedTemporaryFile(suffix=".json") as f:
            f.write(b'{\n "foo": "{{ ds }}"}')
            f.flush()
            template_dir = os.path.dirname(f.name)
            template_file = os.path.basename(f.name)

            with DAG("test-dag", start_date=DEFAULT_DATE, template_searchpath=template_dir):
                task = MySqlOperator(task_id="op1", parameters=template_file, sql="SELECT 1")

            task.resolve_template_files()

        assert isinstance(task.parameters, dict)
        assert task.parameters["foo"] == "{{ ds }}"
