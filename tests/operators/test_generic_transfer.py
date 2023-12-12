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

from contextlib import closing
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

pytestmark = pytest.mark.db_test

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
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        drop_tables = {"test_mysql_to_mysql", "test_airflow"}
        with closing(MySqlHook().get_conn()) as conn:
            for table in drop_tables:
                # Previous version tried to run execute directly on dbapi call, which was accidentally working
                with closing(conn.cursor()) as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    @pytest.mark.parametrize(
        "client",
        [
            "mysqlclient",
            "mysql-connector-python",
        ],
    )
    def test_mysql_to_mysql(self, client):
        from tests.providers.mysql.hooks.test_mysql import MySqlContext

        with MySqlContext(client):
            sql = "SELECT * FROM connection;"
            op = GenericTransfer(
                task_id="test_m2m",
                preoperator=[
                    "DROP TABLE IF EXISTS test_mysql_to_mysql",
                    "CREATE TABLE IF NOT EXISTS test_mysql_to_mysql LIKE connection",
                ],
                source_conn_id="airflow_db",
                destination_conn_id="airflow_db",
                destination_table="test_mysql_to_mysql",
                sql=sql,
                dag=self.dag,
            )
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_mysql_to_mysql_replace(self, mock_insert):
        sql = "SELECT * FROM connection LIMIT 10;"
        op = GenericTransfer(
            task_id="test_m2m",
            preoperator=[
                "DROP TABLE IF EXISTS test_mysql_to_mysql",
                "CREATE TABLE IF NOT EXISTS test_mysql_to_mysql LIKE connection",
            ],
            source_conn_id="airflow_db",
            destination_conn_id="airflow_db",
            destination_table="test_mysql_to_mysql",
            sql=sql,
            dag=self.dag,
            insert_args={"replace": True},
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert mock_insert.called
        _, kwargs = mock_insert.call_args
        assert "replace" in kwargs


@pytest.mark.backend("postgres")
class TestPostgres:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def teardown_method(self):
        tables_to_drop = ["test_postgres_to_postgres", "test_airflow"]
        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    def test_postgres_to_postgres(self):
        sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES LIMIT 100;"
        op = GenericTransfer(
            task_id="test_p2p",
            preoperator=[
                "DROP TABLE IF EXISTS test_postgres_to_postgres",
                "CREATE TABLE IF NOT EXISTS test_postgres_to_postgres (LIKE INFORMATION_SCHEMA.TABLES)",
            ],
            source_conn_id="postgres_default",
            destination_conn_id="postgres_default",
            destination_table="test_postgres_to_postgres",
            sql=sql,
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @mock.patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_postgres_to_postgres_replace(self, mock_insert):
        sql = "SELECT id, conn_id, conn_type FROM connection LIMIT 10;"
        op = GenericTransfer(
            task_id="test_p2p",
            preoperator=[
                "DROP TABLE IF EXISTS test_postgres_to_postgres",
                "CREATE TABLE IF NOT EXISTS test_postgres_to_postgres (LIKE connection INCLUDING INDEXES)",
            ],
            source_conn_id="postgres_default",
            destination_conn_id="postgres_default",
            destination_table="test_postgres_to_postgres",
            sql=sql,
            dag=self.dag,
            insert_args={
                "replace": True,
                "target_fields": ("id", "conn_id", "conn_type"),
                "replace_index": "id",
            },
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        assert mock_insert.called
        _, kwargs = mock_insert.call_args
        assert "replace" in kwargs
