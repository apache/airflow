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

import unittest

import pytest

from airflow.models.dag import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"


@pytest.mark.backend("sqlite")
class TestSqliteOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def tearDown(self):
        tables_to_drop = ["test_airflow", "test_airflow2"]
        from airflow.providers.sqlite.hooks.sqlite import SqliteHook

        with SqliteHook().get_conn() as conn:
            cur = conn.cursor()
            for table in tables_to_drop:
                cur.execute(f"DROP TABLE IF EXISTS {table}")

    def test_sqlite_operator_with_one_statement(self):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        op = SqliteOperator(task_id="basic_sqlite", sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_sqlite_operator_with_multiple_statements(self):
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        op = SqliteOperator(task_id="sqlite_operator_with_multiple_statements", sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_sqlite_operator_with_invalid_sql(self):
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            "INSERT INTO test_airflow2 VALUES ('X')",
        ]

        from sqlite3 import OperationalError

        try:
            op = SqliteOperator(task_id="sqlite_operator_with_multiple_statements", sql=sql, dag=self.dag)
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
            pytest.fail("An exception should have been thrown")
        except OperationalError as e:
            assert "no such table: test_airflow2" in str(e)
