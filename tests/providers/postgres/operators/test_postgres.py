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

import unittest
from unittest import mock

import pytest

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


@pytest.mark.backend("postgres")
class TestPostgres(unittest.TestCase):
    def tearDown(self):
        tables_to_drop = ['test_postgres_to_postgres', 'test_airflow']
        from airflow.providers.postgres.hooks.postgres import PostgresHook

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
        op = PostgresOperator(task_id='basic_postgres', sql=sql)
        op.execute(mock.MagicMock())

        autocommit_task = PostgresOperator(task_id='basic_postgres_with_autocommit', sql=sql, autocommit=True)
        autocommit_task.execute(mock.MagicMock())

    def test_postgres_operator_test_multi(self):
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            "TRUNCATE TABLE test_airflow",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        op = PostgresOperator(task_id='postgres_operator_test_multi', sql=sql)
        op.execute(mock.MagicMock())

    def test_vacuum(self):
        """
        Verifies the VACUUM operation runs well with the PostgresOperator
        """

        sql = "VACUUM ANALYZE;"
        op = PostgresOperator(task_id='postgres_operator_test_vacuum', sql=sql, autocommit=True)
        op.execute(mock.MagicMock())

    def test_overwrite_schema(self):
        """
        Verifies option to overwrite connection schema
        """

        sql = "SELECT 1;"
        op = PostgresOperator(
            task_id='postgres_operator_test_schema_overwrite',
            sql=sql,
            dag=self.dag,
            autocommit=True,
            database="foobar",
        )

        from psycopg2 import OperationalError

        with pytest.raises(OperationalError, match=r'.+database "foobar" does not exist.+'):
            op.execute(mock.MagicMock)
