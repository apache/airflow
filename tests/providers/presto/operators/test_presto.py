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

import pytest

from airflow.models.dag import DAG
from airflow.providers.presto.operators.presto import PrestoOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = 'unit_test_dag'


@pytest.mark.backend("presto")
class TestPresto(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def tearDown(self):
        tables_to_drop = ['test_presto_to_presto', 'test_airflow']
        from airflow.providers.presto.hooks.presto import PrestoHook
        with PrestoHook().get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    def test_presto_operator_test(self):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        op = PrestoOperator(task_id='basic_presto', sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_presto_operator_test_multi(self):
        sql = [
            "CREATE TABLE IF NOT EXISTS test_airflow (dummy VARCHAR(50))",
            "TRUNCATE TABLE test_airflow",
            "INSERT INTO test_airflow VALUES ('X')",
        ]
        op = PrestoOperator(
            task_id='presto_operator_test_multi', sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_overwrite_schema(self):
        """
        Verifies option to overwrite connection schema
        """

        sql = "SELECT 1;"
        op = PrestoOperator(
            task_id='presto_operator_test_schema_overwrite',
            sql=sql,
            dag=self.dag,
            autocommit=True,
            schema="foobar",
        )

        from prestodb import OperationalError
        try:
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                   ignore_ti_state=True)
        except OperationalError as e:
            assert 'Schema "foobar" does not exist' in str(e)

    def test_overwrite_catalog(self):
        """
        Verifies option to overwrite connection catalog
        """

        sql = "SELECT 1;"
        op = PrestoOperator(
            task_id='presto_operator_test_catalog_overwrite',
            sql=sql,
            dag=self.dag,
            autocommit=True,
            schema="foobar",
        )

        from prestodb import OperationalError
        try:
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
                   ignore_ti_state=True)
        except OperationalError as e:
            assert 'Catalog "foobar" does not exist' in str(e)
