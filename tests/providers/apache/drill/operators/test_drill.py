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
from airflow.providers.apache.drill.operators.drill import DrillOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"


@pytest.mark.backend("drill")
class TestDrillOperator(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def tearDown(self):
        tables_to_drop = ["dfs.tmp.test_airflow"]
        from airflow.providers.apache.drill.hooks.drill import DrillHook

        with DrillHook().get_conn() as conn:
            with conn.cursor() as cur:
                for table in tables_to_drop:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    def test_drill_operator_single(self):
        sql = """
        create table dfs.tmp.test_airflow as
        select * from cp.`employee.json` limit 10
        """
        op = DrillOperator(task_id="drill_operator_test_single", sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_drill_operator_multi(self):
        sql = [
            "create table dfs.tmp.test_airflow as select * from cp.`employee.json` limit 10",
            "select sum(employee_id), any_value(full_name) from dfs.tmp.test_airflow",
        ]
        op = DrillOperator(task_id="drill_operator_test_multi", sql=sql, dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
