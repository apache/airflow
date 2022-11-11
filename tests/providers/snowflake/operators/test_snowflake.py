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
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeCheckOperator,
    SnowflakeIntervalCheckOperator,
    SnowflakeOperator,
    SnowflakeValueCheckOperator,
)
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"


class TestSnowflakeOperator(unittest.TestCase):
    def setUp(self):
        super().setUp()
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    @mock.patch("airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator.get_db_hook")
    def test_snowflake_operator(self, mock_get_db_hook):
        sql = """
        CREATE TABLE IF NOT EXISTS test_airflow (
            dummy VARCHAR(50)
        );
        """
        operator = SnowflakeOperator(task_id="basic_snowflake", sql=sql, dag=self.dag, do_xcom_push=False)
        # do_xcom_push=False because otherwise the XCom test will fail due to the mocking (it actually works)
        operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)


@pytest.mark.parametrize(
    "operator_class, kwargs",
    [
        (SnowflakeCheckOperator, dict(sql="Select * from test_table")),
        (SnowflakeValueCheckOperator, dict(sql="Select * from test_table", pass_value=95)),
        (SnowflakeIntervalCheckOperator, dict(table="test-table-id", metrics_thresholds={"COUNT(*)": 1.5})),
    ],
)
class TestSnowflakeCheckOperators:
    @mock.patch("airflow.providers.common.sql.operators.sql.BaseSQLOperator.get_db_hook")
    def test_get_db_hook(
        self,
        mock_get_db_hook,
        operator_class,
        kwargs,
    ):
        operator = operator_class(task_id="snowflake_check", snowflake_conn_id="snowflake_default", **kwargs)
        operator.get_db_hook()
        mock_get_db_hook.assert_called_once()
