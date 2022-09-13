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
import unittest
from unittest.mock import patch

from airflow.providers.mysql.transfers.trino_to_mysql import TrinoToMySqlOperator
from tests.providers.apache.hive import DEFAULT_DATE, TestHiveEnvironment


class TestTrinoToMySqlTransfer(TestHiveEnvironment):
    def setUp(self):
        self.kwargs = dict(
            sql='sql',
            mysql_table='mysql_table',
            task_id='test_trino_to_mysql_transfer',
        )
        super().setUp()

    @patch('airflow.providers.mysql.transfers.trino_to_mysql.MySqlHook')
    @patch('airflow.providers.mysql.transfers.trino_to_mysql.TrinoHook')
    def test_execute(self, mock_trino_hook, mock_mysql_hook):
        TrinoToMySqlOperator(**self.kwargs).execute(context={})

        mock_trino_hook.return_value.get_records.assert_called_once_with(self.kwargs['sql'])
        mock_mysql_hook.return_value.insert_rows.assert_called_once_with(
            table=self.kwargs['mysql_table'], rows=mock_trino_hook.return_value.get_records.return_value
        )

    @patch('airflow.providers.mysql.transfers.trino_to_mysql.MySqlHook')
    @patch('airflow.providers.mysql.transfers.trino_to_mysql.TrinoHook')
    def test_execute_with_mysql_preoperator(self, mock_trino_hook, mock_mysql_hook):
        self.kwargs.update(dict(mysql_preoperator='mysql_preoperator'))

        TrinoToMySqlOperator(**self.kwargs).execute(context={})

        mock_trino_hook.return_value.get_records.assert_called_once_with(self.kwargs['sql'])
        mock_mysql_hook.return_value.run.assert_called_once_with(self.kwargs['mysql_preoperator'])
        mock_mysql_hook.return_value.insert_rows.assert_called_once_with(
            table=self.kwargs['mysql_table'], rows=mock_trino_hook.return_value.get_records.return_value
        )

    @unittest.skipIf(
        'AIRFLOW_RUNALL_TESTS' not in os.environ, "Skipped because AIRFLOW_RUNALL_TESTS is not set"
    )
    def test_trino_to_mysql(self):
        op = TrinoToMySqlOperator(
            task_id='trino_to_mysql_check',
            sql="""
                SELECT name, count(*) as ccount
                FROM airflow.static_babynames
                GROUP BY name
                """,
            mysql_table='test_static_babynames',
            mysql_preoperator='TRUNCATE TABLE test_static_babynames;',
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
