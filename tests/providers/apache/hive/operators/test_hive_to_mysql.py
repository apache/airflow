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
from unittest.mock import PropertyMock, patch

from airflow.providers.apache.hive.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.utils.operator_helpers import context_to_airflow_vars


class TestHiveToMySqlTransfer(unittest.TestCase):

    def setUp(self):
        self.kwargs = dict(
            sql='sql',
            mysql_table='table',
            hiveserver2_conn_id='hiveserver2_default',
            mysql_conn_id='mysql_default',
            task_id='test_hive_to_mysql',
            dag=None
        )

    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.HiveServer2Hook')
    def test_execute(self, mock_hive_hook, mock_mysql_hook):
        HiveToMySqlTransfer(**self.kwargs).execute(context={})

        mock_hive_hook.assert_called_once_with(hiveserver2_conn_id=self.kwargs['hiveserver2_conn_id'])
        mock_hive_hook.return_value.get_records.assert_called_once_with('sql', hive_conf={})
        mock_mysql_hook.assert_called_once_with(mysql_conn_id=self.kwargs['mysql_conn_id'])
        mock_mysql_hook.return_value.insert_rows.assert_called_once_with(
            table=self.kwargs['mysql_table'],
            rows=mock_hive_hook.return_value.get_records.return_value
        )

    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.HiveServer2Hook')
    def test_execute_mysql_preoperator(self, mock_hive_hook, mock_mysql_hook):
        self.kwargs.update(dict(mysql_preoperator='preoperator'))

        HiveToMySqlTransfer(**self.kwargs).execute(context={})

        mock_mysql_hook.return_value.run.assert_called_once_with(self.kwargs['mysql_preoperator'])

    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.HiveServer2Hook')
    def test_execute_with_mysql_postoperator(self, mock_hive_hook, mock_mysql_hook):
        self.kwargs.update(dict(mysql_postoperator='postoperator'))

        HiveToMySqlTransfer(**self.kwargs).execute(context={})

        mock_mysql_hook.return_value.run.assert_called_once_with(self.kwargs['mysql_postoperator'])

    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.NamedTemporaryFile')
    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.HiveServer2Hook')
    def test_execute_bulk_load(self, mock_hive_hook, mock_tmp_file, mock_mysql_hook):
        type(mock_tmp_file).name = PropertyMock(return_value='tmp_file')
        context = {}
        self.kwargs.update(dict(bulk_load=True))

        HiveToMySqlTransfer(**self.kwargs).execute(context=context)

        mock_tmp_file.assert_called_once_with()
        mock_hive_hook.return_value.to_csv.assert_called_once_with(
            self.kwargs['sql'],
            mock_tmp_file.return_value.name,
            delimiter='\t',
            lineterminator='\n',
            output_header=False,
            hive_conf=context_to_airflow_vars(context)
        )
        mock_mysql_hook.return_value.bulk_load.assert_called_once_with(
            table=self.kwargs['mysql_table'],
            tmp_file=mock_tmp_file.return_value.name
        )
        mock_tmp_file.return_value.close.assert_called_once_with()

    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.MySqlHook')
    @patch('airflow.providers.apache.hive.operators.hive_to_mysql.HiveServer2Hook')
    def test_execute_with_hive_conf(self, mock_hive_hook, mock_mysql_hook):
        context = {}
        self.kwargs.update(dict(hive_conf={'mapreduce.job.queuename': 'fake_queue'}))

        HiveToMySqlTransfer(**self.kwargs).execute(context=context)

        hive_conf = context_to_airflow_vars(context)
        hive_conf.update(self.kwargs['hive_conf'])
        mock_hive_hook.return_value.get_records.assert_called_once_with(
            self.kwargs['sql'],
            hive_conf=hive_conf
        )
