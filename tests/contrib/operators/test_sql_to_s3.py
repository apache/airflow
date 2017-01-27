# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
import datetime

from airflow import DAG
from airflow.contrib.operators.sql_to_s3 import SqlToS3
from airflow import configuration
from airflow import models
from airflow.utils import db

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TEST_DAG_ID = 'unit_test_dag'
DEFAULT_DATE = datetime.datetime(2015, 1, 1)


class TestSqlToS3Operator(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
        models.Connection(
            conn_id='s3_default',
            conn_type='s3',
            extra='{"aws_access_key_id":"default_access_key", "aws_secret_access_key": "default_secret_key"}')
        )
        args={
            'owner': 'airflow',
            'mysql_conn_id': 'airflow_db',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag
        self.sql_to_s3 = SqlToS3(
            task_id='task_id',
            db_conn_id='mysql_default',
            sql='select 1',
            s3_bucket='test-bucket-',
            s3_file_key='test-file-key',
            s3_conn_id='s3_default',
            s3_replace_file=True,
            s3_zip_file=False,
            dag=self.dag
        )

    def test_init(self):
        self.assertEqual(self.sql_to_s3.task_id, 'task_id')
        self.assertEqual(self.sql_to_s3.db_conn_id,'mysql_default')
        self.assertEqual(self.sql_to_s3.sql,'select 1')
        self.assertEqual(self.sql_to_s3.s3_bucket,'test-bucket-')
        self.assertEqual(self.sql_to_s3.s3_file_key,'test-file-key')
        self.assertEqual(self.sql_to_s3.s3_conn_id,'s3_default')
        self.assertEqual(self.sql_to_s3.s3_replace_file,True)

    @mock.patch('airflow.hooks.S3_hook.S3Hook')
    @mock.patch('airflow.hooks.base_hook.BaseHook')
    def test_exec(self, base_hook_mock, s3_hook_mock):

        self.sql_to_s3.execute(None)
        base_hook_mock.return_value.get_hook.assert_called_once_with(self.sql_to_s3.db_conn_id)
        s3_hook_mock.assert_called_once_with(s3_conn_id=self.sql_to_s3.s3_conn_id)

        s3_hook_mock.return_value.load_file.assert_called_once_with()
        s3_hook_mock.return_value.connection.return_value.close.assert_called_once_with()

if __name__ == '__main__':
    unittest.main()
