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
from mock import Mock
from mock import patch

from airflow import DAG
from airflow.contrib.operators.sql_to_s3 import SqlToS3
from airflow import models
from airflow.utils import db

TEST_DAG_ID = 'unit_test_dag'
DEFAULT_DATE = datetime.datetime(2015, 1, 1)

class TestSqlToS3Operator(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'mysql_conn_id': 'airflow_db',
            'start_date': DEFAULT_DATE
        }
        dag = DAG(TEST_DAG_ID, default_args=args)
        self.dag = dag

    def test_parameters_assignment(self):
        sql_to_s3 = SqlToS3(
            task_id='task_id',
            db_conn_id='test_db_conn',
            sql='select 1',
            s3_bucket='test-bucket-',
            s3_file_key='test-file-key',
            s3_conn_id='test-s3-conn',
            s3_replace_file=True,
            s3_zip_file=False,
            dag=self.dag
        )
        self.assertEqual(sql_to_s3.task_id, 'task_id')
        self.assertEqual(sql_to_s3.db_conn_id,'test_db_conn')
        self.assertEqual(sql_to_s3.sql,'select 1')
        self.assertEqual(sql_to_s3.s3_bucket,'s3_bucket')
        self.assertEqual(sql_to_s3.s3_conn_id,'s3_conn_id')
        self.assertEqual(sql_to_s3.s3_replace_file,True)
        self.assertEqual(sql_to_s3.s3_zip_file,False)


if __name__ == '__main__':
    unittest.main()
