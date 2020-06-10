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
from datetime import datetime

import mock

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.apache.druid.operators.druid_check import DruidCheckOperator


class TestDruidCheckOperator(unittest.TestCase):

    def setUp(self):
        self.task_id = 'test_task'
        self.druid_broker_conn_id = 'default_conn'

    def __construct_operator(self, sql):

        dag = DAG('test_dag', start_date=datetime(2017, 1, 1))

        return DruidCheckOperator(
            dag=dag,
            task_id=self.task_id,
            druid_broker_conn_id=self.druid_broker_conn_id,
            sql=sql)

    @mock.patch.object(DruidCheckOperator, 'get_first')
    def test_execute_pass(self, mock_get_first):
        mock_get_first.return_value = [10]

        sql = 'select count(*) from tab1 limit 1;'

        operator = self.__construct_operator(sql)

        try:
            operator.execute(None)
        except AirflowException:
            self.fail('Exception should not thrown!')

    @mock.patch.object(DruidCheckOperator, 'get_first')
    def test_execute_fail(self, mock_get_first):
        mock_get_first.return_value = []
        sql = 'select count(*) from tab1 limit 1;'

        operator = self.__construct_operator(sql)

        with self.assertRaises(AirflowException):
            operator.execute()
