# -*- coding: utf-8 -*-
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
from airflow.models import DAG
from airflow.exceptions import AirflowException
from airflow.operators.check_operator import ValueCheckOperator
from tests.compat import mock


class ValueCheckOperatorTest(unittest.TestCase):

    def setUp(self):
        self.task_id = 'test_task'
        self.conn_id = 'default_conn'

    def __construct_operator(self, sql, pass_value, tolerance=None):

        dag = DAG('test_dag', start_date=datetime(2017, 1, 1))

        return ValueCheckOperator(
            dag=dag,
            task_id=self.task_id,
            conn_id=self.conn_id,
            sql=sql,
            pass_value=pass_value,
            tolerance=tolerance)

    def test_pass_value_template_string(self):
        pass_value_str = "2018-03-22"
        operator = self.__construct_operator('select date from tab1;', "{{ ds }}")
        result = operator.render_template('pass_value', operator.pass_value,
                                          {'ds': pass_value_str})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(result, pass_value_str)

    def test_pass_value_template_string_float(self):
        pass_value_float = 4.0
        operator = self.__construct_operator('select date from tab1;', pass_value_float)
        result = operator.render_template('pass_value', operator.pass_value, {})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(result, str(pass_value_float))

    @mock.patch.object(ValueCheckOperator, 'get_db_hook')
    def test_execute_pass(self, mock_get_db_hook):

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [10]
        mock_get_db_hook.return_value = mock_hook

        sql = 'select value from tab1 limit 1;'

        operator = self.__construct_operator(sql, 5, 1)

        operator.execute(None)

        mock_hook.get_first.assert_called_with(sql)

    @mock.patch.object(ValueCheckOperator, 'get_db_hook')
    def test_execute_fail(self, mock_get_db_hook):

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_get_db_hook.return_value = mock_hook

        operator = self.__construct_operator('select value from tab1 limit 1;', 5, 1)

        with self.assertRaisesRegexp(AirflowException, 'Tolerance:100.0%'):
            operator.execute()
