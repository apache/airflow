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
#
import unittest
from datetime import datetime

import mock
from qds_sdk.commands import HiveCommand

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.qubole.hooks.qubole import QuboleHook
from airflow.providers.qubole.hooks.qubole_check import QuboleCheckHook
from airflow.providers.qubole.operators.qubole_check import QuboleValueCheckOperator


class TestQuboleValueCheckOperator(unittest.TestCase):

    def setUp(self):
        self.task_id = 'test_task'
        self.conn_id = 'default_conn'

    def __construct_operator(self, query, pass_value, tolerance=None,
                             results_parser_callable=None):

        dag = DAG('test_dag', start_date=datetime(2017, 1, 1))

        return QuboleValueCheckOperator(
            dag=dag,
            task_id=self.task_id,
            conn_id=self.conn_id,
            query=query,
            pass_value=pass_value,
            results_parser_callable=results_parser_callable,
            command_type='hivecmd',
            tolerance=tolerance)

    def test_pass_value_template(self):
        pass_value_str = "2018-03-22"
        operator = self.__construct_operator('select date from tab1;', "{{ ds }}")
        result = operator.render_template(operator.pass_value, {'ds': pass_value_str})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(result, pass_value_str)

    @mock.patch.object(QuboleValueCheckOperator, 'get_hook')
    def test_execute_pass(self, mock_get_hook):

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [10]
        mock_get_hook.return_value = mock_hook

        query = 'select value from tab1 limit 1;'

        operator = self.__construct_operator(query, 5, 1)

        operator.execute(None)

        mock_hook.get_first.assert_called_once_with(query)

    @mock.patch.object(QuboleValueCheckOperator, 'get_hook')
    def test_execute_assertion_fail(self, mock_get_hook):

        mock_cmd = mock.Mock()
        mock_cmd.status = 'done'
        mock_cmd.id = 123
        mock_cmd.is_success = mock.Mock(
            return_value=HiveCommand.is_success(mock_cmd.status))

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_hook.cmd = mock_cmd
        mock_get_hook.return_value = mock_hook

        operator = self.__construct_operator('select value from tab1 limit 1;', 5, 1)

        with self.assertRaisesRegex(AirflowException,
                                    'Qubole Command Id: ' + str(mock_cmd.id)):
            operator.execute()

        mock_cmd.is_success.assert_called_once_with(mock_cmd.status)

    @mock.patch.object(QuboleValueCheckOperator, 'get_hook')
    def test_execute_assert_query_fail(self, mock_get_hook):

        mock_cmd = mock.Mock()
        mock_cmd.status = 'error'
        mock_cmd.id = 123
        mock_cmd.is_success = mock.Mock(
            return_value=HiveCommand.is_success(mock_cmd.status))

        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_hook.cmd = mock_cmd
        mock_get_hook.return_value = mock_hook

        operator = self.__construct_operator('select value from tab1 limit 1;', 5, 1)

        with self.assertRaises(AirflowException) as cm:
            operator.execute()

        self.assertNotIn('Qubole Command Id: ', str(cm.exception))
        mock_cmd.is_success.assert_called_once_with(mock_cmd.status)

    @mock.patch.object(QuboleCheckHook, 'get_query_results')
    @mock.patch.object(QuboleHook, 'execute')
    def test_results_parser_callable(self, mock_execute, mock_get_query_results):

        mock_execute.return_value = None

        pass_value = 'pass_value'
        mock_get_query_results.return_value = pass_value

        results_parser_callable = mock.Mock()
        results_parser_callable.return_value = [pass_value]

        operator = self.__construct_operator('select value from tab1 limit 1;',
                                             pass_value, None, results_parser_callable)
        operator.execute()
        results_parser_callable.assert_called_once_with([pass_value])
