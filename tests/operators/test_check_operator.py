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

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.operators.check_operator import ValueCheckOperator, CheckOperator, IntervalCheckOperator
from tests.compat import mock


class TestCheckOperator(unittest.TestCase):

    @mock.patch.object(CheckOperator, 'get_db_hook')
    def test_execute_no_records(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = []

        with self.assertRaises(AirflowException):
            CheckOperator(sql='sql').execute()

    @mock.patch.object(CheckOperator, 'get_db_hook')
    def test_execute_not_all_records_are_true(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = ["data", ""]

        with self.assertRaises(AirflowException):
            CheckOperator(sql='sql').execute()


class TestValueCheckOperator(unittest.TestCase):

    def setUp(self):
        self.task_id = 'test_task'
        self.conn_id = 'default_conn'

    def _construct_operator(self, sql, pass_value, tolerance=None):
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
        operator = self._construct_operator('select date from tab1;', "{{ ds }}")

        result = operator.render_template('pass_value', operator.pass_value, {'ds': pass_value_str})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(result, pass_value_str)

    def test_pass_value_template_string_float(self):
        pass_value_float = 4.0
        operator = self._construct_operator('select date from tab1;', pass_value_float)

        result = operator.render_template('pass_value', operator.pass_value, {})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(result, str(pass_value_float))

    @mock.patch.object(ValueCheckOperator, 'get_db_hook')
    def test_execute_pass(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [10]
        mock_get_db_hook.return_value = mock_hook
        sql = 'select value from tab1 limit 1;'
        operator = self._construct_operator(sql, 5, 1)

        operator.execute(None)

        mock_hook.get_first.assert_called_with(sql)

    @mock.patch.object(ValueCheckOperator, 'get_db_hook')
    def test_execute_fail(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator('select value from tab1 limit 1;', 5, 1)

        with self.assertRaisesRegex(AirflowException, 'Tolerance:100.0%'):
            operator.execute()


class IntervalCheckOperatorTest(unittest.TestCase):

    def _construct_operator(self, table, metric_thresholds,
                            ratio_formula, ignore_zero):
        return IntervalCheckOperator(
            task_id='test_task',
            table=table,
            metrics_thresholds=metric_thresholds,
            ratio_formula=ratio_formula,
            ignore_zero=ignore_zero,
        )

    def test_invalid_ratio_formula(self):
        with self.assertRaisesRegex(AirflowException, 'Invalid diff_method'):
            self._construct_operator(
                table='test_table',
                metric_thresholds={
                    'f1': 1,
                },
                ratio_formula='abs',
                ignore_zero=False,
            )

    @mock.patch.object(IntervalCheckOperator, 'get_db_hook')
    def test_execute_not_ignore_zero(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [0]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table='test_table',
            metric_thresholds={
                'f1': 1,
            },
            ratio_formula='max_over_min',
            ignore_zero=False,
        )

        with self.assertRaises(AirflowException):
            operator.execute()

    @mock.patch.object(IntervalCheckOperator, 'get_db_hook')
    def test_execute_ignore_zero(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [0]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table='test_table',
            metric_thresholds={
                'f1': 1,
            },
            ratio_formula='max_over_min',
            ignore_zero=True,
        )

        operator.execute()

    @mock.patch.object(IntervalCheckOperator, 'get_db_hook')
    def test_execute_min_max(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [2, 2, 2, 2],  # reference
                [1, 1, 1, 1],  # current
            ]

            yield from rows

        mock_hook.get_first.side_effect = returned_row()
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table='test_table',
            metric_thresholds={
                'f0': 1.0,
                'f1': 1.5,
                'f2': 2.0,
                'f3': 2.5,
            },
            ratio_formula='max_over_min',
            ignore_zero=True,
        )

        with self.assertRaisesRegex(AirflowException, "f0, f1, f2"):
            operator.execute()

    @mock.patch.object(IntervalCheckOperator, 'get_db_hook')
    def test_execute_diff(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [3, 3, 3, 3],  # reference
                [1, 1, 1, 1],  # current
            ]

            yield from rows

        mock_hook.get_first.side_effect = returned_row()
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table='test_table',
            metric_thresholds={
                'f0': 0.5,
                'f1': 0.6,
                'f2': 0.7,
                'f3': 0.8,
            },
            ratio_formula='relative_diff',
            ignore_zero=True,
        )

        with self.assertRaisesRegex(AirflowException, "f0, f1"):
            operator.execute()
