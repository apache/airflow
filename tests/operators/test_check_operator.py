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
from airflow.exceptions import AirflowException

from airflow.operators.check_operator import IntervalCheckOperator
from mock import mock


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
        with self.assertRaisesRegexp(AirflowException, 'Invalid diff_method'):
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

            for r in rows:
                yield r

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

        with self.assertRaisesRegexp(AirflowException, "f0, f1, f2"):
            operator.execute()

    @mock.patch.object(IntervalCheckOperator, 'get_db_hook')
    def test_execute_diff(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [3, 3, 3, 3],  # reference
                [1, 1, 1, 1],  # current
            ]

            for r in rows:
                yield r

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

        with self.assertRaisesRegexp(AirflowException, "f0, f1"):
            operator.execute()
