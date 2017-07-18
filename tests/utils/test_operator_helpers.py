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

from datetime import datetime
import mock
import unittest

from airflow.utils import operator_helpers


class TestOperatorHelpers(unittest.TestCase):

    def setUp(self):
        super(TestOperatorHelpers, self).setUp()
        self.dag_id = 'dag_id'
        self.task_id = 'task_id'
        self.execution_date = '2017-05-21T00:00:00'
        self.context = {
            'dag': mock.MagicMock(name='dag', dag_id=self.dag_id),
            'dag_run': mock.MagicMock(
                name='dag_run',
                execution_date=datetime.strptime(self.execution_date,
                                                 '%Y-%m-%dT%H:%M:%S'),
            ),
            'task': mock.MagicMock(name='task', task_id=self.task_id),
            'task_instance': mock.MagicMock(
                name='task_instance',
                execution_date=datetime.strptime(self.execution_date,
                                                 '%Y-%m-%dT%H:%M:%S'),
            ),
        }

    def test_context_to_airflow_vars_empty_context(self):
        self.assertDictEqual(operator_helpers.context_to_airflow_vars({}), {})

    def test_context_to_airflow_vars_all_context(self):
        self.assertDictEqual(
            operator_helpers.context_to_airflow_vars(self.context),
            {
                'airflow.ctx.dag.dag_id': self.dag_id,
                'airflow.ctx.dag_run.execution_date': self.execution_date,
                'airflow.ctx.task.task_id': self.task_id,
                'airflow.ctx.task_instance.execution_date': self.execution_date,
            }
        )


if __name__ == '__main__':
    unittest.main()
