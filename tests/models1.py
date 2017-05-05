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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import os
import unittest
import time

from airflow import models, AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.models import TaskInstance as TI
from airflow.models import State as ST
from airflow.operators import DummyOperator, BashOperator, PythonOperator
from airflow.utils.state import State
from airflow.utils.tests import get_dag
from nose_parameterized import parameterized

DEFAULT_DATE = datetime.datetime(2016, 1, 1)
TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')


class BaseOperatorTest(unittest.TestCase):

    def setup_example(self, content, expected):
        start_date = datetime.datetime(2016, 2, 1, 0, 0, 0)
        dag = models.DAG('test-dag', start_date=start_date)
        task = DummyOperator(task_id='downstream',
                             dag=dag, owner='airflow')
        run_date = task.start_date + datetime.timedelta(days=5)
        ti = TI(task, run_date)

        jinja_context = ti.get_template_context()
        results = task.render_template(content, jinja_context)
        self.assertEqual(results, expected)

    def test_check_task_dependencies1(self):
        self.setup_example('{{ ds }}', '2016-02-06')

    def test_check_task_dependencies2(self):
        template = { 'foo': 'bar' }
        expected = { 'foo': 'bar' }
        self.setup_example(template, expected)

    def test_check_task_dependencies3(self):
        template = { 'foo': '{{ ds }}' }
        expected = { 'foo': '2016-02-06' }
        self.setup_example(template, expected)


    def test_check_task_dependencies4(self):
        template = { 'foo': False }
        expected = { 'foo': False }
        self.setup_example(template, expected)

    def test_check_task_dependencies5(self):
        template = { 'foo': '{{ False }}' }
        expected = { 'foo': False }
        self.setup_example(template, expected)
