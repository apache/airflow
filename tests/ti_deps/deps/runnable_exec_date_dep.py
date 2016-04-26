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

import unittest
from datetime import datetime
from mock import patch

from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from fake_models import FakeDag, FakeTask, FakeTI
from tests.test_utils.fake_datetime import FakeDatetime


class RunnableExecDateDepTest(unittest.TestCase):

    @patch('airflow.ti_deps.deps.runnable_exec_date_dep.datetime', FakeDatetime)
    def test_exec_date_after_end_date(self):
        """
        If the dag's execution date is in the future this dep should fail
        """
        FakeDatetime.now = classmethod(lambda cls: datetime(2016, 1, 1))
        dag = FakeDag(end_date=datetime(2016, 1, 3))
        task = FakeTask(dag=dag, end_date=datetime(2016, 1, 3))
        ti = FakeTI(task=task, execution_date=datetime(2016, 1, 2))

        self.assertFalse(RunnableExecDateDep().is_met(ti=ti, dep_context=None))

    def test_exec_date_before_task_end_date(self):
        """
        If the task instance execution date is before the DAG's end date this dep should
        fail
        """
        FakeDatetime.now = classmethod(lambda cls: datetime(2016, 1, 3))
        dag = FakeDag(end_date=datetime(2016, 1, 1))
        task = FakeTask(dag=dag, end_date=datetime(2016, 1, 2))
        ti = FakeTI(task=task, execution_date=datetime(2016, 1, 1))

        self.assertFalse(RunnableExecDateDep().is_met(ti=ti, dep_context=None))

    def test_exec_date_after_task_end_date(self):
        """
        If the task instance execution date is after the DAG's end date this dep should
        fail
        """
        FakeDatetime.now = classmethod(lambda cls: datetime(2016, 1, 3))
        dag = FakeDag(end_date=datetime(2016, 1, 3))
        task = FakeTask(dag=dag, end_date=datetime(2016, 1, 1))
        ti = FakeTI(task=task, execution_date=datetime(2016, 1, 2))

        self.assertFalse(RunnableExecDateDep().is_met(ti=ti, dep_context=None))

    def test_exec_date_before_dag_end_date(self):
        """
        If the task instance execution date is before the dag's end date this dep should
        fail
        """
        dag = FakeDag(start_date=datetime(2016, 1, 2))
        task = FakeTask(dag=dag, start_date=datetime(2016, 1, 1))
        ti = FakeTI(task=task, execution_date=datetime(2016, 1, 1))

        self.assertFalse(RunnableExecDateDep().is_met(ti=ti, dep_context=None))

    def test_exec_date_after_dag_end_date(self):
        """
        If the task instance execution date is after the dag's end date this dep should
        fail
        """
        dag = FakeDag(end_date=datetime(2016, 1, 1))
        task = FakeTask(dag=dag, end_date=datetime(2016, 1, 3))
        ti = FakeTI(task=task, execution_date=datetime(2016, 1, 2))

        self.assertFalse(RunnableExecDateDep().is_met(ti=ti, dep_context=None))

    def test_all_deps_met(self):
        """
        Test to make sure all of the conditions for the dep are met
        """
        dag = FakeDag(end_date=datetime(2016, 1, 2))
        task = FakeTask(dag=dag, end_date=datetime(2016, 1, 2))
        ti = FakeTI(task=task, execution_date=datetime(2016, 1, 1))

        self.assertTrue(RunnableExecDateDep().is_met(ti=ti, dep_context=None))
