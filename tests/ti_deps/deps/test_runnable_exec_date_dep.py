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
from unittest.mock import Mock

from freezegun import freeze_time

from airflow.models import TaskInstance
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.utils.timezone import datetime


class TestRunnableExecDateDep(unittest.TestCase):

    def _get_task_instance(self, execution_date, dag_end_date=None, task_end_date=None):
        dag = Mock(end_date=dag_end_date)
        task = Mock(dag=dag, end_date=task_end_date, pool_capacity=1)
        return TaskInstance(task=task, execution_date=execution_date)

    @freeze_time('2016-01-01')
    def test_exec_date_after_end_date(self):
        """
        If the dag's execution date is in the future this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 3),
            task_end_date=datetime(2016, 1, 3),
            execution_date=datetime(2016, 1, 2),
        )
        self.assertFalse(RunnableExecDateDep().is_met(ti=ti))

    def test_exec_date_after_task_end_date(self):
        """
        If the task instance execution date is after the tasks's end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 3),
            task_end_date=datetime(2016, 1, 1),
            execution_date=datetime(2016, 1, 2),
        )
        self.assertFalse(RunnableExecDateDep().is_met(ti=ti))

    def test_exec_date_after_dag_end_date(self):
        """
        If the task instance execution date is after the dag's end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 1),
            task_end_date=datetime(2016, 1, 3),
            execution_date=datetime(2016, 1, 2),
        )
        self.assertFalse(RunnableExecDateDep().is_met(ti=ti))

    def test_all_deps_met(self):
        """
        Test to make sure all of the conditions for the dep are met
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 2),
            task_end_date=datetime(2016, 1, 2),
            execution_date=datetime(2016, 1, 1),
        )
        self.assertTrue(RunnableExecDateDep().is_met(ti=ti))
