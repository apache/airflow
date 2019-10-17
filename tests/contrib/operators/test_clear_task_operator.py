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

import datetime
import unittest

from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State


class TestClearTaskOperator(unittest.TestCase):
    def setUp(self):
        dag_bag = DagBag()
        dag = dag_bag.get_dag("example_clear_task_operator")

        dummy1 = dag.get_task("dummy1")
        dummy2 = dag.get_task("dummy2")
        clear_dummy1 = dag.get_task("clear_dummy1")
        clear_dummy1_downstream = dag.get_task("clear_dummy1_downstream")
        clear_dummy1_future = dag.get_task("clear_dummy1_future")

        execution_date = datetime.datetime(2019, 1, 1)
        next_execution_date = datetime.datetime(2019, 1, 2)

        self.ti1 = TaskInstance(task=dummy1, execution_date=execution_date)
        self.ti2 = TaskInstance(task=dummy2, execution_date=execution_date)
        self.ti_clear_dummy1 = TaskInstance(task=clear_dummy1, execution_date=execution_date)

        self.ti1_next = TaskInstance(task=dummy1, execution_date=next_execution_date)
        self.ti2_next = TaskInstance(task=dummy2, execution_date=next_execution_date)

        self.ti_clear_dummy1_downstream = TaskInstance(task=clear_dummy1_downstream,
                                                       execution_date=execution_date)
        self.ti_clear_dummy1_future = TaskInstance(task=clear_dummy1_future,
                                                   execution_date=execution_date)

        self.ti1.run()
        self.ti2.run()
        self.ti1_next.run()
        self.ti2_next.run()

        self.assert_ti_state_equal(self.ti1, State.SUCCESS)
        self.assert_ti_state_equal(self.ti2, State.SUCCESS)
        self.assert_ti_state_equal(self.ti1_next, State.SUCCESS)
        self.assert_ti_state_equal(self.ti2_next, State.SUCCESS)

    def assert_ti_state_equal(self, task_instance, state):
        task_instance.refresh_from_db()
        self.assertEqual(task_instance.state, state)

    def test_clear_task_operator(self):
        """
        Tests ClearTaskOperator with default arguments.
        """
        self.ti_clear_dummy1.run(ignore_all_deps=True)
        self.assert_ti_state_equal(self.ti_clear_dummy1, State.SUCCESS)

        self.assert_ti_state_equal(self.ti1, State.NONE)
        self.assert_ti_state_equal(self.ti2, State.SUCCESS)
        self.assert_ti_state_equal(self.ti1_next, State.SUCCESS)
        self.assert_ti_state_equal(self.ti2_next, State.SUCCESS)

    def test_clear_task_operator_downstream(self):
        """
        Tests ClearTaskOperator with downstream set to True.
        """
        self.ti_clear_dummy1_downstream.run(ignore_all_deps=True)
        self.assert_ti_state_equal(self.ti_clear_dummy1_downstream, State.SUCCESS)

        self.assert_ti_state_equal(self.ti1, State.NONE)
        self.assert_ti_state_equal(self.ti2, State.NONE)
        self.assert_ti_state_equal(self.ti1_next, State.SUCCESS)
        self.assert_ti_state_equal(self.ti2_next, State.SUCCESS)

    def test_clear_task_operator_future(self):
        """
        Tests ClearTaskOperator with future set to True.
        """
        self.ti_clear_dummy1_future.run(ignore_all_deps=True)
        self.assert_ti_state_equal(self.ti_clear_dummy1_future, State.SUCCESS)

        self.assert_ti_state_equal(self.ti1, State.NONE)
        self.assert_ti_state_equal(self.ti2, State.SUCCESS)
        self.assert_ti_state_equal(self.ti1_next, State.NONE)
        self.assert_ti_state_equal(self.ti2_next, State.SUCCESS)
