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

import pytest
from parameterized import parameterized

from airflow.dagwhat import api
from airflow.dagwhat import base
from tests.dagwhat.dagwhat_test_example_dags_utils import basic_dag


class DagwhatTaskSelectorTest(unittest.TestCase):

    @parameterized.expand([
        ('task_1', 1),
        ('task_2', 1),
        ('task_3', 1),
        ('task_4', 0),
    ])
    def test_single_task_selector(self, task_id, tasks_found):
        thedag = basic_dag()
        cases = api.task(task_id)

        task_groups = cases.generate_task_groups(thedag)
        self.assertListEqual(
            task_groups,
            [[(task_id, thedag.task_dict[task_id])] for _ in range(tasks_found)]
        )

    @parameterized.expand([
        (['task_1'], 1),
        (['task_1', 'task_2'], 2),
        (['task_1', 'task_2', 'task_3'], 3),
    ])
    def test_multiple_task_selector(self, task_ids, tasks_found):
        thedag = basic_dag()
        cases = api.tasks(*task_ids)

        task_groups = cases.generate_task_groups(thedag)
        self.assertListEqual(
            task_groups,
            [[(task_id, thedag.task_dict[task_id]) for task_id in task_ids]]
        )

    @parameterized.expand([
        (api.tasks('task_1', 'task_2', 'task_3', 'task_4'),),
        (api.task('task4'),),
    ])
    def test_task_must_exist_to_be_in_selector(self, selector):
        thedag = basic_dag()

        with self.assertRaises(ValueError):
            task_groups = selector.generate_task_groups(thedag)


