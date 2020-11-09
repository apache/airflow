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
from datetime import datetime, timedelta
from unittest import mock

from airflow.executors.base_executor import BaseExecutor
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.utils.state import State


class TestBaseExecutor(unittest.TestCase):
    def test_get_event_buffer(self):
        executor = BaseExecutor()

        date = datetime.utcnow()
        try_number = 1
        key1 = TaskInstanceKey("my_dag1", "my_task1", date, try_number)
        key2 = TaskInstanceKey("my_dag2", "my_task1", date, try_number)
        key3 = TaskInstanceKey("my_dag2", "my_task2", date, try_number)
        state = State.SUCCESS
        executor.event_buffer[key1] = state, None
        executor.event_buffer[key2] = state, None
        executor.event_buffer[key3] = state, None

        self.assertEqual(len(executor.get_event_buffer(("my_dag1",))), 1)
        self.assertEqual(len(executor.get_event_buffer()), 2)
        self.assertEqual(len(executor.event_buffer), 0)

    @mock.patch('airflow.executors.base_executor.BaseExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = BaseExecutor()
        executor.heartbeat()
        calls = [
            mock.call('executor.open_slots', mock.ANY),
            mock.call('executor.queued_tasks', mock.ANY),
            mock.call('executor.running_tasks', mock.ANY),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    def test_try_adopt_task_instances(self):
        date = datetime.utcnow()
        start_date = datetime.utcnow() - timedelta(days=2)

        with DAG("test_try_adopt_task_instances"):
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)
            task_3 = BaseOperator(task_id="task_3", start_date=start_date)

        key1 = TaskInstance(task=task_1, execution_date=date)
        key2 = TaskInstance(task=task_2, execution_date=date)
        key3 = TaskInstance(task=task_3, execution_date=date)
        tis = [key1, key2, key3]
        self.assertEqual(BaseExecutor().try_adopt_task_instances(tis), tis)
