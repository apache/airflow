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
from unittest import mock

from airflow.executors.base_executor import BaseExecutor
from airflow.utils.state import State

from datetime import datetime


class TestBaseExecutor(unittest.TestCase):
    def test_get_event_buffer(self):
        executor = BaseExecutor()

        date = datetime.utcnow()
        try_number = 1
        key1 = ("my_dag1", "my_task1", date, try_number)
        key2 = ("my_dag2", "my_task1", date, try_number)
        key3 = ("my_dag2", "my_task2", date, try_number)
        state = State.SUCCESS
        executor.event_buffer[key1] = state
        executor.event_buffer[key2] = state
        executor.event_buffer[key3] = state

        self.assertEqual(len(executor.get_event_buffer(("my_dag1",))), 1)
        self.assertEqual(len(executor.get_event_buffer()), 2)
        self.assertEqual(len(executor.event_buffer), 0)

    @mock.patch('airflow.executors.base_executor.BaseExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.stats.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = BaseExecutor()
        executor.heartbeat()
        calls = [mock.call('executor.open_slots', mock.ANY),
                 mock.call('executor.queued_tasks', mock.ANY),
                 mock.call('executor.running_tasks', mock.ANY)]
        mock_stats_gauge.assert_has_calls(calls)
