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
import subprocess
import unittest
from unittest import mock

from airflow.executors.local_executor import LocalExecutor
from airflow.utils.state import State


class TestLocalExecutor(unittest.TestCase):

    TEST_SUCCESS_COMMANDS = 5

    @mock.patch('airflow.executors.local_executor.subprocess.check_call')
    def execution_parallelism(self, mock_check_call, parallelism=0):
        success_command = ['airflow', 'tasks', 'run', 'true', 'some_parameter']
        fail_command = ['airflow', 'tasks', 'run', 'false']

        def fake_execute_command(command, close_fds=True):  # pylint: disable=unused-argument
            if command != success_command:
                raise subprocess.CalledProcessError(returncode=1, cmd=command)
            else:
                return 0

        mock_check_call.side_effect = fake_execute_command

        executor = LocalExecutor(parallelism=parallelism)
        executor.start()

        success_key = 'success {}'
        self.assertTrue(executor.result_queue.empty())

        execution_date = datetime.datetime.now()
        for i in range(self.TEST_SUCCESS_COMMANDS):
            key_id, command = success_key.format(i), success_command
            key = key_id, 'fake_ti', execution_date, 0
            executor.running.add(key)
            executor.execute_async(key=key, command=command)

        fail_key = 'fail', 'fake_ti', execution_date, 0
        executor.running.add(fail_key)
        executor.execute_async(key=fail_key, command=fail_command)

        executor.end()
        # By that time Queues are already shutdown so we cannot check if they are empty
        self.assertEqual(len(executor.running), 0)

        for i in range(self.TEST_SUCCESS_COMMANDS):
            key_id = success_key.format(i)
            key = key_id, 'fake_ti', execution_date, 0
            self.assertEqual(executor.event_buffer[key][0], State.SUCCESS)
        self.assertEqual(executor.event_buffer[fail_key][0], State.FAILED)

        expected = self.TEST_SUCCESS_COMMANDS + 1 if parallelism == 0 else parallelism
        self.assertEqual(executor.workers_used, expected)

    def test_execution_unlimited_parallelism(self):
        self.execution_parallelism(parallelism=0)  # pylint: disable=no-value-for-parameter

    def test_execution_limited_parallelism(self):
        test_parallelism = 2
        self.execution_parallelism(parallelism=test_parallelism)  # pylint: disable=no-value-for-parameter

    @mock.patch('airflow.executors.local_executor.LocalExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = LocalExecutor()
        executor.heartbeat()
        calls = [mock.call('executor.open_slots', mock.ANY),
                 mock.call('executor.queued_tasks', mock.ANY),
                 mock.call('executor.running_tasks', mock.ANY)]
        mock_stats_gauge.assert_has_calls(calls)
