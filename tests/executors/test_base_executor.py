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
from datetime import datetime
from unittest import mock

from airflow import DAG
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.state import State


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

    @mock.patch('airflow.executors.base_executor.BaseExecutor.execute_async')
    def test_execute_async_is_called_with_rendered_executor_config(self, mock_execute_async):
        executor = BaseExecutor()
        dag = DAG(dag_id='foo', start_date=datetime(2019, 11, 5), end_date=datetime(2019, 11, 5))

        class MyBashOperator(BashOperator):
            template_fields = ('bash_command', 'env', 'executor_config')

        executor_config = {
            "KubernetesExecutor": {
                "image": "ubuntu:16.04",
                "request_memory": "2Gi",
                "request_cpu": '1',
                "volumes": [
                    {
                        "name": "data",
                        "persistentVolumeClaim": {"claimName": "some_claim_name"},
                    },
                ],
                "volume_mounts": [
                    {
                        "mountPath": "/data",
                        "name": "data",
                        "subPath": "{{ ds }}"
                    },
                ]
            }
        }
        operator = MyBashOperator(
            task_id='task_id',
            bash_command='echo {{ run_id }}',
            executor_config=executor_config,
            dag=dag,
        )

        ti = TaskInstance(task=operator, execution_date=datetime(2020, 1, 1))
        executor.queue_task_instance(task_instance=ti)
        executor.heartbeat()
        actual_executor_config = mock_execute_async.call_args_list[0][1]['executor_config']

        # the execute_async is called with rendered executor_config
        self.assertEqual('2020-01-01', actual_executor_config['KubernetesExecutor']['volume_mounts'][0]['subPath'])

        # bash_command template field is not rendered (backward compatible)
        self.assertEqual('echo {{ run_id }}', ti.task.bash_command)

        self.assertEqual(ti.executor_config,
                         ti.task.executor_config,
                         "executor_config in task instance and the task must be the same")
        self.assertEqual('2020-01-01', ti.executor_config['KubernetesExecutor']['volume_mounts'][0]['subPath'])
