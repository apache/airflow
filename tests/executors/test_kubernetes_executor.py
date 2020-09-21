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
#
import random
import re
import string
import unittest
from datetime import datetime

import mock
from urllib3 import HTTPResponse

from airflow.utils import timezone
from tests.test_utils.config import conf_vars

try:
    from kubernetes.client.rest import ApiException

    from airflow.executors.kubernetes_executor import AirflowKubernetesScheduler, KubernetesExecutor
    from airflow.kubernetes import pod_generator
    from airflow.kubernetes.pod_generator import PodGenerator
    from airflow.utils.state import State
except ImportError:
    AirflowKubernetesScheduler = None  # type: ignore


# pylint: disable=unused-argument
class TestAirflowKubernetesScheduler(unittest.TestCase):
    @staticmethod
    def _gen_random_string(seed, str_len):
        char_list = []
        for char_seed in range(str_len):
            random.seed(str(seed) * char_seed)
            char_list.append(random.choice(string.printable))
        return ''.join(char_list)

    def _cases(self):
        cases = [
            ("my_dag_id", "my-task-id"),
            ("my.dag.id", "my.task.id"),
            ("MYDAGID", "MYTASKID"),
            ("my_dag_id", "my_task_id"),
            ("mydagid" * 200, "my_task_id" * 200)
        ]

        cases.extend([
            (self._gen_random_string(seed, 200), self._gen_random_string(seed, 200))
            for seed in range(100)
        ])

        return cases

    @staticmethod
    def _is_valid_pod_id(name):
        regex = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        return (
            len(name) <= 253 and
            all(ch.lower() == ch for ch in name) and
            re.match(regex, name))

    @staticmethod
    def _is_safe_label_value(value):
        regex = r'^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$'
        return (
            len(value) <= 63 and
            re.match(regex, value))

    @unittest.skipIf(AirflowKubernetesScheduler is None,
                     'kubernetes python package is not installed')
    def test_create_pod_id(self):
        for dag_id, task_id in self._cases():
            pod_name = PodGenerator.make_unique_pod_id(
                AirflowKubernetesScheduler._create_pod_id(dag_id, task_id)
            )
            self.assertTrue(self._is_valid_pod_id(pod_name))

    def test_make_safe_label_value(self):
        for dag_id, task_id in self._cases():
            safe_dag_id = pod_generator.make_safe_label_value(dag_id)
            self.assertTrue(self._is_safe_label_value(safe_dag_id))
            safe_task_id = pod_generator.make_safe_label_value(task_id)
            self.assertTrue(self._is_safe_label_value(safe_task_id))
            dag_id = "my_dag_id"
            self.assertEqual(
                dag_id,
                pod_generator.make_safe_label_value(dag_id)
            )
            dag_id = "my_dag_id_" + "a" * 64
            self.assertEqual(
                "my_dag_id_" + "a" * 43 + "-0ce114c45",
                pod_generator.make_safe_label_value(dag_id)
            )

    def test_execution_date_serialize_deserialize(self):
        datetime_obj = datetime.now()
        serialized_datetime = \
            pod_generator.datetime_to_label_safe_datestring(
                datetime_obj)
        new_datetime_obj = pod_generator.label_safe_datestring_to_datetime(
            serialized_datetime)

        self.assertEqual(datetime_obj, new_datetime_obj)


class TestKubernetesExecutor(unittest.TestCase):
    """
    Tests if an ApiException from the Kube Client will cause the task to
    be rescheduled.
    """
    @unittest.skipIf(AirflowKubernetesScheduler is None,
                     'kubernetes python package is not installed')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_run_next_exception(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        import sys
        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'

        # When a quota is exceeded this is the ApiException we get
        response = HTTPResponse(
            body='{"kind": "Status", "apiVersion": "v1", "metadata": {}, "status": "Failure", '
                 '"message": "pods \\"podname\\" is forbidden: exceeded quota: compute-resources, '
                 'requested: limits.memory=4Gi, used: limits.memory=6508Mi, limited: limits.memory=10Gi", '
                 '"reason": "Forbidden", "details": {"name": "podname", "kind": "pods"}, "code": 403}')
        response.status = 403
        response.reason = "Forbidden"

        # A mock kube_client that throws errors when making a pod
        mock_kube_client = mock.patch('kubernetes.client.CoreV1Api', autospec=True)
        mock_kube_client.create_namespaced_pod = mock.MagicMock(
            side_effect=ApiException(http_resp=response))
        mock_get_kube_client.return_value = mock_kube_client
        mock_api_client = mock.MagicMock()
        mock_api_client.sanitize_for_serialization.return_value = {}
        mock_kube_client.api_client = mock_api_client
        config = {
            ('kubernetes', 'pod_template_file'): path,
        }
        with conf_vars(config):

            kubernetes_executor = KubernetesExecutor()
            kubernetes_executor.start()
            # Execute a task while the Api Throws errors
            try_number = 1
            kubernetes_executor.execute_async(key=('dag', 'task', datetime.utcnow(), try_number),
                                              queue=None,
                                              command=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
                                              )
            kubernetes_executor.sync()
            kubernetes_executor.sync()

            assert mock_kube_client.create_namespaced_pod.called
            self.assertFalse(kubernetes_executor.task_queue.empty())

            # Disable the ApiException
            mock_kube_client.create_namespaced_pod.side_effect = None

            # Execute the task without errors should empty the queue
            kubernetes_executor.sync()
            assert mock_kube_client.create_namespaced_pod.called
            self.assertTrue(kubernetes_executor.task_queue.empty())

    @mock.patch('airflow.executors.kubernetes_executor.KubeConfig')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync, mock_kube_config):
        executor = KubernetesExecutor()
        executor.heartbeat()
        calls = [mock.call('executor.open_slots', mock.ANY),
                 mock.call('executor.queued_tasks', mock.ANY),
                 mock.call('executor.running_tasks', mock.ANY)]
        mock_stats_gauge.assert_has_calls(calls)

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_change_state_running(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = KubernetesExecutor()
        executor.start()
        key = ('dag_id', 'task_id', 'ex_time', 'try_number1')
        executor._change_state(key, State.RUNNING, 'pod_id', 'default')
        self.assertTrue(executor.event_buffer[key][0] == State.RUNNING)

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_success(self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = KubernetesExecutor()
        executor.start()
        test_time = timezone.utcnow()
        key = ('dag_id', 'task_id', test_time, 'try_number2')
        executor._change_state(key, State.SUCCESS, 'pod_id', 'default')
        self.assertTrue(executor.event_buffer[key][0] == State.SUCCESS)
        mock_delete_pod.assert_called_once_with('pod_id', 'default')

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_failed_no_deletion(
        self,
        mock_delete_pod,
        mock_get_kube_client,
        mock_kubernetes_job_watcher
    ):
        executor = KubernetesExecutor()
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False
        executor.start()
        test_time = timezone.utcnow()
        key = ('dag_id', 'task_id', test_time, 'try_number3')
        executor._change_state(key, State.FAILED, 'pod_id', 'default')
        self.assertTrue(executor.event_buffer[key][0] == State.FAILED)
        mock_delete_pod.assert_not_called()
# pylint: enable=unused-argument

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_skip_pod_deletion(self, mock_delete_pod, mock_get_kube_client,
                                            mock_kubernetes_job_watcher):
        test_time = timezone.utcnow()
        executor = KubernetesExecutor()
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False

        executor.start()
        key = ('dag_id', 'task_id', test_time, 'try_number2')
        executor._change_state(key, State.SUCCESS, 'pod_id', 'default')
        self.assertTrue(executor.event_buffer[key][0] == State.SUCCESS)
        mock_delete_pod.assert_not_called()

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_failed_pod_deletion(self, mock_delete_pod, mock_get_kube_client,
                                              mock_kubernetes_job_watcher):
        executor = KubernetesExecutor()
        executor.kube_config.delete_worker_pods_on_failure = True

        executor.start()
        key = ('dag_id', 'task_id', 'ex_time', 'try_number2')
        executor._change_state(key, State.FAILED, 'pod_id', 'test-namespace')
        self.assertTrue(executor.event_buffer[key][0] == State.FAILED)
        mock_delete_pod.assert_called_once_with('pod_id', 'test-namespace')
