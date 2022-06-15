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
import pathlib
import random
import re
import string
import unittest
from datetime import datetime, timedelta
from unittest import mock

import pytest
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException
from urllib3 import HTTPResponse

from airflow import AirflowException
from airflow.models.taskinstance import TaskInstanceKey
from airflow.operators.bash import BashOperator
from airflow.utils import timezone
from tests.test_utils.config import conf_vars

try:
    from airflow.executors.kubernetes_executor import (
        AirflowKubernetesScheduler,
        KubernetesExecutor,
        KubernetesJobWatcher,
        ResourceVersion,
        create_pod_id,
        get_base_pod_from_template,
    )
    from airflow.kubernetes import pod_generator
    from airflow.kubernetes.kubernetes_helper_functions import annotations_to_key
    from airflow.kubernetes.pod_generator import PodGenerator
    from airflow.utils.state import State
except ImportError:
    AirflowKubernetesScheduler = None  # type: ignore


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
            ("mydagid" * 200, "my_task_id" * 200),
            ("my_dág_id", "my_tásk_id"),
            ("Компьютер", "niedołężność"),
            ("影師嗎", "中華民國;$"),
        ]

        cases.extend(
            [(self._gen_random_string(seed, 200), self._gen_random_string(seed, 200)) for seed in range(100)]
        )

        return cases

    @staticmethod
    def _is_valid_pod_id(name):
        regex = r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"
        return len(name) <= 253 and all(ch.lower() == ch for ch in name) and re.match(regex, name)

    @staticmethod
    def _is_safe_label_value(value):
        regex = r'^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$'
        return len(value) <= 63 and re.match(regex, value)

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    def test_create_pod_id(self):
        for dag_id, task_id in self._cases():
            pod_name = PodGenerator.make_unique_pod_id(create_pod_id(dag_id, task_id))
            assert self._is_valid_pod_id(pod_name)

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    @mock.patch("airflow.kubernetes.pod_generator.PodGenerator")
    @mock.patch("airflow.executors.kubernetes_executor.KubeConfig")
    def test_get_base_pod_from_template(self, mock_kubeconfig, mock_generator):
        pod_template_file_path = "/bar/biz"
        get_base_pod_from_template(pod_template_file_path, None)
        assert "deserialize_model_dict" == mock_generator.mock_calls[0][0]
        assert pod_template_file_path == mock_generator.mock_calls[0][1][0]
        mock_kubeconfig.pod_template_file = "/foo/bar"
        get_base_pod_from_template(None, mock_kubeconfig)
        assert "deserialize_model_dict" == mock_generator.mock_calls[1][0]
        assert "/foo/bar" == mock_generator.mock_calls[1][1][0]

    def test_make_safe_label_value(self):
        for dag_id, task_id in self._cases():
            safe_dag_id = pod_generator.make_safe_label_value(dag_id)
            assert self._is_safe_label_value(safe_dag_id)
            safe_task_id = pod_generator.make_safe_label_value(task_id)
            assert self._is_safe_label_value(safe_task_id)
            dag_id = "my_dag_id"
            assert dag_id == pod_generator.make_safe_label_value(dag_id)
            dag_id = "my_dag_id_" + "a" * 64
            assert "my_dag_id_" + "a" * 43 + "-0ce114c45" == pod_generator.make_safe_label_value(dag_id)

    def test_execution_date_serialize_deserialize(self):
        datetime_obj = datetime.now()
        serialized_datetime = pod_generator.datetime_to_label_safe_datestring(datetime_obj)
        new_datetime_obj = pod_generator.label_safe_datestring_to_datetime(serialized_datetime)

        assert datetime_obj == new_datetime_obj

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.client')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    def test_delete_pod_successfully(self, mock_watcher, mock_client, mock_kube_client):
        pod_id = "my-pod-1"
        namespace = "my-namespace-1"

        mock_delete_namespace = mock.MagicMock()
        mock_kube_client.return_value.delete_namespaced_pod = mock_delete_namespace

        kube_executor = KubernetesExecutor()
        kube_executor.job_id = "test-job-id"
        kube_executor.start()
        kube_executor.kube_scheduler.delete_pod(pod_id, namespace)

        mock_delete_namespace.assert_called_with(pod_id, namespace, body=mock_client.V1DeleteOptions())

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.client')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    def test_delete_pod_raises_404(self, mock_watcher, mock_client, mock_kube_client):
        pod_id = "my-pod-1"
        namespace = "my-namespace-2"

        mock_delete_namespace = mock.MagicMock()
        mock_kube_client.return_value.delete_namespaced_pod = mock_delete_namespace

        # ApiException is raised because status is not 404
        mock_kube_client.return_value.delete_namespaced_pod.side_effect = ApiException(status=400)
        kube_executor = KubernetesExecutor()
        kube_executor.job_id = "test-job-id"
        kube_executor.start()

        with pytest.raises(ApiException):
            kube_executor.kube_scheduler.delete_pod(pod_id, namespace)
            mock_delete_namespace.assert_called_with(pod_id, namespace, body=mock_client.V1DeleteOptions())

    @unittest.skipIf(AirflowKubernetesScheduler is None, 'kubernetes python package is not installed')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.client')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    def test_delete_pod_404_not_raised(self, mock_watcher, mock_client, mock_kube_client):
        pod_id = "my-pod-1"
        namespace = "my-namespace-3"

        mock_delete_namespace = mock.MagicMock()
        mock_kube_client.return_value.delete_namespaced_pod = mock_delete_namespace

        # ApiException not raised because the status is 404
        mock_kube_client.return_value.delete_namespaced_pod.side_effect = ApiException(status=404)
        kube_executor = KubernetesExecutor()
        kube_executor.job_id = "test-job-id"
        kube_executor.start()

        kube_executor.kube_scheduler.delete_pod(pod_id, namespace)
        mock_delete_namespace.assert_called_with(pod_id, namespace, body=mock_client.V1DeleteOptions())


class TestKubernetesExecutor:
    """
    Tests if an ApiException from the Kube Client will cause the task to
    be rescheduled.
    """

    def setup_method(self) -> None:
        self.kubernetes_executor = KubernetesExecutor()
        self.kubernetes_executor.job_id = 5

    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason='kubernetes python package is not installed'
    )
    @pytest.mark.parametrize(
        'status, should_requeue',
        [
            pytest.param(403, True, id='403 Forbidden'),
            pytest.param(12345, True, id='12345 fake-unhandled-reason'),
            pytest.param(422, False, id='422 Unprocessable Entity'),
            pytest.param(400, False, id='400 BadRequest'),
        ],
    )
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_run_next_exception_requeue(
        self, mock_get_kube_client, mock_kubernetes_job_watcher, status, should_requeue
    ):
        """
        When pod scheduling fails with either reason 'Forbidden', or any reason not yet
        handled in the relevant try-except block, the task should stay in the ``task_queue``
        and be attempted on a subsequent executor sync.  When reason is 'Unprocessable Entity'
        or 'BadRequest', the task should be failed without being re-queued.

        Note on error scenarios:

        - 403 Forbidden will be returned when your request exceeds namespace quota.
        - 422 Unprocessable Entity is returned when your parameters are valid but unsupported
            e.g. limits lower than requests.
        - 400 BadRequest is returned when your parameters are invalid e.g. asking for cpu=100ABC123.

        """
        import sys

        path = sys.path[0] + '/tests/kubernetes/pod_generator_base_with_secrets.yaml'

        response = HTTPResponse(body='{"message": "any message"}', status=status)

        # A mock kube_client that throws errors when making a pod
        mock_kube_client = mock.patch('kubernetes.client.CoreV1Api', autospec=True)
        mock_kube_client.create_namespaced_pod = mock.MagicMock(side_effect=ApiException(http_resp=response))
        mock_get_kube_client.return_value = mock_kube_client
        mock_api_client = mock.MagicMock()
        mock_api_client.sanitize_for_serialization.return_value = {}
        mock_kube_client.api_client = mock_api_client
        config = {
            ('kubernetes', 'pod_template_file'): path,
        }
        with conf_vars(config):
            kubernetes_executor = self.kubernetes_executor
            kubernetes_executor.start()
            # Execute a task while the Api Throws errors
            try_number = 1
            task_instance_key = TaskInstanceKey('dag', 'task', 'run_id', try_number)
            kubernetes_executor.execute_async(
                key=task_instance_key,
                queue=None,
                command=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
            )
            kubernetes_executor.sync()

            assert mock_kube_client.create_namespaced_pod.call_count == 1

            if should_requeue:
                assert not kubernetes_executor.task_queue.empty()

                # Disable the ApiException
                mock_kube_client.create_namespaced_pod.side_effect = None

                # Execute the task without errors should empty the queue
                mock_kube_client.create_namespaced_pod.reset_mock()
                kubernetes_executor.sync()
                assert mock_kube_client.create_namespaced_pod.called
                assert kubernetes_executor.task_queue.empty()
            else:
                assert kubernetes_executor.task_queue.empty()
                assert kubernetes_executor.event_buffer[task_instance_key][0] == State.FAILED

    @mock.patch('airflow.executors.kubernetes_executor.KubeConfig')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor.sync')
    @mock.patch('airflow.executors.base_executor.BaseExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync, mock_kube_config):
        executor = self.kubernetes_executor
        executor.heartbeat()
        calls = [
            mock.call('executor.open_slots', mock.ANY),
            mock.call('executor.queued_tasks', mock.ANY),
            mock.call('executor.running_tasks', mock.ANY),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_invalid_executor_config(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()

        assert executor.event_buffer == {}
        executor.execute_async(
            key=('dag', 'task', datetime.utcnow(), 1),
            queue=None,
            command=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
            executor_config=k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[k8s.V1Container(name="base", image="myimage", image_pull_policy="Always")]
                )
            ),
        )

        assert list(executor.event_buffer.values())[0][1] == "Invalid executor_config passed"

    @pytest.mark.execution_timeout(10)
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason='kubernetes python package is not installed'
    )
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.run_pod_async')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_pod_template_file_override_in_executor_config(self, mock_get_kube_client, mock_run_pod_async):
        current_folder = pathlib.Path(__file__).parent.resolve()
        template_file = str(
            (current_folder / "kubernetes_executor_template_files" / "basic_template.yaml").resolve()
        )

        mock_kube_client = mock.patch('kubernetes.client.CoreV1Api', autospec=True)
        mock_get_kube_client.return_value = mock_kube_client

        with conf_vars({('kubernetes', 'pod_template_file'): ''}):
            executor = self.kubernetes_executor
            executor.start()

            assert executor.event_buffer == {}
            assert executor.task_queue.empty()

            executor.execute_async(
                key=TaskInstanceKey('dag', 'task', 'run_id', 1),
                queue=None,
                command=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
                executor_config={
                    "pod_template_file": template_file,
                    "pod_override": k8s.V1Pod(
                        metadata=k8s.V1ObjectMeta(labels={"release": "stable"}),
                        spec=k8s.V1PodSpec(
                            containers=[k8s.V1Container(name="base", image="airflow:3.6")],
                        ),
                    ),
                },
            )

            assert not executor.task_queue.empty()
            task = executor.task_queue.get_nowait()
            _, _, expected_executor_config, expected_pod_template_file = task

            # Test that the correct values have been put to queue
            assert expected_executor_config.metadata.labels == {'release': 'stable'}
            assert expected_pod_template_file == template_file

            self.kubernetes_executor.kube_scheduler.run_next(task)
            mock_run_pod_async.assert_called_once_with(
                k8s.V1Pod(
                    api_version="v1",
                    kind="Pod",
                    metadata=k8s.V1ObjectMeta(
                        name=mock.ANY,
                        namespace="default",
                        annotations={
                            'dag_id': 'dag',
                            'run_id': 'run_id',
                            'task_id': 'task',
                            'try_number': '1',
                        },
                        labels={
                            'airflow-worker': '5',
                            'airflow_version': mock.ANY,
                            'dag_id': 'dag',
                            'run_id': 'run_id',
                            'kubernetes_executor': 'True',
                            'mylabel': 'foo',
                            'release': 'stable',
                            'task_id': 'task',
                            'try_number': '1',
                        },
                    ),
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                image="airflow:3.6",
                                args=['airflow', 'tasks', 'run', 'true', 'some_parameter'],
                                env=[k8s.V1EnvVar(name='AIRFLOW_IS_K8S_EXECUTOR_POD', value='True')],
                            )
                        ],
                        image_pull_secrets=[k8s.V1LocalObjectReference(name='airflow-registry')],
                        scheduler_name='default-scheduler',
                        security_context=k8s.V1PodSecurityContext(fs_group=50000, run_as_user=50000),
                    ),
                )
            )

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_change_state_running(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        key = ('dag_id', 'task_id', 'run_id', 'try_number1')
        executor._change_state(key, State.RUNNING, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.RUNNING

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_success(self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        key = ('dag_id', 'task_id', 'run_id', 'try_number2')
        executor._change_state(key, State.SUCCESS, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.SUCCESS
        mock_delete_pod.assert_called_once_with('pod_id', 'default')

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_failed_no_deletion(
        self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False
        executor.start()
        key = ('dag_id', 'task_id', 'run_id', 'try_number3')
        executor._change_state(key, State.FAILED, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.FAILED
        mock_delete_pod.assert_not_called()

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_skip_pod_deletion(
        self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False

        executor.start()
        key = ('dag_id', 'task_id', 'run_id', 'try_number2')
        executor._change_state(key, State.SUCCESS, 'pod_id', 'default')
        assert executor.event_buffer[key][0] == State.SUCCESS
        mock_delete_pod.assert_not_called()

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler.delete_pod')
    def test_change_state_failed_pod_deletion(
        self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods_on_failure = True

        executor.start()
        key = ('dag_id', 'task_id', 'run_id', 'try_number2')
        executor._change_state(key, State.FAILED, 'pod_id', 'test-namespace')
        assert executor.event_buffer[key][0] == State.FAILED
        mock_delete_pod.assert_called_once_with('pod_id', 'test-namespace')

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor.adopt_launched_task')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods')
    def test_try_adopt_task_instances(self, mock_adopt_completed_pods, mock_adopt_launched_task):
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "10"
        ti_key = annotations_to_key(
            {
                'dag_id': 'dag',
                'run_id': 'run_id',
                'task_id': 'task',
                'try_number': '1',
            }
        )
        mock_ti = mock.MagicMock(queued_by_job_id="1", external_executor_id="1", key=ti_key)
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="foo"))
        mock_kube_client = mock.MagicMock()
        mock_kube_client.list_namespaced_pod.return_value.items = [pod]
        executor.kube_client = mock_kube_client

        # First adoption
        reset_tis = executor.try_adopt_task_instances([mock_ti])
        mock_kube_client.list_namespaced_pod.assert_called_once_with(
            namespace='default', label_selector='airflow-worker=1'
        )
        mock_adopt_launched_task.assert_called_once_with(mock_kube_client, pod, {ti_key: mock_ti})
        mock_adopt_completed_pods.assert_called_once()
        assert reset_tis == [mock_ti]  # assume failure adopting when checking return

        # Second adoption (queued_by_job_id and external_executor_id no longer match)
        mock_kube_client.reset_mock()
        mock_adopt_launched_task.reset_mock()
        mock_adopt_completed_pods.reset_mock()

        mock_ti.queued_by_job_id = "10"  # scheduler_job would have updated this after the first adoption
        executor.scheduler_job_id = "20"
        # assume success adopting when checking return, `adopt_launched_task` pops `ti_key` from `pod_ids`
        mock_adopt_launched_task.side_effect = lambda client, pod, pod_ids: pod_ids.pop(ti_key)

        reset_tis = executor.try_adopt_task_instances([mock_ti])
        mock_kube_client.list_namespaced_pod.assert_called_once_with(
            namespace='default', label_selector='airflow-worker=10'
        )
        mock_adopt_launched_task.assert_called_once()  # Won't check args this time around as they get mutated
        mock_adopt_completed_pods.assert_called_once()
        assert reset_tis == []  # This time our return is empty - no TIs to reset

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods')
    def test_try_adopt_task_instances_multiple_scheduler_ids(self, mock_adopt_completed_pods):
        """We try to find pods only once per scheduler id"""
        executor = self.kubernetes_executor
        mock_kube_client = mock.MagicMock()
        executor.kube_client = mock_kube_client

        mock_tis = [
            mock.MagicMock(queued_by_job_id="10", external_executor_id="1", dag_id="dag", task_id="task"),
            mock.MagicMock(queued_by_job_id="40", external_executor_id="1", dag_id="dag", task_id="task2"),
            mock.MagicMock(queued_by_job_id="40", external_executor_id="1", dag_id="dag", task_id="task3"),
        ]

        executor.try_adopt_task_instances(mock_tis)
        assert mock_kube_client.list_namespaced_pod.call_count == 2
        mock_kube_client.list_namespaced_pod.assert_has_calls(
            [
                mock.call(namespace='default', label_selector='airflow-worker=10'),
                mock.call(namespace='default', label_selector='airflow-worker=40'),
            ],
            any_order=True,
        )

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor.adopt_launched_task')
    @mock.patch('airflow.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods')
    def test_try_adopt_task_instances_no_matching_pods(
        self, mock_adopt_completed_pods, mock_adopt_launched_task
    ):
        executor = self.kubernetes_executor
        mock_ti = mock.MagicMock(queued_by_job_id="1", external_executor_id="1", dag_id="dag", task_id="task")
        mock_kube_client = mock.MagicMock()
        mock_kube_client.list_namespaced_pod.return_value.items = []
        executor.kube_client = mock_kube_client

        tis_to_flush = executor.try_adopt_task_instances([mock_ti])
        assert tis_to_flush == [mock_ti]
        mock_adopt_launched_task.assert_not_called()
        mock_adopt_completed_pods.assert_called_once()

    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_adopt_launched_task(self, mock_kube_client):
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        annotations = {
            'dag_id': 'dag',
            'run_id': 'run_id',
            'task_id': 'task',
            'try_number': '1',
        }
        ti_key = annotations_to_key(annotations)
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="foo", labels={"airflow-worker": "bar"}, annotations=annotations)
        )
        pod_ids = {ti_key: {}}

        executor.adopt_launched_task(mock_kube_client, pod=pod, pod_ids=pod_ids)
        assert mock_kube_client.patch_namespaced_pod.call_args[1] == {
            'body': {
                'metadata': {
                    'labels': {'airflow-worker': 'modified'},
                    'annotations': annotations,
                    'name': 'foo',
                }
            },
            'name': 'foo',
            'namespace': None,
        }
        assert pod_ids == {}
        assert executor.running == {ti_key}

    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    def test_not_adopt_unassigned_task(self, mock_kube_client):
        """
        We should not adopt any tasks that were not assigned by the scheduler.
        This ensures that there is no contention over pod management.
        """

        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        pod_ids = {"foobar": {}}
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo",
                labels={"airflow-worker": "bar"},
                annotations={
                    'dag_id': 'dag',
                    'run_id': 'run_id',
                    'task_id': 'task',
                    'try_number': '1',
                },
            )
        )
        executor.adopt_launched_task(mock_kube_client, pod=pod, pod_ids=pod_ids)
        assert not mock_kube_client.patch_namespaced_pod.called
        assert pod_ids == {"foobar": {}}

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler')
    def test_pending_pod_timeout(self, mock_kubescheduler, mock_get_kube_client, mock_kubernetes_job_watcher):
        mock_delete_pod = mock_kubescheduler.return_value.delete_pod
        mock_kube_client = mock_get_kube_client.return_value
        now = timezone.utcnow()
        pending_pods = [
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name="foo60",
                    labels={"airflow-worker": "123"},
                    creation_timestamp=now - timedelta(seconds=60),
                    namespace="mynamespace",
                )
            ),
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name="foo90",
                    labels={"airflow-worker": "123"},
                    creation_timestamp=now - timedelta(seconds=90),
                    namespace="mynamespace",
                )
            ),
        ]
        mock_kube_client.list_namespaced_pod.return_value.items = pending_pods

        config = {
            ('kubernetes', 'namespace'): 'mynamespace',
            ('kubernetes', 'worker_pods_pending_timeout'): '75',
            ('kubernetes', 'worker_pods_pending_timeout_batch_size'): '5',
            ('kubernetes', 'kube_client_request_args'): '{"sentinel": "foo"}',
        }
        with conf_vars(config):
            executor = KubernetesExecutor()
            executor.job_id = "123"
            executor.start()
            assert 2 == len(executor.event_scheduler.queue)
            executor._check_worker_pods_pending_timeout()

        mock_kube_client.list_namespaced_pod.assert_called_once_with(
            'mynamespace',
            field_selector='status.phase=Pending',
            label_selector='airflow-worker=123',
            limit=5,
            sentinel='foo',
        )
        mock_delete_pod.assert_called_once_with('foo90', 'mynamespace')

    @mock.patch('airflow.executors.kubernetes_executor.KubernetesJobWatcher')
    @mock.patch('airflow.executors.kubernetes_executor.get_kube_client')
    @mock.patch('airflow.executors.kubernetes_executor.AirflowKubernetesScheduler')
    def test_pending_pod_timeout_multi_namespace_mode(
        self, mock_kubescheduler, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        mock_delete_pod = mock_kubescheduler.return_value.delete_pod
        mock_kube_client = mock_get_kube_client.return_value
        now = timezone.utcnow()
        pending_pods = [
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name="foo90",
                    labels={"airflow-worker": "123"},
                    creation_timestamp=now - timedelta(seconds=500),
                    namespace="anothernamespace",
                )
            ),
        ]
        mock_kube_client.list_pod_for_all_namespaces.return_value.items = pending_pods

        config = {
            ('kubernetes', 'namespace'): 'mynamespace',
            ('kubernetes', 'multi_namespace_mode'): 'true',
            ('kubernetes', 'kube_client_request_args'): '{"sentinel": "foo"}',
        }
        with conf_vars(config):
            executor = KubernetesExecutor()
            executor.job_id = "123"
            executor.start()
            executor._check_worker_pods_pending_timeout()

        mock_kube_client.list_pod_for_all_namespaces.assert_called_once_with(
            field_selector='status.phase=Pending',
            label_selector='airflow-worker=123',
            limit=100,
            sentinel='foo',
        )
        mock_delete_pod.assert_called_once_with('foo90', 'anothernamespace')

    def test_clear_not_launched_queued_tasks_not_launched(self, dag_maker, create_dummy_dag, session):
        """If a pod isn't found for a TI, reset the state to scheduled"""
        mock_kube_client = mock.MagicMock()
        mock_kube_client.list_namespaced_pod.return_value = k8s.V1PodList(items=[])

        create_dummy_dag(dag_id="test_clear", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_by_job_id = 1
        session.flush()

        executor = self.kubernetes_executor
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti.refresh_from_db()
        assert ti.state == State.SCHEDULED
        assert mock_kube_client.list_namespaced_pod.call_count == 2
        mock_kube_client.list_namespaced_pod.assert_any_call(
            "default", label_selector="dag_id=test_clear,task_id=task1,airflow-worker=1,run_id=test"
        )
        # also check that we fall back to execution_date if we didn't find the pod with run_id
        execution_date_label = pod_generator.datetime_to_label_safe_datestring(ti.execution_date)
        mock_kube_client.list_namespaced_pod.assert_called_with(
            "default",
            label_selector=(
                f"dag_id=test_clear,task_id=task1,airflow-worker=1,execution_date={execution_date_label}"
            ),
        )

    @pytest.mark.parametrize(
        'task_queue, kubernetes_queue',
        [
            pytest.param('default', None),
            pytest.param('kubernetes', None),
            pytest.param('kubernetes', 'kubernetes'),
        ],
    )
    def test_clear_not_launched_queued_tasks_launched(
        self, dag_maker, create_dummy_dag, session, task_queue, kubernetes_queue
    ):
        """Leave the state alone if a pod already exists"""
        mock_kube_client = mock.MagicMock()
        mock_kube_client.list_namespaced_pod.return_value = k8s.V1PodList(items=["something"])

        create_dummy_dag(dag_id="test_clear", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_by_job_id = 1
        ti.queue = task_queue
        session.flush()

        executor = self.kubernetes_executor
        executor.kubernetes_queue = kubernetes_queue
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti.refresh_from_db()
        assert ti.state == State.QUEUED
        mock_kube_client.list_namespaced_pod.assert_called_once_with(
            "default", label_selector="dag_id=test_clear,task_id=task1,airflow-worker=1,run_id=test"
        )

    def test_clear_not_launched_queued_tasks_mapped_task(self, dag_maker, session):
        """One mapped task has a launched pod - other does not."""

        def list_namespaced_pod(*args, **kwargs):
            if 'map_index=0' in kwargs['label_selector']:
                return k8s.V1PodList(items=["something"])
            else:
                return k8s.V1PodList(items=[])

        mock_kube_client = mock.MagicMock()
        mock_kube_client.list_namespaced_pod.side_effect = list_namespaced_pod

        with dag_maker(dag_id='test_clear'):
            op = BashOperator.partial(task_id="bash").expand(bash_command=["echo 0", "echo 1"])

        dag_run = dag_maker.create_dagrun()
        ti0 = dag_run.get_task_instance(op.task_id, session, map_index=0)
        ti0.state = State.QUEUED
        ti0.queued_by_job_id = 1

        ti1 = dag_run.get_task_instance(op.task_id, session, map_index=1)
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = 1

        session.flush()

        executor = self.kubernetes_executor
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti0.refresh_from_db()
        ti1.refresh_from_db()
        assert ti0.state == State.QUEUED
        assert ti1.state == State.SCHEDULED

        assert mock_kube_client.list_namespaced_pod.call_count == 3
        execution_date_label = pod_generator.datetime_to_label_safe_datestring(dag_run.execution_date)
        mock_kube_client.list_namespaced_pod.assert_has_calls(
            [
                mock.call(
                    "default",
                    label_selector="dag_id=test_clear,task_id=bash,airflow-worker=1,map_index=0,run_id=test",
                ),
                mock.call(
                    "default",
                    label_selector="dag_id=test_clear,task_id=bash,airflow-worker=1,map_index=1,run_id=test",
                ),
                mock.call(
                    "default",
                    label_selector=f"dag_id=test_clear,task_id=bash,airflow-worker=1,map_index=1,"
                    f"execution_date={execution_date_label}",
                ),
            ],
            any_order=True,
        )

    def test_clear_not_launched_queued_tasks_not_launched_other_queue(
        self, dag_maker, create_dummy_dag, session
    ):
        """Queued TI has no pod, but it is not queued for the k8s executor"""
        mock_kube_client = mock.MagicMock()
        mock_kube_client.list_namespaced_pod.return_value = k8s.V1PodList(items=[])

        create_dummy_dag(dag_id="test_clear", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_by_job_id = 1
        session.flush()

        executor = self.kubernetes_executor
        executor.kubernetes_queue = 'kubernetes'
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti.refresh_from_db()
        assert ti.state == State.QUEUED
        assert mock_kube_client.list_namespaced_pod.call_count == 0


class TestKubernetesJobWatcher(unittest.TestCase):
    def setUp(self):
        self.watcher = KubernetesJobWatcher(
            namespace="airflow",
            multi_namespace_mode=False,
            watcher_queue=mock.MagicMock(),
            resource_version="0",
            scheduler_job_id="123",
            kube_config=mock.MagicMock(),
        )
        self.kube_client = mock.MagicMock()
        self.core_annotations = {
            "dag_id": "dag",
            "task_id": "task",
            "run_id": "run_id",
            "try_number": "1",
            "execution_date": None,
        }
        self.pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo",
                annotations={"airflow-worker": "bar", **self.core_annotations},
                namespace="airflow",
                resource_version="456",
            ),
            status=k8s.V1PodStatus(phase="Pending"),
        )
        self.events = []

    def _run(self):
        with mock.patch('airflow.executors.kubernetes_executor.watch') as mock_watch:
            mock_watch.Watch.return_value.stream.return_value = self.events
            latest_resource_version = self.watcher._run(
                self.kube_client,
                self.watcher.resource_version,
                self.watcher.scheduler_job_id,
                self.watcher.kube_config,
            )
            assert self.pod.metadata.resource_version == latest_resource_version

    def assert_watcher_queue_called_once_with_state(self, state):
        self.watcher.watcher_queue.put.assert_called_once_with(
            (
                self.pod.metadata.name,
                self.watcher.namespace,
                state,
                self.core_annotations,
                self.pod.metadata.resource_version,
            )
        )

    def test_process_status_pending(self):
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    def test_process_status_pending_deleted(self):
        self.events.append({"type": 'DELETED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_failed(self):
        self.pod.status.phase = "Failed"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_succeeded(self):
        self.pod.status.phase = "Succeeded"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(None)

    def test_process_status_running_deleted(self):
        self.pod.status.phase = "Running"
        self.events.append({"type": 'DELETED', "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_running(self):
        self.pod.status.phase = "Running"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    def test_process_status_catchall(self):
        self.pod.status.phase = "Unknown"
        self.events.append({"type": 'MODIFIED', "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    @mock.patch.object(KubernetesJobWatcher, 'process_error')
    def test_process_error_event_for_410(self, mock_process_error):
        message = "too old resource version: 27272 (43334)"
        self.pod.status.phase = 'Pending'
        self.pod.metadata.resource_version = '0'
        mock_process_error.return_value = '0'
        raw_object = {"code": 410, "message": message}
        self.events.append({"type": "ERROR", "object": self.pod, "raw_object": raw_object})
        self._run()
        mock_process_error.assert_called_once_with(self.events[0])

    def test_process_error_event_for_raise_if_not_410(self):
        message = "Failure message"
        self.pod.status.phase = 'Pending'
        raw_object = {"code": 422, "message": message, "reason": "Test"}
        self.events.append({"type": "ERROR", "object": self.pod, "raw_object": raw_object})
        with self.assertRaises(AirflowException) as e:
            self._run()
        assert str(e.exception) == (
            f"Kubernetes failure for {raw_object['reason']} "
            f"with code {raw_object['code']} and message: {raw_object['message']}"
        )

    def test_recover_from_resource_too_old(self):
        # too old resource
        mock_underscore_run = mock.MagicMock()

        def effect():
            yield '500'
            while True:
                yield Exception('sentinel')

        mock_underscore_run.side_effect = effect()

        self.watcher._run = mock_underscore_run

        with mock.patch('airflow.executors.kubernetes_executor.get_kube_client'):
            try:
                # self.watcher._run() is mocked and return "500" as last resource_version
                self.watcher.run()
            except Exception as e:
                assert e.args == ('sentinel',)

            # both  resource_version should be 0 after _run raises and exception
            assert self.watcher.resource_version == '0'
            assert ResourceVersion().resource_version == '0'

            # check that in the next run, _run is invoked with resource_version = 0
            mock_underscore_run.reset_mock()
            try:
                self.watcher.run()
            except Exception as e:
                assert e.args == ('sentinel',)

            mock_underscore_run.assert_called_once_with(mock.ANY, '0', mock.ANY, mock.ANY)
