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
from __future__ import annotations

import random
import re
import string
from datetime import datetime, timedelta
from unittest import mock

import pytest
import yaml
from kubernetes.client import models as k8s
from kubernetes.client.rest import ApiException
from urllib3 import HTTPResponse

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes import pod_generator
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import (
    KubernetesExecutor,
    PodReconciliationError,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
    ADOPTED,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
    AirflowKubernetesScheduler,
    KubernetesJobWatcher,
    ResourceVersion,
    get_base_pod_from_template,
)
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    annotations_for_logging_task_metadata,
    annotations_to_key,
    create_unique_id,
    get_logs_task_metadata,
)
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
from airflow.utils import timezone
from airflow.utils.state import State, TaskInstanceState
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.skip_if_database_isolation_mode


class TestAirflowKubernetesScheduler:
    @staticmethod
    def _gen_random_string(seed, str_len):
        char_list = []
        for char_seed in range(str_len):
            random.seed(str(seed) * char_seed)
            char_list.append(random.choice(string.printable))
        return "".join(char_list)

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
        return len(name) <= 253 and name.islower() and re.match(regex, name)

    @staticmethod
    def _is_safe_label_value(value):
        regex = r"^[^a-z0-9A-Z]*|[^a-zA-Z0-9_\-\.]|[^a-z0-9A-Z]*$"
        return len(value) <= 63 and re.match(regex, value)

    def test_create_pod_id(self):
        for dag_id, task_id in self._cases():
            with pytest.warns(AirflowProviderDeprecationWarning, match=r"deprecated\. Use `add_pod_suffix`"):
                pod_name = PodGenerator.make_unique_pod_id(create_unique_id(dag_id, task_id))
            assert self._is_valid_pod_id(pod_name), f"dag_id={dag_id!r}, task_id={task_id!r}"

    @mock.patch("airflow.providers.cncf.kubernetes.pod_generator.PodGenerator")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubeConfig")
    def test_get_base_pod_from_template(self, mock_kubeconfig, mock_generator, data_file):
        # Provide non-existent file path,
        # so None will be passed to deserialize_model_dict().
        pod_template_file_path = "/bar/biz"
        get_base_pod_from_template(pod_template_file_path, None)
        assert "deserialize_model_dict" == mock_generator.mock_calls[0][0]
        assert mock_generator.mock_calls[0][1][0] is None

        mock_kubeconfig.pod_template_file = "/foo/bar"
        get_base_pod_from_template(None, mock_kubeconfig)
        assert "deserialize_model_dict" == mock_generator.mock_calls[1][0]
        assert mock_generator.mock_calls[1][1][0] is None

        # Provide existent file path,
        # so loaded YAML file content should be used to call deserialize_model_dict(), rather than None.
        pod_template_file = data_file("pods/template.yaml")
        with open(pod_template_file) as stream:
            expected_pod_dict = yaml.safe_load(stream)

        pod_template_file_path = pod_template_file.as_posix()
        get_base_pod_from_template(pod_template_file_path, None)
        assert "deserialize_model_dict" == mock_generator.mock_calls[2][0]
        assert mock_generator.mock_calls[2][1][0] == expected_pod_dict

        mock_kubeconfig.pod_template_file = pod_template_file.as_posix()
        get_base_pod_from_template(None, mock_kubeconfig)
        assert "deserialize_model_dict" == mock_generator.mock_calls[3][0]
        assert mock_generator.mock_calls[3][1][0] == expected_pod_dict

    def test_make_safe_label_value(self):
        for dag_id, task_id in self._cases():
            case = f"dag_id={dag_id!r}, task_id={task_id!r}"
            safe_dag_id = pod_generator.make_safe_label_value(dag_id)
            assert self._is_safe_label_value(safe_dag_id), case
            safe_task_id = pod_generator.make_safe_label_value(task_id)
            assert self._is_safe_label_value(safe_task_id), case

        dag_id = "my_dag_id"
        assert dag_id == pod_generator.make_safe_label_value(dag_id)
        dag_id = "my_dag_id_" + "a" * 64
        assert "my_dag_id_" + "a" * 43 + "-0ce114c45" == pod_generator.make_safe_label_value(dag_id)

    def test_execution_date_serialize_deserialize(self):
        datetime_obj = datetime.now()
        serialized_datetime = pod_generator.datetime_to_label_safe_datestring(datetime_obj)
        new_datetime_obj = pod_generator.label_safe_datestring_to_datetime(serialized_datetime)

        assert datetime_obj == new_datetime_obj

    @pytest.mark.db_test
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.client")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    def test_delete_pod_successfully(self, mock_watcher, mock_client, mock_kube_client):
        pod_name = "my-pod-1"
        namespace = "my-namespace-1"

        mock_delete_namespace = mock.MagicMock()
        mock_kube_client.return_value.delete_namespaced_pod = mock_delete_namespace

        kube_executor = KubernetesExecutor()
        kube_executor.job_id = 1
        kube_executor.start()
        try:
            kube_executor.kube_scheduler.delete_pod(pod_name, namespace)
            mock_delete_namespace.assert_called_with(pod_name, namespace, body=mock_client.V1DeleteOptions())
        finally:
            kube_executor.end()

    @pytest.mark.db_test
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.client")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    def test_delete_pod_raises_404(self, mock_watcher, mock_client, mock_kube_client):
        pod_name = "my-pod-1"
        namespace = "my-namespace-2"

        mock_delete_namespace = mock.MagicMock()
        mock_kube_client.return_value.delete_namespaced_pod = mock_delete_namespace

        # ApiException is raised because status is not 404
        mock_kube_client.return_value.delete_namespaced_pod.side_effect = ApiException(status=400)
        kube_executor = KubernetesExecutor()
        kube_executor.job_id = 1
        kube_executor.start()

        with pytest.raises(ApiException):
            kube_executor.kube_scheduler.delete_pod(pod_name, namespace)
        mock_delete_namespace.assert_called_with(pod_name, namespace, body=mock_client.V1DeleteOptions())

    @pytest.mark.db_test
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.client")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    def test_delete_pod_404_not_raised(self, mock_watcher, mock_client, mock_kube_client):
        pod_name = "my-pod-1"
        namespace = "my-namespace-3"

        mock_delete_namespace = mock.MagicMock()
        mock_kube_client.return_value.delete_namespaced_pod = mock_delete_namespace

        # ApiException not raised because the status is 404
        mock_kube_client.return_value.delete_namespaced_pod.side_effect = ApiException(status=404)
        kube_executor = KubernetesExecutor()
        kube_executor.job_id = 1
        kube_executor.start()
        try:
            kube_executor.kube_scheduler.delete_pod(pod_name, namespace)
            mock_delete_namespace.assert_called_with(pod_name, namespace, body=mock_client.V1DeleteOptions())
        finally:
            kube_executor.end()

    def test_running_pod_log_lines(self):
        # default behaviour
        kube_executor = KubernetesExecutor()
        assert kube_executor.RUNNING_POD_LOG_LINES == 100

        # monkey-patching for second executor
        kube_executor_2 = KubernetesExecutor()
        kube_executor_2.RUNNING_POD_LOG_LINES = 200

        # monkey-patching should not affect the class constant
        assert kube_executor.RUNNING_POD_LOG_LINES == 100
        assert kube_executor_2.RUNNING_POD_LOG_LINES == 200


class TestKubernetesExecutor:
    """
    Tests if an ApiException from the Kube Client will cause the task to
    be rescheduled.
    """

    def setup_method(self) -> None:
        self.kubernetes_executor = KubernetesExecutor()
        self.kubernetes_executor.job_id = 5

    @pytest.mark.db_test
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @pytest.mark.parametrize(
        "response, task_publish_max_retries, should_requeue, task_expected_state",
        [
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=400),
                0,
                False,
                State.FAILED,
                id="400 BadRequest",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=400),
                1,
                False,
                State.FAILED,
                id="400 BadRequest (task_publish_max_retries=1)",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=403),
                0,
                False,
                State.FAILED,
                id="403 Forbidden (permission denied)",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=403),
                1,
                False,
                State.FAILED,
                id="403 Forbidden (permission denied) (task_publish_max_retries=1)",
            ),
            pytest.param(
                HTTPResponse(
                    body='{"message": "pods pod1 is forbidden: exceeded quota: '
                    "resouece-quota, requested: pods=1, used: pods=10, "
                    'limited: pods=10"}',
                    status=403,
                ),
                0,
                False,
                State.FAILED,
                id="403 Forbidden (exceeded quota)",
            ),
            pytest.param(
                HTTPResponse(
                    body='{"message": "pods pod1 is forbidden: exceeded quota: '
                    "resouece-quota, requested: pods=1, used: pods=10, "
                    'limited: pods=10"}',
                    status=403,
                ),
                1,
                True,
                State.SUCCESS,
                id="403 Forbidden (exceeded quota) (task_publish_max_retries=1) (retry succeeded)",
            ),
            pytest.param(
                HTTPResponse(
                    body='{"message": "pods pod1 is forbidden: exceeded quota: '
                    "resouece-quota, requested: pods=1, used: pods=10, "
                    'limited: pods=10"}',
                    status=403,
                ),
                1,
                True,
                State.FAILED,
                id="403 Forbidden (exceeded quota) (task_publish_max_retries=1)  (retry failed)",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=404),
                0,
                False,
                State.FAILED,
                id="404 Not Found",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=404),
                1,
                False,
                State.FAILED,
                id="404 Not Found (task_publish_max_retries=1)",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=422),
                0,
                False,
                State.FAILED,
                id="422 Unprocessable Entity",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=422),
                1,
                False,
                State.FAILED,
                id="422 Unprocessable Entity (task_publish_max_retries=1)",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=12345),
                0,
                False,
                State.FAILED,
                id="12345 fake-unhandled-reason",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=12345),
                1,
                False,
                State.FAILED,
                id="12345 fake-unhandled-reason (task_publish_max_retries=1) (retry succeeded)",
            ),
            pytest.param(
                HTTPResponse(body='{"message": "any message"}', status=12345),
                1,
                False,
                State.FAILED,
                id="12345 fake-unhandled-reason (task_publish_max_retries=1) (retry failed)",
            ),
        ],
    )
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_run_next_exception_requeue(
        self,
        mock_get_kube_client,
        mock_kubernetes_job_watcher,
        response,
        task_publish_max_retries,
        should_requeue,
        task_expected_state,
        data_file,
    ):
        """
        When pod scheduling fails with any reason not yet
        handled in the relevant try-except block and task publish retries not exhausted, the task should stay
        in the ``task_queue`` and be attempted on a subsequent executor sync.
        When reason is 'Unprocessable Entity' or 'BadRequest' or task publish retries exhausted,
        the task should be failed without being re-queued.

        Note on error scenarios:

        - 400 BadRequest will returns in scenarios like
            - your request parameters are invalid e.g. asking for cpu=100ABC123.
        - 403 Forbidden will returns in scenarios like
            - your request exceeds the namespace quota
            - scheduler role doesn't have permission to launch the pod
        - 404 Not Found will returns in scenarios like
            - your requested namespace doesn't exists
        - 422 Unprocessable Entity will returns in scenarios like
            - your request parameters are valid but unsupported e.g. limits lower than requests.

        """
        template_file = data_file("pods/generator_base_with_secrets.yaml").as_posix()
        # A mock kube_client that throws errors when making a pod
        mock_kube_client = mock.patch("kubernetes.client.CoreV1Api", autospec=True)
        mock_kube_client.create_namespaced_pod = mock.MagicMock(side_effect=ApiException(http_resp=response))
        mock_get_kube_client.return_value = mock_kube_client
        mock_api_client = mock.MagicMock()
        mock_api_client.sanitize_for_serialization.return_value = {}
        mock_kube_client.api_client = mock_api_client
        config = {
            ("kubernetes", "pod_template_file"): template_file,
        }
        with conf_vars(config):
            kubernetes_executor = self.kubernetes_executor
            kubernetes_executor.task_publish_max_retries = task_publish_max_retries
            kubernetes_executor.start()
            try:
                # Execute a task while the Api Throws errors
                try_number = 1
                task_instance_key = TaskInstanceKey("dag", "task", "run_id", try_number)
                kubernetes_executor.execute_async(
                    key=task_instance_key,
                    queue=None,
                    command=["airflow", "tasks", "run", "true", "some_parameter"],
                )
                kubernetes_executor.sync()

                assert mock_kube_client.create_namespaced_pod.call_count == 1

                if should_requeue:
                    assert not kubernetes_executor.task_queue.empty()

                    # Disable the ApiException
                    if task_expected_state == State.SUCCESS:
                        mock_kube_client.create_namespaced_pod.side_effect = None

                    # Execute the task without errors should empty the queue
                    mock_kube_client.create_namespaced_pod.reset_mock()
                    kubernetes_executor.sync()
                    assert mock_kube_client.create_namespaced_pod.called
                    assert kubernetes_executor.task_queue.empty()
                    if task_expected_state != State.SUCCESS:
                        assert kubernetes_executor.event_buffer[task_instance_key][0] == task_expected_state
                else:
                    assert kubernetes_executor.task_queue.empty()
                    assert kubernetes_executor.event_buffer[task_instance_key][0] == task_expected_state
            finally:
                kubernetes_executor.end()

    @pytest.mark.db_test
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @mock.patch("airflow.settings.pod_mutation_hook")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_run_next_pmh_error(self, mock_get_kube_client, mock_pmh):
        """
        Exception during Pod Mutation Hook execution should be handled gracefully.
        """
        exception_in_pmh = Exception("Purposely generate error for test")
        mock_pmh.side_effect = exception_in_pmh

        mock_kube_client = mock.patch("kubernetes.client.CoreV1Api", autospec=True)
        mock_kube_client.create_namespaced_pod = mock.MagicMock()
        mock_get_kube_client.return_value = mock_kube_client

        kubernetes_executor = self.kubernetes_executor
        kubernetes_executor.start()
        try:
            try_number = 1
            task_instance_key = TaskInstanceKey("dag", "task", "run_id", try_number)
            kubernetes_executor.execute_async(
                key=task_instance_key,
                queue=None,
                command=["airflow", "tasks", "run", "true", "some_parameter"],
            )
            kubernetes_executor.sync()

            # The pod_mutation_hook should have been called once.
            assert mock_pmh.call_count == 1
            # There should be no pod creation request sent
            assert mock_kube_client.create_namespaced_pod.call_count == 0
            # The task is not re-queued and there is the failed record in event_buffer
            assert kubernetes_executor.task_queue.empty()
            assert kubernetes_executor.event_buffer[task_instance_key][0] == State.FAILED
            assert kubernetes_executor.event_buffer[task_instance_key][1].__cause__ == exception_in_pmh
        finally:
            kubernetes_executor.end()

    @pytest.mark.db_test
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_run_next_pod_reconciliation_error(
        self, mock_get_kube_client, mock_kubernetes_job_watcher, data_file
    ):
        """
        When construct_pod raises PodReconciliationError, we should fail the task.
        """
        template_file = data_file("pods/generator_base_with_secrets.yaml").as_posix()

        mock_kube_client = mock.patch("kubernetes.client.CoreV1Api", autospec=True)
        fail_msg = "test message"
        mock_kube_client.create_namespaced_pod = mock.MagicMock(side_effect=PodReconciliationError(fail_msg))
        mock_get_kube_client.return_value = mock_kube_client
        mock_api_client = mock.MagicMock()
        mock_api_client.sanitize_for_serialization.return_value = {}
        mock_kube_client.api_client = mock_api_client
        config = {("kubernetes", "pod_template_file"): template_file}
        with conf_vars(config):
            kubernetes_executor = self.kubernetes_executor
            kubernetes_executor.start()
            try:
                # Execute a task while the Api Throws errors
                try_number = 1
                task_instance_key = TaskInstanceKey("dag", "task", "run_id", try_number)
                kubernetes_executor.execute_async(
                    key=task_instance_key,
                    queue=None,
                    command=["airflow", "tasks", "run", "true", "some_parameter"],
                )
                kubernetes_executor.sync()

                assert kubernetes_executor.task_queue.empty()
                assert kubernetes_executor.event_buffer[task_instance_key][0] == State.FAILED
                assert kubernetes_executor.event_buffer[task_instance_key][1].args[0] == fail_msg
            finally:
                kubernetes_executor.end()

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubeConfig")
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.sync")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
    @mock.patch("airflow.executors.base_executor.Stats.gauge")
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync, mock_kube_config):
        executor = self.kubernetes_executor
        executor.heartbeat()
        calls = [
            mock.call(
                "executor.open_slots", value=mock.ANY, tags={"status": "open", "name": "KubernetesExecutor"}
            ),
            mock.call(
                "executor.queued_tasks",
                value=mock.ANY,
                tags={"status": "queued", "name": "KubernetesExecutor"},
            ),
            mock.call(
                "executor.running_tasks",
                value=mock.ANY,
                tags={"status": "running", "name": "KubernetesExecutor"},
            ),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_invalid_executor_config(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        try:
            assert executor.event_buffer == {}
            executor.execute_async(
                key=("dag", "task", timezone.utcnow(), 1),
                queue=None,
                command=["airflow", "tasks", "run", "true", "some_parameter"],
                executor_config=k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[k8s.V1Container(name="base", image="myimage", image_pull_policy="Always")]
                    )
                ),
            )

            assert next(iter(executor.event_buffer.values()))[1] == "Invalid executor_config passed"
        finally:
            executor.end()

    @pytest.mark.db_test
    @pytest.mark.execution_timeout(10)
    @pytest.mark.skipif(
        AirflowKubernetesScheduler is None, reason="kubernetes python package is not installed"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler.run_pod_async"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_pod_template_file_override_in_executor_config(
        self, mock_get_kube_client, mock_run_pod_async, data_file
    ):
        executor_template_file = data_file("executor/basic_template.yaml")
        mock_kube_client = mock.patch("kubernetes.client.CoreV1Api", autospec=True)
        mock_get_kube_client.return_value = mock_kube_client

        with conf_vars({("kubernetes", "pod_template_file"): None}):
            executor = self.kubernetes_executor
            executor.start()
            try:
                assert executor.event_buffer == {}
                assert executor.task_queue.empty()

                executor.execute_async(
                    key=TaskInstanceKey("dag", "task", "run_id", 1),
                    queue=None,
                    command=["airflow", "tasks", "run", "true", "some_parameter"],
                    executor_config={
                        "pod_template_file": executor_template_file,
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
                executor.task_queue.task_done()
                # Test that the correct values have been put to queue
                assert expected_executor_config.metadata.labels == {"release": "stable"}
                assert expected_pod_template_file == executor_template_file

                self.kubernetes_executor.kube_scheduler.run_next(task)
                mock_run_pod_async.assert_called_once_with(
                    k8s.V1Pod(
                        api_version="v1",
                        kind="Pod",
                        metadata=k8s.V1ObjectMeta(
                            name=mock.ANY,
                            namespace="default",
                            annotations={
                                "dag_id": "dag",
                                "run_id": "run_id",
                                "task_id": "task",
                                "try_number": "1",
                            },
                            labels={
                                "airflow-worker": "5",
                                "airflow_version": mock.ANY,
                                "dag_id": "dag",
                                "run_id": "run_id",
                                "kubernetes_executor": "True",
                                "mylabel": "foo",
                                "release": "stable",
                                "task_id": "task",
                                "try_number": "1",
                            },
                        ),
                        spec=k8s.V1PodSpec(
                            containers=[
                                k8s.V1Container(
                                    name="base",
                                    image="airflow:3.6",
                                    args=["airflow", "tasks", "run", "true", "some_parameter"],
                                    env=[k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value="True")],
                                )
                            ],
                            image_pull_secrets=[
                                k8s.V1LocalObjectReference(name="all-right-then-keep-your-secrets")
                            ],
                            scheduler_name="default-scheduler",
                            security_context=k8s.V1PodSecurityContext(fs_group=50000, run_as_user=50001),
                        ),
                    )
                )
            finally:
                executor.end()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_change_state_running(self, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        try:
            key = ("dag_id", "task_id", "run_id", "try_number1")
            executor.running = {key}
            executor._change_state(key, State.RUNNING, "pod_name", "default")
            assert executor.event_buffer[key][0] == State.RUNNING
            assert executor.running == {key}
        finally:
            executor.end()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler.delete_pod"
    )
    def test_change_state_success(self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        try:
            key = ("dag_id", "task_id", "run_id", "try_number2")
            executor.running = {key}
            executor._change_state(key, State.SUCCESS, "pod_name", "default")
            assert executor.event_buffer[key][0] == State.SUCCESS
            assert executor.running == set()
            mock_delete_pod.assert_called_once_with(pod_name="pod_name", namespace="default")
        finally:
            executor.end()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler"
    )
    def test_change_state_failed_no_deletion(
        self, mock_kubescheduler, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        mock_delete_pod = mock_kubescheduler.return_value.delete_pod
        mock_patch_pod = mock_kubescheduler.return_value.patch_pod_executor_done
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False
        executor.start()
        try:
            key = ("dag_id", "task_id", "run_id", "try_number3")
            executor.running = {key}
            executor._change_state(key, State.FAILED, "pod_id", "test-namespace")
            assert executor.event_buffer[key][0] == State.FAILED
            assert executor.running == set()
            mock_delete_pod.assert_not_called()
            mock_patch_pod.assert_called_once_with(pod_name="pod_id", namespace="test-namespace")
        finally:
            executor.end()

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "ti_state", [TaskInstanceState.SUCCESS, TaskInstanceState.FAILED, TaskInstanceState.DEFERRED]
    )
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler.delete_pod"
    )
    def test_change_state_none(
        self,
        mock_delete_pod,
        mock_get_kube_client,
        mock_kubernetes_job_watcher,
        ti_state,
        create_task_instance,
    ):
        """Ensure that when change_state gets state=None, it looks up the TI state from the db"""
        executor = self.kubernetes_executor
        executor.start()
        try:
            ti = create_task_instance(state=ti_state)
            key = ti.key
            executor.running = {key}
            executor._change_state(key, None, "pod_name", "default")
            assert executor.event_buffer[key][0] == ti_state
            assert executor.running == set()
            mock_delete_pod.assert_called_once_with(pod_name="pod_name", namespace="default")
        finally:
            executor.end()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler.delete_pod"
    )
    def test_change_state_adopted(self, mock_delete_pod, mock_get_kube_client, mock_kubernetes_job_watcher):
        executor = self.kubernetes_executor
        executor.start()
        try:
            key = ("dag_id", "task_id", "run_id", "try_number2")
            executor.running = {key}
            executor._change_state(key, ADOPTED, "pod_name", "default")
            assert len(executor.event_buffer) == 0
            assert len(executor.running) == 0
            mock_delete_pod.assert_not_called()
        finally:
            executor.end()

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "multi_namespace_mode_namespace_list, watchers_keys",
        [
            pytest.param(["A", "B", "C"], ["A", "B", "C"]),
            pytest.param(None, ["ALL_NAMESPACES"]),
        ],
    )
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_watchers_under_multi_namespace_mode(
        self, mock_get_kube_client, multi_namespace_mode_namespace_list, watchers_keys
    ):
        executor = self.kubernetes_executor
        executor.kube_config.multi_namespace_mode = True
        executor.kube_config.multi_namespace_mode_namespace_list = multi_namespace_mode_namespace_list
        executor.start()
        try:
            assert list(executor.kube_scheduler.kube_watchers.keys()) == watchers_keys
            assert all(
                isinstance(v, KubernetesJobWatcher) for v in executor.kube_scheduler.kube_watchers.values()
            )
        finally:
            executor.end()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler"
    )
    def test_change_state_skip_pod_deletion(
        self, mock_kubescheduler, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        mock_delete_pod = mock_kubescheduler.return_value.delete_pod
        mock_patch_pod = mock_kubescheduler.return_value.patch_pod_executor_done
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods = False
        executor.kube_config.delete_worker_pods_on_failure = False

        executor.start()
        try:
            key = ("dag_id", "task_id", "run_id", "try_number2")
            executor.running = {key}
            executor._change_state(key, State.SUCCESS, "pod_name", "test-namespace")
            assert executor.event_buffer[key][0] == State.SUCCESS
            assert executor.running == set()
            mock_delete_pod.assert_not_called()
            mock_patch_pod.assert_called_once_with(pod_name="pod_name", namespace="test-namespace")
        finally:
            executor.end()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler"
    )
    def test_change_state_failed_pod_deletion(
        self, mock_kubescheduler, mock_get_kube_client, mock_kubernetes_job_watcher
    ):
        mock_delete_pod = mock_kubescheduler.return_value.delete_pod
        mock_patch_pod = mock_kubescheduler.return_value.patch_pod_executor_done
        executor = self.kubernetes_executor
        executor.kube_config.delete_worker_pods_on_failure = True

        executor.start()
        try:
            key = ("dag_id", "task_id", "run_id", "try_number2")
            executor.running = {key}
            executor._change_state(key, State.FAILED, "pod_name", "test-namespace")
            assert executor.event_buffer[key][0] == State.FAILED
            assert executor.running == set()
            mock_delete_pod.assert_called_once_with(pod_name="pod_name", namespace="test-namespace")
            mock_patch_pod.assert_not_called()
        finally:
            executor.end()

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.adopt_launched_task"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods"
    )
    def test_try_adopt_task_instances(
        self, mock_adopt_completed_pods, mock_adopt_launched_task, mock_kube_dynamic_client
    ):
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "10"
        ti_key = annotations_to_key(
            {
                "dag_id": "dag",
                "run_id": "run_id",
                "task_id": "task",
                "try_number": "1",
            }
        )
        mock_ti = mock.MagicMock(queued_by_job_id="1", external_executor_id="1", key=ti_key)
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="foo"))
        mock_kube_client = mock.MagicMock()
        executor.kube_client = mock_kube_client
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_pod_resource = mock.MagicMock()
        mock_kube_dynamic_client.return_value.resources.get.return_value = mock_pod_resource
        mock_kube_dynamic_client.return_value.get.return_value.items = [pod]

        # First adoption
        reset_tis = executor.try_adopt_task_instances([mock_ti])
        mock_kube_dynamic_client.return_value.get.assert_called_once_with(
            resource=mock_pod_resource,
            namespace="default",
            field_selector="status.phase!=Succeeded",
            label_selector="kubernetes_executor=True,airflow-worker=1,airflow_executor_done!=True",
            header_params={"Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"},
        )
        mock_adopt_launched_task.assert_called_once_with(mock_kube_client, pod, {ti_key: mock_ti})
        mock_adopt_completed_pods.assert_called_once()
        assert reset_tis == [mock_ti]  # assume failure adopting when checking return

        # Second adoption (queued_by_job_id and external_executor_id no longer match)
        mock_kube_dynamic_client.return_value.reset_mock()
        mock_adopt_launched_task.reset_mock()
        mock_adopt_completed_pods.reset_mock()

        mock_ti.queued_by_job_id = "10"  # scheduler_job would have updated this after the first adoption
        executor.scheduler_job_id = "20"
        # assume success adopting, `adopt_launched_task` pops `ti_key` from `tis_to_flush_by_key`
        mock_adopt_launched_task.side_effect = (
            lambda client, pod, tis_to_flush_by_key: tis_to_flush_by_key.pop(ti_key)
        )

        reset_tis = executor.try_adopt_task_instances([mock_ti])
        mock_kube_dynamic_client.return_value.get.assert_called_once_with(
            resource=mock_pod_resource,
            namespace="default",
            field_selector="status.phase!=Succeeded",
            label_selector="kubernetes_executor=True,airflow-worker=10,airflow_executor_done!=True",
            header_params={"Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"},
        )
        mock_adopt_launched_task.assert_called_once()  # Won't check args this time around as they get mutated
        mock_adopt_completed_pods.assert_called_once()
        assert reset_tis == []  # This time our return is empty - no TIs to reset

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods"
    )
    def test_try_adopt_task_instances_multiple_scheduler_ids(
        self, mock_adopt_completed_pods, mock_kube_dynamic_client
    ):
        """We try to find pods only once per scheduler id"""
        executor = self.kubernetes_executor
        mock_kube_client = mock.MagicMock()
        executor.kube_client = mock_kube_client
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_pod_resource = mock.MagicMock()
        mock_kube_dynamic_client.return_value.resources.get.return_value = mock_pod_resource

        mock_tis = [
            mock.MagicMock(queued_by_job_id="10", external_executor_id="1", dag_id="dag", task_id="task"),
            mock.MagicMock(queued_by_job_id="40", external_executor_id="1", dag_id="dag", task_id="task2"),
            mock.MagicMock(queued_by_job_id="40", external_executor_id="1", dag_id="dag", task_id="task3"),
        ]

        executor.try_adopt_task_instances(mock_tis)
        assert mock_kube_dynamic_client.return_value.get.call_count == 2
        mock_kube_dynamic_client.return_value.get.assert_has_calls(
            [
                mock.call(
                    resource=mock_pod_resource,
                    namespace="default",
                    field_selector="status.phase!=Succeeded",
                    label_selector="kubernetes_executor=True,airflow-worker=10,airflow_executor_done!=True",
                    header_params={
                        "Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"
                    },
                ),
                mock.call(
                    resource=mock_pod_resource,
                    namespace="default",
                    field_selector="status.phase!=Succeeded",
                    label_selector="kubernetes_executor=True,airflow-worker=40,airflow_executor_done!=True",
                    header_params={
                        "Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"
                    },
                ),
            ],
            any_order=True,
        )

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.adopt_launched_task"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods"
    )
    def test_try_adopt_task_instances_no_matching_pods(
        self, mock_adopt_completed_pods, mock_adopt_launched_task, mock_kube_dynamic_client
    ):
        executor = self.kubernetes_executor
        mock_ti = mock.MagicMock(queued_by_job_id="1", external_executor_id="1", dag_id="dag", task_id="task")
        mock_kube_client = mock.MagicMock()
        executor.kube_client = mock_kube_client
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_kube_dynamic_client.return_value.get.return_value.items = []

        tis_to_flush = executor.try_adopt_task_instances([mock_ti])
        assert tis_to_flush == [mock_ti]
        assert executor.running == set()
        mock_adopt_launched_task.assert_not_called()
        mock_adopt_completed_pods.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.adopt_launched_task"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor._adopt_completed_pods"
    )
    def test_try_adopt_already_adopted_task_instances(
        self, mock_adopt_completed_pods, mock_adopt_launched_task, mock_kube_dynamic_client
    ):
        """For TIs that are already adopted, we should not flush them"""
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_kube_dynamic_client.return_value.get.return_value.items = []
        mock_kube_client = mock.MagicMock()
        executor = self.kubernetes_executor
        executor.kube_client = mock_kube_client
        ti_key = TaskInstanceKey("dag", "task", "run_id", 1)
        mock_ti = mock.MagicMock(queued_by_job_id="1", external_executor_id="1", key=ti_key)
        executor.running = {ti_key}

        tis_to_flush = executor.try_adopt_task_instances([mock_ti])
        mock_adopt_launched_task.assert_not_called()
        mock_adopt_completed_pods.assert_called_once()
        assert tis_to_flush == []
        assert executor.running == {ti_key}

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_adopt_launched_task(self, mock_kube_client):
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        annotations = {
            "dag_id": "dag",
            "run_id": "run_id",
            "task_id": "task",
            "try_number": "1",
        }
        ti_key = annotations_to_key(annotations)
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="foo", labels={"airflow-worker": "bar"}, annotations=annotations)
        )
        tis_to_flush_by_key = {ti_key: {}}

        executor.adopt_launched_task(mock_kube_client, pod=pod, tis_to_flush_by_key=tis_to_flush_by_key)
        mock_kube_client.patch_namespaced_pod.assert_called_once_with(
            body={"metadata": {"labels": {"airflow-worker": "modified"}}},
            name="foo",
            namespace=None,
        )
        assert tis_to_flush_by_key == {}
        assert executor.running == {ti_key}

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_adopt_launched_task_api_exception(self, mock_kube_client):
        """We shouldn't think we are running the task if aren't able to patch the pod"""
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        annotations = {
            "dag_id": "dag",
            "run_id": "run_id",
            "task_id": "task",
            "try_number": "1",
        }
        ti_key = annotations_to_key(annotations)
        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="foo", annotations=annotations))
        tis_to_flush_by_key = {ti_key: {}}

        mock_kube_client.patch_namespaced_pod.side_effect = ApiException(status=400)
        executor.adopt_launched_task(mock_kube_client, pod=pod, tis_to_flush_by_key=tis_to_flush_by_key)
        mock_kube_client.patch_namespaced_pod.assert_called_once_with(
            body={"metadata": {"labels": {"airflow-worker": "modified"}}},
            name="foo",
            namespace=None,
        )
        assert tis_to_flush_by_key == {ti_key: {}}
        assert executor.running == set()

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_adopt_completed_pods(self, mock_kube_client, mock_kube_dynamic_client):
        """We should adopt all completed pods from other schedulers"""
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        executor.kube_client = mock_kube_client
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_pod_resource = mock.MagicMock()
        mock_kube_dynamic_client.return_value.resources.get.return_value = mock_pod_resource
        mock_kube_dynamic_client.return_value.get.return_value.items = []
        executor.kube_config.kube_namespace = "somens"
        pod_names = ["one", "two"]

        def get_annotations(pod_name):
            return {
                "dag_id": "dag",
                "run_id": "run_id",
                "task_id": pod_name,
                "try_number": "1",
            }

        mock_kube_dynamic_client.return_value.get.return_value.items = [
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name=pod_name,
                    labels={"airflow-worker": pod_name},
                    annotations=get_annotations(pod_name),
                    namespace="somens",
                )
            )
            for pod_name in pod_names
        ]
        expected_running_ti_keys = {annotations_to_key(get_annotations(pod_name)) for pod_name in pod_names}

        executor._adopt_completed_pods(mock_kube_client)
        mock_kube_dynamic_client.return_value.get.assert_called_once_with(
            resource=mock_pod_resource,
            namespace="somens",
            field_selector="status.phase=Succeeded",
            label_selector="kubernetes_executor=True,airflow-worker!=modified,airflow_executor_done!=True",
            header_params={"Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"},
        )
        assert len(pod_names) == mock_kube_client.patch_namespaced_pod.call_count
        mock_kube_client.patch_namespaced_pod.assert_has_calls(
            [
                mock.call(
                    body={"metadata": {"labels": {"airflow-worker": "modified"}}},
                    name=pod_name,
                    namespace="somens",
                )
                for pod_name in pod_names
            ],
            any_order=True,
        )
        assert executor.running == expected_running_ti_keys

    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_adopt_completed_pods_api_exception(self, mock_kube_client, mock_kube_dynamic_client):
        """We should gracefully handle exceptions when adopting completed pods from other schedulers"""
        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        executor.kube_client = mock_kube_client
        executor.kube_config.kube_namespace = "somens"
        pod_names = ["one", "two"]

        def get_annotations(pod_name):
            return {
                "dag_id": "dag",
                "run_id": "run_id",
                "task_id": pod_name,
                "try_number": "1",
            }

        mock_kube_dynamic_client.return_value.get.return_value.items = [
            k8s.V1Pod(
                metadata=k8s.V1ObjectMeta(
                    name=pod_name,
                    labels={"airflow-worker": pod_name},
                    annotations=get_annotations(pod_name),
                    namespace="somens",
                )
            )
            for pod_name in pod_names
        ]

        mock_kube_client.patch_namespaced_pod.side_effect = ApiException(status=400)
        executor._adopt_completed_pods(mock_kube_client)
        assert len(pod_names) == mock_kube_client.patch_namespaced_pod.call_count
        assert executor.running == set()

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_not_adopt_unassigned_task(self, mock_kube_client):
        """
        We should not adopt any tasks that were not assigned by the scheduler.
        This ensures that there is no contention over pod management.
        """

        executor = self.kubernetes_executor
        executor.scheduler_job_id = "modified"
        tis_to_flush_by_key = {"foobar": {}}
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo",
                labels={"airflow-worker": "bar"},
                annotations={
                    "dag_id": "dag",
                    "run_id": "run_id",
                    "task_id": "task",
                    "try_number": "1",
                },
            )
        )
        executor.adopt_launched_task(mock_kube_client, pod=pod, tis_to_flush_by_key=tis_to_flush_by_key)
        assert not mock_kube_client.patch_namespaced_pod.called
        assert tis_to_flush_by_key == {"foobar": {}}

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.AirflowKubernetesScheduler.delete_pod"
    )
    def test_cleanup_stuck_queued_tasks(self, mock_delete_pod, mock_kube_client, dag_maker, session):
        """Delete any pods associated with a task stuck in queued."""
        executor = KubernetesExecutor()
        executor.start()
        executor.scheduler_job_id = "123"
        with dag_maker(dag_id="test_cleanup_stuck_queued_tasks"):
            op = BashOperator(task_id="bash", bash_command=["echo 0", "echo 1"])
        dag_run = dag_maker.create_dagrun()
        ti = dag_run.get_task_instance(op.task_id, session)
        ti.retries = 1
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow() - timedelta(minutes=30)
        ti.refresh_from_db()
        tis = [ti]
        executor.cleanup_stuck_queued_tasks(tis)
        mock_delete_pod.assert_called_once()
        assert executor.running == set()
        executor.end()

    @pytest.mark.parametrize(
        "raw_multi_namespace_mode, raw_value_namespace_list, expected_value_in_kube_config",
        [
            pytest.param("true", "A,B,C", ["A", "B", "C"]),
            pytest.param("true", "", None),
            pytest.param("false", "A,B,C", None),
            pytest.param("false", "", None),
        ],
    )
    def test_kube_config_get_namespace_list(
        self, raw_multi_namespace_mode, raw_value_namespace_list, expected_value_in_kube_config
    ):
        config = {
            ("kubernetes", "multi_namespace_mode"): raw_multi_namespace_mode,
            ("kubernetes", "multi_namespace_mode_namespace_list"): raw_value_namespace_list,
        }
        with conf_vars(config):
            executor = KubernetesExecutor()

        assert executor.kube_config.multi_namespace_mode_namespace_list == expected_value_in_kube_config

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    def test_clear_not_launched_queued_tasks_not_launched(
        self, mock_kube_dynamic_client, dag_maker, create_dummy_dag, session
    ):
        """If a pod isn't found for a TI, reset the state to scheduled"""
        mock_kube_client = mock.MagicMock()
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_pod_resource = mock.MagicMock()
        mock_kube_dynamic_client.return_value.resources.get.return_value = mock_pod_resource
        mock_kube_dynamic_client.return_value.get.return_value.items = []

        create_dummy_dag(dag_id="test_clear", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_by_job_id = 1
        session.flush()

        executor = self.kubernetes_executor
        executor.job_id = 1
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti.refresh_from_db()
        assert ti.state == State.SCHEDULED
        assert mock_kube_dynamic_client.return_value.get.call_count == 1
        mock_kube_dynamic_client.return_value.get.assert_called_with(
            resource=mock_pod_resource,
            namespace="default",
            label_selector="airflow-worker=1",
            header_params={"Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"},
        )

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "task_queue, kubernetes_queue",
        [
            pytest.param("default", None),
            pytest.param("kubernetes", None),
            pytest.param("kubernetes", "kubernetes"),
        ],
    )
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    def test_clear_not_launched_queued_tasks_launched(
        self, mock_kube_dynamic_client, dag_maker, create_dummy_dag, session, task_queue, kubernetes_queue
    ):
        """Leave the state alone if a pod already exists"""
        mock_kube_client = mock.MagicMock()
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_pod_resource = mock.MagicMock()
        mock_kube_dynamic_client.return_value.resources.get.return_value = mock_pod_resource
        mock_kube_dynamic_client.return_value.get.return_value = k8s.V1PodList(
            items=[
                k8s.V1Pod(
                    metadata=k8s.V1ObjectMeta(
                        labels={
                            "role": "airflow-worker",
                            "dag_id": "test_clear",
                            "task_id": "task1",
                            "airflow-worker": 1,
                            "run_id": "test",
                        },
                    ),
                    status=k8s.V1PodStatus(phase="Pending"),
                )
            ]
        )

        create_dummy_dag(dag_id="test_clear", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_by_job_id = 1
        ti.queue = task_queue
        session.flush()

        executor = self.kubernetes_executor
        executor.job_id = 1
        executor.kubernetes_queue = kubernetes_queue
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti.refresh_from_db()
        assert ti.state == State.QUEUED
        mock_kube_dynamic_client.return_value.get.assert_called_once_with(
            resource=mock_pod_resource,
            namespace="default",
            label_selector="airflow-worker=1",
            header_params={"Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"},
        )

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    def test_clear_not_launched_queued_tasks_mapped_task(self, mock_kube_dynamic_client, dag_maker, session):
        """One mapped task has a launched pod - other does not."""

        def get(*args, **kwargs):
            return k8s.V1PodList(
                items=[
                    k8s.V1Pod(
                        metadata=k8s.V1ObjectMeta(
                            labels={
                                "role": "airflow-worker",
                                "dag_id": "test_clear",
                                "task_id": "bash",
                                "airflow-worker": 1,
                                "map_index": 0,
                                "run_id": "test",
                            },
                        ),
                        status=k8s.V1PodStatus(phase="Pending"),
                    )
                ]
            )

        mock_kube_client = mock.MagicMock()
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_pod_resource = mock.MagicMock()
        mock_kube_dynamic_client.return_value.resources.get.return_value = mock_pod_resource
        mock_kube_dynamic_client.return_value.get.side_effect = get

        with dag_maker(dag_id="test_clear"):
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
        executor.job_id = 1
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti0.refresh_from_db()
        ti1.refresh_from_db()
        assert ti0.state == State.QUEUED
        assert ti1.state == State.SCHEDULED

        assert mock_kube_dynamic_client.return_value.get.call_count == 1
        mock_kube_dynamic_client.return_value.get.assert_called_with(
            resource=mock_pod_resource,
            namespace="default",
            label_selector="airflow-worker=1",
            header_params={"Accept": "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io"},
        )

    @pytest.mark.db_test
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
        executor.job_id = 1
        executor.kubernetes_queue = "kubernetes"

        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti.refresh_from_db()
        assert ti.state == State.QUEUED
        assert mock_kube_client.list_namespaced_pod.call_count == 0

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.DynamicClient")
    def test_clear_not_launched_queued_tasks_clear_only_by_job_id(
        self, mock_kube_dynamic_client, dag_maker, create_dummy_dag, session
    ):
        """clear only not launched queued  tasks which are queued by the same executor job"""
        mock_kube_client = mock.MagicMock()
        mock_kube_dynamic_client.return_value = mock.MagicMock()
        mock_kube_dynamic_client.return_value.get.return_value = k8s.V1PodList(items=[])

        create_dummy_dag(dag_id="test_clear_0", task_id="task0", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti0 = dag_run.task_instances[0]
        ti0.state = State.QUEUED
        ti0.queued_by_job_id = 1
        session.flush()

        create_dummy_dag(dag_id="test_clear_1", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti1 = dag_run.task_instances[0]
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = 2
        session.flush()

        executor = self.kubernetes_executor
        executor.job_id = 1
        executor.kube_client = mock_kube_client
        executor.clear_not_launched_queued_tasks(session=session)

        ti0.refresh_from_db()
        ti1.refresh_from_db()
        assert ti0.state == State.SCHEDULED
        assert ti1.state == State.QUEUED

    @pytest.mark.db_test
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client")
    def test_get_task_log(self, mock_get_kube_client, create_task_instance_of_operator):
        """fetch task log from pod"""
        mock_kube_client = mock_get_kube_client.return_value

        mock_kube_client.read_namespaced_pod_log.return_value = [b"a_", b"b_", b"c_"]
        mock_pod = mock.Mock()
        mock_pod.metadata.name = "x"
        mock_kube_client.list_namespaced_pod.return_value.items = [mock_pod]
        ti = create_task_instance_of_operator(EmptyOperator, dag_id="test_k8s_log_dag", task_id="test_task")

        executor = KubernetesExecutor()
        messages, logs = executor.get_task_log(ti=ti, try_number=1)

        mock_kube_client.read_namespaced_pod_log.assert_called_once()
        assert messages == [
            "Attempting to fetch logs from pod  through kube API",
            "Found logs through kube API",
        ]
        assert logs[0] == "a_\nb_\nc_"

        mock_kube_client.reset_mock()
        mock_kube_client.read_namespaced_pod_log.side_effect = Exception("error_fetching_pod_log")

        messages, logs = executor.get_task_log(ti=ti, try_number=1)
        assert logs == [""]
        assert messages == [
            "Attempting to fetch logs from pod  through kube API",
            "Reading from k8s pod logs failed: error_fetching_pod_log",
        ]

    def test_supports_pickling(self):
        assert KubernetesExecutor.supports_pickling

    def test_supports_sentry(self):
        assert not KubernetesExecutor.supports_sentry

    def test_cli_commands_vended(self):
        assert KubernetesExecutor.get_cli_commands()

    def test_annotations_for_logging_task_metadata(self):
        annotations_test = {
            "dag_id": "dag",
            "run_id": "run_id",
            "task_id": "task",
            "try_number": "1",
        }
        get_logs_task_metadata.cache_clear()
        try:
            with conf_vars({("kubernetes", "logs_task_metadata"): "True"}):
                expected_annotations = {
                    "dag_id": "dag",
                    "run_id": "run_id",
                    "task_id": "task",
                    "try_number": "1",
                }
                annotations_actual = annotations_for_logging_task_metadata(annotations_test)
                assert annotations_actual == expected_annotations
        finally:
            get_logs_task_metadata.cache_clear()

    def test_annotations_for_logging_task_metadata_fallback(self):
        annotations_test = {
            "dag_id": "dag",
            "run_id": "run_id",
            "task_id": "task",
            "try_number": "1",
        }
        get_logs_task_metadata.cache_clear()
        try:
            with conf_vars({("kubernetes", "logs_task_metadata"): "False"}):
                expected_annotations = "<omitted>"
                annotations_actual = annotations_for_logging_task_metadata(annotations_test)
                assert annotations_actual == expected_annotations
        finally:
            get_logs_task_metadata.cache_clear()


class TestKubernetesJobWatcher:
    test_namespace = "airflow"

    def setup_method(self):
        self.watcher = KubernetesJobWatcher(
            namespace=self.test_namespace,
            watcher_queue=mock.MagicMock(),
            resource_version="0",
            scheduler_job_id="123",
            kube_config=mock.MagicMock(),
        )
        self.watcher.kube_config.worker_pod_pending_fatal_container_state_reasons = [
            "CreateContainerConfigError",
            "CrashLoopBackOff",
            "ErrImagePull",
            "CreateContainerError",
            "ImageInspectError",
            "InvalidImageName",
        ]
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
                labels={},
            ),
            status=k8s.V1PodStatus(phase="Pending"),
        )
        self.events = []

    def _run(self):
        with mock.patch(
            "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.watch"
        ) as mock_watch, mock.patch.object(
            KubernetesJobWatcher,
            "_pod_events",
        ) as mock_pod_events:
            mock_watch.Watch.return_value.stream.return_value = self.events
            mock_pod_events.return_value = self.events
            latest_resource_version = self.watcher._run(
                self.kube_client,
                self.watcher.resource_version,
                self.watcher.scheduler_job_id,
                self.watcher.kube_config,
            )
            assert self.pod.metadata.resource_version == latest_resource_version
            mock_pod_events.assert_called_once_with(
                kube_client=self.kube_client,
                query_kwargs={
                    "label_selector": "airflow-worker=123,airflow_executor_done!=True",
                    "resource_version": "0",
                    "_request_timeout": 30,
                    "timeout_seconds": 3600,
                },
            )

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

    @pytest.mark.parametrize(
        "raw_object, is_watcher_queue_called",
        [
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {
                                    "waiting": {
                                        "reason": "CreateContainerConfigError",
                                        "message": 'secret "my-secret" not found',
                                    }
                                },
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:latest",
                                "imageID": "",
                            }
                        ],
                    }
                },
                True,
                id="CreateContainerConfigError",
            ),
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {
                                    "waiting": {"reason": "ErrImagePull", "message": "pull QPS exceeded"}
                                },
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:latest",
                                "imageID": "",
                            }
                        ],
                    }
                },
                False,
                id="ErrImagePull Image QPS Exceeded",
            ),
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {
                                    "waiting": {
                                        "reason": "ErrImagePull",
                                        "message": "rpc error: code = Unknown desc = Error response from daemon: manifest for dockerhub.com/apache/airflow:xyz not found: manifest unknown: Requested image not found",
                                    }
                                },
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:xyz",
                                "imageID": "",
                            }
                        ],
                    }
                },
                True,
                id="ErrImagePull Image Not Found",
            ),
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {
                                    "waiting": {
                                        "reason": "CreateContainerError",
                                        "message": r'Error: Error response from daemon: create \invalid\path: "\invalid\path" includes invalid characters for a local volume name, only "[a-zA-Z0-9][a-zA-Z0-9_.-]" are allowed. If you intended to pass a host directory, use absolute path',
                                    }
                                },
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:latest",
                                "imageID": "",
                            }
                        ],
                    }
                },
                True,
                id="CreateContainerError",
            ),
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {
                                    "waiting": {
                                        "reason": "ImageInspectError",
                                        "message": 'Failed to inspect image "dockerhub.com/apache/airflow:latest": rpc error: code = Unknown desc = Error response from daemon: readlink /var/lib/docker/overlay2: invalid argument',
                                    }
                                },
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:latest",
                                "imageID": "",
                            }
                        ],
                    }
                },
                True,
                id="ImageInspectError",
            ),
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {
                                    "waiting": {
                                        "reason": "InvalidImageName",
                                        "message": 'Failed to apply default image tag "dockerhub.com/apache/airflow:latest+07": couldnot parse image reference "dockerhub.com/apache/airflow:latest+07": invalid reference format',
                                    }
                                },
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:latest+07",
                                "imageID": "",
                            }
                        ],
                    }
                },
                True,
                id="InvalidImageName",
            ),
            pytest.param(
                {
                    "status": {
                        "startTime": "2020-05-12T03:49:57Z",
                        "containerStatuses": [
                            {
                                "name": "base",
                                "state": {"waiting": {"reason": "OtherReasons", "message": ""}},
                                "lastState": {},
                                "ready": False,
                                "restartCount": 0,
                                "image": "dockerhub.com/apache/airflow:latest",
                                "imageID": "",
                            }
                        ],
                    }
                },
                False,
                id="OtherReasons",
            ),
        ],
    )
    def test_process_status_pending(self, raw_object, is_watcher_queue_called):
        self.events.append({"type": "MODIFIED", "object": self.pod, "raw_object": raw_object})

        self._run()
        if is_watcher_queue_called:
            self.assert_watcher_queue_called_once_with_state(State.FAILED)
        else:
            self.watcher.watcher_queue.put.assert_not_called()

    def test_process_status_pending_deleted(self):
        self.events.append({"type": "DELETED", "object": self.pod})
        self.pod.metadata.deletion_timestamp = timezone.utcnow()

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_failed(self):
        self.pod.status.phase = "Failed"
        self.events.append({"type": "MODIFIED", "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_provider_failed(self):
        self.pod.status.reason = "ProviderFailed"
        self.events.append({"type": "MODIFIED", "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_succeeded(self):
        self.pod.status.phase = "Succeeded"
        self.events.append({"type": "MODIFIED", "object": self.pod})

        self._run()
        # We don't know the TI state, so we send in None
        self.assert_watcher_queue_called_once_with_state(None)

    def test_process_status_succeeded_dedup_timestamp(self):
        self.pod.status.phase = "Succeeded"
        self.pod.metadata.deletion_timestamp = timezone.utcnow()
        self.events.append({"type": "MODIFIED", "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    @pytest.mark.parametrize(
        "ti_state",
        [
            TaskInstanceState.SUCCESS,
            TaskInstanceState.FAILED,
            TaskInstanceState.RUNNING,
            TaskInstanceState.QUEUED,
            TaskInstanceState.UP_FOR_RETRY,
        ],
    )
    def test_process_status_pod_adopted(self, ti_state):
        self.pod.status.phase = ti_state
        self.events.append({"type": "DELETED", "object": self.pod})
        self.pod.metadata.deletion_timestamp = None

        self._run()
        self.watcher.watcher_queue.put.assert_called_once_with(
            (
                self.pod.metadata.name,
                self.watcher.namespace,
                ADOPTED,
                self.core_annotations,
                self.pod.metadata.resource_version,
            )
        )

    def test_process_status_running_deleted(self):
        self.pod.status.phase = "Running"
        self.pod.metadata.deletion_timestamp = timezone.utcnow()
        self.events.append({"type": "DELETED", "object": self.pod})

        self._run()
        self.assert_watcher_queue_called_once_with_state(State.FAILED)

    def test_process_status_running(self):
        self.pod.status.phase = "Running"
        self.events.append({"type": "MODIFIED", "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    def test_process_status_catchall(self):
        self.pod.status.phase = "Unknown"
        self.events.append({"type": "MODIFIED", "object": self.pod})

        self._run()
        self.watcher.watcher_queue.put.assert_not_called()

    @mock.patch.object(KubernetesJobWatcher, "process_error")
    def test_process_error_event_for_410(self, mock_process_error):
        message = "too old resource version: 27272 (43334)"
        self.pod.status.phase = "Pending"
        self.pod.metadata.resource_version = "0"
        mock_process_error.return_value = "0"
        raw_object = {"code": 410, "message": message}
        self.events.append({"type": "ERROR", "object": self.pod, "raw_object": raw_object})
        self._run()
        mock_process_error.assert_called_once_with(self.events[0])

    def test_process_error_event_for_raise_if_not_410(self):
        message = "Failure message"
        self.pod.status.phase = "Pending"
        raw_object = {"code": 422, "message": message, "reason": "Test"}
        self.events.append({"type": "ERROR", "object": self.pod, "raw_object": raw_object})
        error_message = (
            rf"Kubernetes failure for {raw_object['reason']} "
            rf"with code {raw_object['code']} and message: {raw_object['message']}"
        )
        with pytest.raises(AirflowException, match=error_message):
            self._run()

    def test_recover_from_resource_too_old(self):
        # too old resource
        mock_underscore_run = mock.MagicMock()

        def effect():
            yield "500"
            while True:
                yield SystemError("sentinel")

        mock_underscore_run.side_effect = effect()

        self.watcher._run = mock_underscore_run

        with mock.patch(
            "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.get_kube_client"
        ):
            with pytest.raises(SystemError, match="sentinel"):
                # self.watcher._run() is mocked and return "500" as last resource_version
                self.watcher.run()

            # both resource_version should be 0 after _run raises an exception
            assert self.watcher.resource_version == "0"
            assert ResourceVersion().resource_version[self.test_namespace] == "0"

            # check that in the next run, _run is invoked with resource_version = 0
            mock_underscore_run.reset_mock()
            with pytest.raises(SystemError, match="sentinel"):
                self.watcher.run()

            mock_underscore_run.assert_called_once_with(mock.ANY, "0", mock.ANY, mock.ANY)
