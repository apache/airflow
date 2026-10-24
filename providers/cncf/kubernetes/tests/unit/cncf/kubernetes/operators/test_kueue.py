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

import json
from datetime import datetime
from unittest import mock

import pytest
from kubernetes.client import models as k8s
from kubernetes.utils import FailToCreateError

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.kueue import (
    KubernetesInstallKueueOperator,
    KubernetesStartKueueJobOperator,
)
from airflow.providers.cncf.kubernetes.triggers.kueue import KubernetesKueueJobTrigger, build_job_event
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

from unit.cncf.kubernetes.triggers.test_kueue import JOB_NAME, NAMESPACE, build_job, build_pod

TEST_TASK_ID = "test_task"
TEST_K8S_CONN_ID = "test_kubernetes_conn_id"
TEST_ERROR_CLASS = "TestException"
TEST_ERROR_BODY = "Test Exception Body"
TEST_QUEUE_NAME = "test-queue-name"

KUEUE_VERSION = "9.0.1"
KUEUE_YAML_URL = f"https://github.com/kubernetes-sigs/kueue/releases/download/{KUEUE_VERSION}/manifests.yaml"
KUEUE_OPERATORS_PATH = "airflow.providers.cncf.kubernetes.operators.kueue.{}"


class TestKubernetesInstallKueueOperator:
    def setup_method(self):
        self.operator = KubernetesInstallKueueOperator(
            task_id=TEST_TASK_ID,
            kueue_version=KUEUE_VERSION,
            kubernetes_conn_id=TEST_K8S_CONN_ID,
        )

    def test_template_fields(self):
        expected_template_fields = {"kueue_version", "kubernetes_conn_id"}
        assert set(KubernetesInstallKueueOperator.template_fields) == expected_template_fields

    def test_kueue_yaml_url_uses_rendered_version(self):
        with DAG("kueue", schedule=None, start_date=datetime(2020, 1, 1)):
            op = KubernetesInstallKueueOperator(
                task_id=TEST_TASK_ID,
                kueue_version="{{ params.v }}",
                kubernetes_conn_id=TEST_K8S_CONN_ID,
            )
        op.render_template_fields({"params": {"v": "v0.9.1"}})
        assert (
            op._kueue_yaml_url
            == "https://github.com/kubernetes-sigs/kueue/releases/download/v0.9.1/manifests.yaml"
        )

    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesHook"))
    def test_hook(self, mock_hook):
        mock_hook_instance = mock_hook.return_value

        actual_hook = self.operator.hook

        mock_hook.assert_called_once_with(conn_id=TEST_K8S_CONN_ID)
        assert actual_hook == mock_hook_instance

    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesInstallKueueOperator.log"))
    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesHook"))
    def test_execute(self, mock_hook, mock_log):
        mock_get_yaml_content_from_file = mock_hook.return_value.get_yaml_content_from_file
        mock_yaml_objects = mock_get_yaml_content_from_file.return_value

        self.operator.execute(context=mock.MagicMock())

        mock_get_yaml_content_from_file.assert_called_once_with(kueue_yaml_url=KUEUE_YAML_URL)
        mock_hook.return_value.apply_from_yaml_file.assert_called_once_with(yaml_objects=mock_yaml_objects)
        mock_hook.return_value.check_kueue_deployment_running.assert_called_once_with(
            name="kueue-controller-manager",
            namespace="kueue-system",
        )
        mock_log.info.assert_called_once_with("Kueue installed successfully!")

    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesInstallKueueOperator.log"))
    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesHook"))
    def test_execute_already_exist(self, mock_hook, mock_log):
        mock_get_yaml_content_from_file = mock_hook.return_value.get_yaml_content_from_file
        mock_yaml_objects = mock_get_yaml_content_from_file.return_value
        mock_apply_from_yaml_file = mock_hook.return_value.apply_from_yaml_file
        api_exceptions = [mock.MagicMock(body=json.dumps({"reason": "AlreadyExists"})) for _ in range(4)]
        mock_apply_from_yaml_file.side_effect = FailToCreateError(api_exceptions)

        self.operator.execute(context=mock.MagicMock())

        mock_get_yaml_content_from_file.assert_called_once_with(kueue_yaml_url=KUEUE_YAML_URL)
        mock_apply_from_yaml_file.assert_called_once_with(yaml_objects=mock_yaml_objects)
        mock_hook.return_value.check_kueue_deployment_running.assert_not_called()
        mock_log.info.assert_called_once_with("Kueue is already enabled for the cluster")

    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesInstallKueueOperator.log"))
    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesHook"))
    def test_execute_error(self, mock_hook, mock_log):
        mock_get_yaml_content_from_file = mock_hook.return_value.get_yaml_content_from_file
        mock_yaml_objects = mock_get_yaml_content_from_file.return_value
        mock_apply_from_yaml_file = mock_hook.return_value.apply_from_yaml_file
        api_exceptions = [
            mock.MagicMock(body=json.dumps({"reason": "AlreadyExists"})),
            mock.MagicMock(body=json.dumps({"reason": TEST_ERROR_CLASS, "body": TEST_ERROR_BODY})),
            mock.MagicMock(body=json.dumps({"reason": TEST_ERROR_CLASS, "body": TEST_ERROR_BODY})),
        ]
        mock_apply_from_yaml_file.side_effect = FailToCreateError(api_exceptions)
        expected_error_message = f"{TEST_ERROR_BODY}\n{TEST_ERROR_BODY}"

        with pytest.raises(AirflowException, match=expected_error_message):
            self.operator.execute(context=mock.MagicMock())

        mock_get_yaml_content_from_file.assert_called_once_with(kueue_yaml_url=KUEUE_YAML_URL)
        mock_apply_from_yaml_file.assert_called_once_with(yaml_objects=mock_yaml_objects)
        mock_hook.return_value.check_kueue_deployment_running.assert_not_called()
        mock_log.info.assert_called_once_with("Kueue is already enabled for the cluster")

    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesInstallKueueOperator.log"))
    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesHook"))
    def test_execute_non_json_response(self, mock_hook, mock_log):
        """Test handling of non-JSON API response bodies (e.g., 429 errors)."""
        mock_get_yaml_content_from_file = mock_hook.return_value.get_yaml_content_from_file
        mock_yaml_objects = mock_get_yaml_content_from_file.return_value
        mock_apply_from_yaml_file = mock_hook.return_value.apply_from_yaml_file

        # Create mock exceptions with non-JSON bodies (simulating 429 errors)
        api_exceptions = [
            mock.MagicMock(body="Too many requests, please try again later.", reason="TooManyRequests"),
            mock.MagicMock(body="", reason="RateLimited"),  # Empty body case
        ]
        mock_apply_from_yaml_file.side_effect = FailToCreateError(api_exceptions)
        expected_error_message = "Too many requests, please try again later.\nRateLimited"

        with pytest.raises(AirflowException, match=expected_error_message):
            self.operator.execute(context=mock.MagicMock())

        mock_get_yaml_content_from_file.assert_called_once_with(kueue_yaml_url=KUEUE_YAML_URL)
        mock_apply_from_yaml_file.assert_called_once_with(yaml_objects=mock_yaml_objects)
        mock_hook.return_value.check_kueue_deployment_running.assert_not_called()


class TestKubernetesStartKueueJobOperator:
    def test_template_fields(self):
        expected_template_fields = {"queue_name"} | set(KubernetesJobOperator.template_fields)
        assert set(KubernetesStartKueueJobOperator.template_fields) == expected_template_fields

    def test_init(self):
        operator = KubernetesStartKueueJobOperator(
            task_id=TEST_TASK_ID, queue_name=TEST_QUEUE_NAME, suspend=True
        )

        assert operator.queue_name == TEST_QUEUE_NAME
        assert operator.suspend is True
        assert operator.labels == {"kueue.x-k8s.io/queue-name": TEST_QUEUE_NAME}
        assert operator.annotations == {"kueue.x-k8s.io/queue-name": TEST_QUEUE_NAME}

    def test_init_suspend_is_false(self):
        expected_error_message = (
            "The `suspend` parameter can't be False. If you want to use Kueue for running Job"
            " in a Kubernetes cluster, set the `suspend` parameter to True."
        )
        with pytest.raises(AirflowException, match=expected_error_message):
            KubernetesStartKueueJobOperator(task_id=TEST_TASK_ID, queue_name=TEST_QUEUE_NAME, suspend=False)

    @mock.patch(KUEUE_OPERATORS_PATH.format("KubernetesStartKueueJobOperator.log"))
    def test_init_suspend_is_none(self, mock_log):
        expected_info_message = (
            "You have not set parameter `suspend` in class %s. "
            "For running a Job in Kueue the `suspend` parameter has been set to True."
        )
        operator = KubernetesStartKueueJobOperator(
            task_id=TEST_TASK_ID,
            queue_name=TEST_QUEUE_NAME,
        )

        assert operator.queue_name == TEST_QUEUE_NAME
        assert operator.suspend is True
        assert operator.labels == {"kueue.x-k8s.io/queue-name": TEST_QUEUE_NAME}
        assert operator.annotations == {"kueue.x-k8s.io/queue-name": TEST_QUEUE_NAME}
        mock_log.info.assert_called_once_with(expected_info_message, "KubernetesStartKueueJobOperator")


class TestKubernetesStartKueueJobOperatorWait:
    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        operator_class = KubernetesStartKueueJobOperator
        with (
            mock.patch.object(operator_class, "hook") as self.mock_hook,
            mock.patch.object(operator_class, "client") as self.mock_client,
            mock.patch.object(operator_class, "pod_manager") as self.mock_pod_manager,
            mock.patch.object(operator_class, "_write_logs") as self.mock_write_logs,
            mock.patch.object(operator_class, "post_complete_action"),
            mock.patch.object(operator_class, "build_job_request_obj", return_value=build_job(True)),
            mock.patch.object(
                operator_class, "create_job", side_effect=lambda job_request_obj: job_request_obj
            ),
        ):
            self.mock_hook.get_pod.side_effect = lambda name, namespace: self.pods[name]
            yield

    def set_pods(self, *pod_lists: list[k8s.V1Pod]) -> None:
        """Make each list_namespaced_pod call return the next list of pods."""
        self.pods = {pod.metadata.name: pod for pods in pod_lists for pod in pods}
        self.mock_client.list_namespaced_pod.side_effect = [k8s.V1PodList(items=pods) for pods in pod_lists]

    def build_operator(self, **kwargs) -> KubernetesStartKueueJobOperator:
        return KubernetesStartKueueJobOperator(
            task_id=TEST_TASK_ID,
            queue_name=TEST_QUEUE_NAME,
            namespace=NAMESPACE,
            wait_until_job_complete=True,
            job_poll_interval=0,
            **kwargs,
        )

    @mock.patch.object(KubernetesJobOperator, "execute", autospec=True)
    def test_execute_without_wait_runs_job_operator(self, mock_execute):
        operator = self.build_operator()
        operator.wait_until_job_complete = False

        assert operator.execute({}) == mock_execute.return_value
        self.mock_hook.get_job_status.assert_not_called()

    def test_execute_waits_through_preemption_and_reads_pods_of_last_run(self):
        """Kueue admits the Job, preempts it (its first pod is deleted), admits it again, then it completes."""
        last_run = build_pod("last-run", base="terminated", sidecar="running", phase="Succeeded")
        self.set_pods([build_pod("first-run", base="running", sidecar="running")], [last_run], [last_run])
        self.mock_hook.get_job_status.side_effect = [
            build_job(True),
            build_job(False),
            build_job(True),
            build_job(False),
            build_job(False, "Complete"),
        ]
        self.mock_pod_manager.extract_xcom.return_value = '{"a": 1}'

        result = self.build_operator(do_xcom_push=True, get_logs=True).execute({"ti": mock.MagicMock()})

        assert result == {"a": 1}
        self.mock_hook.get_job_status.assert_called_with(job_name=JOB_NAME, namespace=NAMESPACE)
        self.mock_pod_manager.extract_xcom.assert_called_once_with(last_run, ignore_kill_failure=False)
        self.mock_write_logs.assert_called_once_with(last_run)

    def test_execute_raises_when_job_failed(self):
        self.set_pods([])
        self.mock_hook.get_job_status.side_effect = [build_job(True), build_job(False, "Failed")]

        with pytest.raises(AirflowException, match="Job failed with error: BackoffLimitExceeded"):
            self.build_operator().execute({"ti": mock.MagicMock()})

    def test_execute_deferrable_defers_to_kueue_job_trigger(self):
        with pytest.raises(TaskDeferred) as deferred:
            self.build_operator(deferrable=True).execute({"ti": mock.MagicMock()})

        assert isinstance(deferred.value.trigger, KubernetesKueueJobTrigger)
        assert deferred.value.method_name == "execute_complete"
        self.mock_client.list_namespaced_pod.assert_not_called()

    def test_execute_complete_returns_xcom_of_succeeded_pods_of_the_job(self):
        self.set_pods(
            [
                build_pod("succeeded", base="terminated", phase="Succeeded"),
                build_pod("failed", base="terminated", phase="Failed"),
            ]
        )
        event = build_job_event(
            build_job(False, "Complete"), {"succeeded": '{"a": 1}', "failed": '{"b": 2}', "preempted": "3"}
        )

        result = self.build_operator(do_xcom_push=True, unwrap_single=False).execute_complete(
            {"ti": mock.MagicMock()}, event
        )

        assert result == [{"a": 1}]
