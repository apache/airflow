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
from unittest import mock

import pytest
from kubernetes.utils import FailToCreateError

from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.kueue import (
    KubernetesInstallKueueOperator,
    KubernetesStartKueueJobOperator,
)
from airflow.providers.common.compat.sdk import AirflowException

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
