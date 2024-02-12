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
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from dateutil import tz

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.dataprocgdc import (
    DataprocGDCSubmitSparkJobKrmOperator,
    DataprocGdcCreateAppEnvironmentKrmOperator
)


class TestDataprocGDCSparkSubmitKrmOperator:
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.Watch.stream")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc._load_body_to_dict")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.KubernetesHook")
    def test_submit_spark_app_with_watch(self, mock_kubernetes_hook, mock_load_body_to_dict, mock_stream):
        mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}

        mock_kubernetes_hook.return_value.create_custom_object.return_value = {
            "metadata": {"name": "spark-app", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }
        mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

        object_mock = MagicMock()
        object_mock.reason = "SparkDriverRunning"
        object_mock.last_timestamp = datetime(2024, 1, 1, 23, 59, 59, tzinfo=tz.tzutc())
        mock_stream.side_effect = [[{"object": object_mock}], []]

        op = DataprocGDCSubmitSparkJobKrmOperator(
            task_id="task_id", application_file="application_file", watch=True
        )
        operator_output = op.execute({})

        mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            version="v1alpha1",
            plural="sparkapplications",
            body={"metadata": {"name": "spark-app"}},
            namespace="default",
        )

        assert mock_stream.call_count == 2
        mock_stream.assert_any_call(
            mock_kubernetes_hook.return_value.core_v1_client.list_namespaced_event,
            namespace="default",
            watch=True,
            field_selector="involvedObject.kind=SparkApplication,involvedObject.name=spark-app",
        )
        mock_stream.assert_any_call(
            mock_kubernetes_hook.return_value.core_v1_client.read_namespaced_pod_log,
            name="spark-app-driver",
            namespace="default",
            timestamps=True,
        )

        assert operator_output == {
            "metadata": {"name": "spark-app", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }

    @patch(
        "airflow.providers.google.cloud.operators.dataprocgdc.DataprocGDCSubmitSparkJobKrmOperator.on_kill"
    )
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.Watch.stream")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc._load_body_to_dict")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.KubernetesHook")
    def test_fail_spark_app(self, mock_kubernetes_hook, mock_load_body_to_dict, mock_stream, mock_on_kill):
        mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}

        mock_kubernetes_hook.return_value.create_custom_object.return_value = {
            "metadata": {"name": "spark-app", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }
        mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

        object_mock = MagicMock()
        object_mock.reason = "SparkApplicationSubmissionFailed"
        object_mock.message = "Submission Failed"
        object_mock.last_timestamp = datetime(2024, 1, 1, 23, 59, 59, tzinfo=tz.tzutc())

        mock_stream.side_effect = [[{"object": object_mock}], []]
        op = DataprocGDCSubmitSparkJobKrmOperator(
            task_id="task_id", application_file="application_file", watch=True
        )
        with pytest.raises(AirflowException, match="Submission Failed"):
            op.execute({})

        assert mock_on_kill.has_called_once()

    @patch("airflow.providers.google.cloud.operators.dataprocgdc._load_body_to_dict")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.KubernetesHook")
    def test_submit_spark_app_without_watch(self, mock_kubernetes_hook, mock_load_body_to_dict):
        mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}

        mock_kubernetes_hook.return_value.create_custom_object.return_value = {
            "metadata": {"name": "spark-app", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }
        mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

        op = DataprocGDCSubmitSparkJobKrmOperator(task_id="task_id", application_file="application_file")
        operator_output = op.execute({})

        mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            version="v1alpha1",
            plural="sparkapplications",
            body={"metadata": {"name": "spark-app"}},
            namespace="default",
        )
        assert operator_output == {
            "metadata": {"name": "spark-app", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }

    @patch("airflow.providers.google.cloud.operators.dataprocgdc._load_body_to_dict")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.KubernetesHook")
    def test_on_kill(self, mock_kubernetes_hook, mock_load_body_to_dict):
        mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}
        mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

        op = DataprocGDCSubmitSparkJobKrmOperator(task_id="task_id", application_file="application_file")

        op.on_kill()

        mock_kubernetes_hook.return_value.delete_custom_object.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            version="v1alpha1",
            plural="sparkapplications",
            namespace="default",
            name="spark-app",
        )


class TestDataprocGDCCreateAppEnvironmentKrmOperator:
    @patch("airflow.providers.google.cloud.operators.dataprocgdc._load_body_to_dict")
    @patch("airflow.providers.google.cloud.operators.dataprocgdc.KubernetesHook")
    def test_create_app_env(self, mock_kubernetes_hook, mock_load_body_to_dict):
        mock_load_body_to_dict.return_value = {"metadata": {"name": "app-env"}}

        mock_kubernetes_hook.return_value.create_custom_object.return_value = {
            "metadata": {"name": "app-env", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }
        mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

        op = DataprocGdcCreateAppEnvironmentKrmOperator(
            task_id="task_id", application_file="application_file"
        )
        operator_output = op.execute({})

        mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            version="v1alpha1",
            plural="applicationenvironments",
            body={"metadata": {"name": "app-env"}},
            namespace="default",
        )
        assert operator_output == {
            "metadata": {"name": "app-env", "creationTimestamp": "2024-01-01T00:00:00Z"}
        }
