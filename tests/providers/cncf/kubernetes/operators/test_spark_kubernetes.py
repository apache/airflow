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
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator(mock_kubernetes_hook):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file="application_file",
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )
    mock_kubernetes_hook.assert_not_called()  # constructor shouldn't call the hook

    assert "hook" not in operator.__dict__  # Cached property has not been accessed as part of construction.


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator_hook(mock_kubernetes_hook):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file="application_file",
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )
    operator.hook
    mock_kubernetes_hook.assert_called_with(
        conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.Watch.stream")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes._load_body_to_dict")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_execute_with_watch(mock_kubernetes_hook, mock_load_body_to_dict, mock_stream):
    mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}

    mock_kubernetes_hook.return_value.create_custom_object.return_value = {
        "metadata": {"name": "spark-app", "creationTimestamp": "2022-01-01T00:00:00Z"}
    }
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

    object_mock = MagicMock()
    object_mock.reason = "SparkDriverRunning"
    object_mock.last_timestamp = datetime(2022, 1, 1, 23, 59, 59, tzinfo=tz.tzutc())
    mock_stream.side_effect = [[{"object": object_mock}], []]

    op = SparkKubernetesOperator(task_id="task_id", application_file="application_file", watch=True)
    operator_output = op.execute({})

    mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
        group="sparkoperator.k8s.io",
        version="v1beta2",
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

    assert operator_output == {"metadata": {"name": "spark-app", "creationTimestamp": "2022-01-01T00:00:00Z"}}


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator.on_kill")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.Watch.stream")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes._load_body_to_dict")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_raise_exception_when_job_fails(
    mock_kubernetes_hook, mock_load_body_to_dict, mock_stream, mock_on_kill
):
    mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}

    mock_kubernetes_hook.return_value.create_custom_object.return_value = {
        "metadata": {"name": "spark-app", "creationTimestamp": "2022-01-01T00:00:00Z"}
    }
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

    object_mock = MagicMock()
    object_mock.reason = "SparkApplicationFailed"
    object_mock.message = "spark-app submission failed"
    object_mock.last_timestamp = datetime(2022, 1, 1, 23, 59, 59, tzinfo=tz.tzutc())

    mock_stream.side_effect = [[{"object": object_mock}], []]
    op = SparkKubernetesOperator(task_id="task_id", application_file="application_file", watch=True)
    with pytest.raises(AirflowException, match="spark-app submission failed"):
        op.execute({})

    assert mock_on_kill.has_called_once()


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes._load_body_to_dict")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_execute_without_watch(mock_kubernetes_hook, mock_load_body_to_dict):
    mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}

    mock_kubernetes_hook.return_value.create_custom_object.return_value = {
        "metadata": {"name": "spark-app", "creationTimestamp": "2022-01-01T00:00:00Z"}
    }
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

    op = SparkKubernetesOperator(task_id="task_id", application_file="application_file")
    operator_output = op.execute({})

    mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        plural="sparkapplications",
        body={"metadata": {"name": "spark-app"}},
        namespace="default",
    )
    assert operator_output == {"metadata": {"name": "spark-app", "creationTimestamp": "2022-01-01T00:00:00Z"}}


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes._load_body_to_dict")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_on_kill(mock_kubernetes_hook, mock_load_body_to_dict):
    mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

    op = SparkKubernetesOperator(task_id="task_id", application_file="application_file")

    op.on_kill()

    mock_kubernetes_hook.return_value.delete_custom_object.assert_called_once_with(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        plural="sparkapplications",
        namespace="default",
        name="spark-app",
    )


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_execute_with_application_file_dict(mock_kubernetes_hook):
    op = SparkKubernetesOperator(task_id="task_id", application_file={"metadata": {"name": "spark-app"}})
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

    op.execute({})

    mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        plural="sparkapplications",
        body={"metadata": {"name": "spark-app"}},
        namespace="default",
    )


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_on_kill_with_application_file_dict(mock_kubernetes_hook):
    op = SparkKubernetesOperator(task_id="task_id", application_file={"metadata": {"name": "spark-app"}})
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"

    op.on_kill()

    mock_kubernetes_hook.return_value.delete_custom_object.assert_called_once_with(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        plural="sparkapplications",
        name="spark-app",
        namespace="default",
    )
