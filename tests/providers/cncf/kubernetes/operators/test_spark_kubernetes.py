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

from unittest.mock import patch

from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator(mock_kubernetes_hook):
    SparkKubernetesOperator(
        task_id="task_id",
        application_file="application_file",
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )

    mock_kubernetes_hook.assert_called_once_with(
        conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.Watch.stream")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes._load_body_to_dict")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_execute(mock_kubernetes_hook, mock_load_body_to_dict, mock_stream):
    mock_load_body_to_dict.return_value = {"metadata": {"name": "spark-app"}}
    mock_kubernetes_hook.return_value.get_namespace.return_value = "default"
    mock_stream.side_effect = [[{"type": "ADDED"}], []]

    op = SparkKubernetesOperator(task_id="task_id", application_file="application_file")
    op.execute({})
    mock_kubernetes_hook.return_value.create_custom_object.assert_called_once_with(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        plural="sparkapplications",
        body={"metadata": {"name": "spark-app"}},
        namespace="default",
    )

    assert mock_stream.call_count == 2
    mock_stream.assert_any_call(
        mock_kubernetes_hook.return_value.core_v1_client.list_namespaced_pod,
        namespace="default",
        _preload_content=False,
        watch=True,
        label_selector="sparkoperator.k8s.io/app-name=spark-app,spark-role=driver",
        field_selector="status.phase=Running",
    )

    mock_stream.assert_any_call(
        mock_kubernetes_hook.return_value.core_v1_client.read_namespaced_pod_log,
        name="spark-app-driver",
        namespace="default",
        _preload_content=False,
        timestamps=True,
    )


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
