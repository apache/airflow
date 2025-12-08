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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api

    from airflow.providers.common.compat.sdk import Context


class FlinkKubernetesOperator(BaseOperator):
    """
    Creates flinkDeployment object in kubernetes cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:FlinkKubernetesOperator`

    .. seealso::
        For more detail about Flink Deployment Object have a look at the reference:
        https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/reference/#flinkdeployment

    :param application_file: Defines Kubernetes 'custom_resource_definition' of 'flinkDeployment' as either a
        path to a '.yaml' file, '.json' file, YAML string or JSON string.
    :param namespace: kubernetes namespace to put flinkDeployment
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the to Kubernetes cluster.
    :param api_group: kubernetes api group of flinkDeployment
    :param api_version: kubernetes api version of flinkDeployment
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    """

    template_fields: Sequence[str] = ("application_file", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        application_file: str,
        namespace: str | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = "flink.apache.org",
        api_version: str = "v1beta1",
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        config_file: str | None = None,
        plural: str = "flinkdeployments",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = plural
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.config_file = config_file

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    def execute(self, context: Context):
        self.log.info(
            "Creating flinkApplication with Context: %s and op_context: %s", self.cluster_context, context
        )

        self.hook.custom_object_client.list_cluster_custom_object(
            group=self.api_group, version=self.api_version, plural=self.plural
        )
        self.log.info("body=self.application_file: %s", self.application_file)
        response = self.hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response
