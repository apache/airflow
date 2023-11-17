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
"""Manage a Kubernetes Resource."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

import yaml
from kubernetes.utils import create_from_yaml

from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.utils.delete_from import delete_from_yaml

if TYPE_CHECKING:
    from kubernetes.client import ApiClient

__all__ = ["KubernetesCreateResourceOperator", "KubernetesDeleteResourceOperator"]


class KubernetesResourceBaseOperator(BaseOperator):
    """
    Abstract base class for all Kubernetes Resource operators.

    :param yaml_conf: string. Contains the kubernetes resources to Create or Delete
    :param namespace: string. Contains the namespace to create all resources inside.
        The namespace must preexist otherwise the resource creation will fail.
        If the API object in the yaml file already contains a namespace definition then
        this parameter has no effect.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    """

    template_fields = ("yaml_conf",)
    template_fields_renderers = {"yaml_conf": "yaml"}

    def __init__(
        self,
        *,
        yaml_conf: str,
        namespace: str | None = None,
        kubernetes_conn_id: str | None = KubernetesHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.yaml_conf = yaml_conf

    @cached_property
    def client(self) -> ApiClient:
        return self.hook.api_client

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(conn_id=self.kubernetes_conn_id)
        return hook

    def get_namespace(self) -> str:
        if self._namespace:
            return self._namespace
        else:
            return self.hook.get_namespace() or "default"


class KubernetesCreateResourceOperator(KubernetesResourceBaseOperator):
    """Create a resource in a kubernetes."""

    def execute(self, context) -> None:
        create_from_yaml(
            k8s_client=self.client,
            yaml_objects=yaml.safe_load_all(self.yaml_conf),
            namespace=self.get_namespace(),
        )


class KubernetesDeleteResourceOperator(KubernetesResourceBaseOperator):
    """Delete a resource in a kubernetes."""

    def execute(self, context) -> None:
        delete_from_yaml(
            k8s_client=self.client,
            yaml_objects=yaml.safe_load_all(self.yaml_conf),
            namespace=self.get_namespace(),
        )
