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
"""Manage a Kubernetes Resource"""

from __future__ import annotations

from typing import Sequence

from kubernetes.client import CoreV1Api

from airflow.compat.functools import cached_property
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils import yaml


class KubernetesResourceOperator(BaseOperator):
    """Abstract base class for all Kubernetes Resource operators."""

    template_fields: Sequence[str] = ("resource_name",)
    template_fields_renderers = {}

    def __init__(
        self,
        *,
        namespace: str | None = None,
        resource_name: str | None = None,
        kubernetes_conn_id: str | None = "kubernetes_default",
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        config_file: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._namespace = namespace
        self.resource_name = resource_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.config_file = config_file

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        return hook

    def get_namespace(self) -> str:
        if self._namespace:
            return self._namespace
        else:
            tmp = self.hook.get_namespace()
            if tmp:
                return tmp
            else:
                return "default"


class kubernetesPVCcreate(KubernetesResourceOperator):
    """Create PVC resource in a kubernetes."""

    template_fields: Sequence[str] = tuple(
        {
            "pvc_conf",
        }
        | set(KubernetesResourceOperator.template_fields)
    )
    pvc_conf: str

    def __int__(self, *, pvc_conf: str, **kwargs):
        super().__init__(**kwargs)
        self.pvc_conf = pvc_conf

    def execute(self, context) -> None:
        self.client.create_namespaced_persistent_volume_claim(
            namespace=self.get_namespace(), body=yaml.safe_load(self.pvc_conf)
        )


class kubernetesPVCdelete(KubernetesResourceOperator):
    """Delete PVC resource in a kubernetes."""

    def execute(self, context) -> None:
        self.client.delete_namespaced_persistent_volume_claim(
            name=self.resource_name, namespace=self.get_namespace()
        )
