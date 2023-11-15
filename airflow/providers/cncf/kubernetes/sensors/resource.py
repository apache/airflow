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
from kubernetes.dynamic import DynamicClient, ResourceInstance

from airflow.exceptions import AirflowFailException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from kubernetes.client import ApiClient

    from airflow.utils.context import Context


class BaseKubernetesResourceSensor(BaseSensorOperator):
    """
    Abstract base class for all Kubernetes Resource sensors.

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
        custom_resource_definition: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.yaml_conf = yaml_conf
        self.custom_resource_definition = custom_resource_definition

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

    def get_state(self) -> dict:
        body = next(yaml.safe_load_all(self.yaml_conf))  # only first element

        dynamic_client = DynamicClient(self.client)
        api_version = body.get("apiVersion")
        kind = body.get("kind")
        namespace = body.get("metadata").get("namespace")
        if namespace is None:
            namespace = self.get_namespace()

        crd_api = dynamic_client.resources.get(api_version=api_version, kind=kind)
        rst: ResourceInstance = crd_api.get(body=body, namespace=namespace)
        return rst.to_dict()


class JobKubernetesResourceSensor(BaseKubernetesResourceSensor):
    """Sensor a resource in a kubernetes."""

    def poke(self, context: Context) -> bool:
        state = self.get_state()
        job_status = state["items"][0]["status"]["jobStatus"]

        self.log.info("jobStatus : %s", job_status)

        if job_status == "SUCCEEDED":
            return True
        if job_status in ["STOPPED", "FAILED"]:
            raise AirflowFailException("Job error, status is : %s", job_status)
        return False
