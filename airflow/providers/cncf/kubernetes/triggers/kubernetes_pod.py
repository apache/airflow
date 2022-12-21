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

import asyncio
from enum import Enum
from typing import Any, AsyncIterator

from kubernetes_asyncio.client.models import V1Pod

from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

BASE_CONTAINER_NAME = "base"


class ContainerState(str, Enum):
    """
    Possible container states
    See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.
    """

    WAITING = "waiting"
    RUNNING = "running"
    TERMINATED = "terminated"
    FAILED = "failed"


class ContainerStateUndefined(BaseException):
    """Exception class to be used when trigger can't define container status."""


class KubernetesCreatePodTrigger(BaseTrigger):
    """
    KubernetesCreatePodTrigger run on the trigger worker to check the state of Pod.

    :param pod_name: The name of the pod.
    :param pod_namespace: The namespace of the pod.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param cluster_context: Context that points to kubernetes cluster.
    :param config_dict: Kubernetes config file content in dict format. If not specified,
        default value is ``~/.kube/config``
    :param poll_interval: Polling period in seconds to check for the status.
    """

    def __init__(
        self,
        pod_name: str,
        pod_namespace: str,
        kubernetes_conn_id: str | None = None,
        poll_interval: float = 10,
        cluster_context: str | None = None,
        config_dict: dict | None = None,
        in_cluster: bool | None = None,
    ):
        super().__init__()
        self.kubernetes_conn_id = kubernetes_conn_id
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_dict = config_dict
        self.in_cluster = in_cluster

        self._hook: AsyncKubernetesHook | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes KubernetesCreatePodTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.kubernetes_pod.KubernetesCreatePodTrigger",
            {
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "config_dict": self.config_dict,
                "in_cluster": self.in_cluster,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current pod status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        self.log.info("Checking pod %r in namespace %r.", self.pod_name, self.pod_namespace)
        while True:
            try:
                pod = await hook.get_pod(
                    name=self.pod_name,
                    namespace=self.pod_namespace,
                )
                container_state = self.define_container_state(pod)
                self.log.debug("Container %s status: %s", BASE_CONTAINER_NAME, container_state)

                if container_state == ContainerState.TERMINATED:
                    yield TriggerEvent(
                        {
                            "name": self.pod_name,
                            "namespace": self.pod_namespace,
                            "status": "success",
                            "message": "All containers inside pod have started successfully.",
                            "config_dict": self.config_dict,
                        }
                    )
                    return
                elif container_state == ContainerState.WAITING or container_state == ContainerState.RUNNING:
                    self.log.info("Container is not completed and still working.")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent(
                        {
                            "name": self.pod_name,
                            "namespace": self.pod_namespace,
                            "status": "failed",
                            "message": pod.status.message,
                            "config_dict": self.config_dict,
                        }
                    )
                    return
            except Exception as e:
                self.log.exception("Exception occurred while checking pod phase:")
                yield TriggerEvent(
                    {
                        "name": self.pod_name,
                        "namespace": self.pod_namespace,
                        "status": "error",
                        "message": str(e),
                        "config_dict": self.config_dict,
                    }
                )
                return

    def _get_async_hook(self) -> AsyncKubernetesHook:
        if self._hook is None:
            self._hook = AsyncKubernetesHook(
                conn_id=self.kubernetes_conn_id,
                in_cluster=self.in_cluster,
                config_dict=self.config_dict,
                cluster_context=self.cluster_context,
            )
        return self._hook

    @staticmethod
    def define_container_state(pod: V1Pod) -> ContainerState:
        pod_containers = pod.status.container_statuses
        container = [container for container in pod_containers if container.name == BASE_CONTAINER_NAME][0]

        for state in (ContainerState.RUNNING, ContainerState.WAITING, ContainerState.TERMINATED):
            state_obj = getattr(container.state, state)
            if state_obj is not None:
                if state != ContainerState.TERMINATED:
                    return state
                else:
                    return ContainerState.TERMINATED if state_obj.exit_code == 0 else ContainerState.FAILED
        raise ContainerStateUndefined(
            f"Can not define state of the container. Statuses of the containers are {pod_containers}",
        )
