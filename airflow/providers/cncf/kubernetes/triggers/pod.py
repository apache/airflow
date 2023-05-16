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
from asyncio import CancelledError
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator

import pytz
from kubernetes_asyncio.client.models import V1Pod

from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ContainerState(str, Enum):
    """
    Possible container states
    See https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase.
    """

    WAITING = "waiting"
    RUNNING = "running"
    TERMINATED = "terminated"
    FAILED = "failed"
    UNDEFINED = "undefined"


class KubernetesPodTrigger(BaseTrigger):
    """
    KubernetesPodTrigger run on the trigger worker to check the state of Pod.

    :param pod_name: The name of the pod.
    :param pod_namespace: The namespace of the pod.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param cluster_context: Context that points to kubernetes cluster.
    :param config_file: Path to kubeconfig file.
    :param poll_interval: Polling period in seconds to check for the status.
    :param trigger_start_time: time in Datetime format when the trigger was started
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param should_delete_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param startup_timeout: timeout in seconds to start up the pod.
    """

    def __init__(
        self,
        pod_name: str,
        pod_namespace: str,
        trigger_start_time: datetime,
        base_container_name: str,
        kubernetes_conn_id: str | None = None,
        poll_interval: float = 2,
        cluster_context: str | None = None,
        config_file: str | None = None,
        in_cluster: bool | None = None,
        should_delete_pod: bool = True,
        get_logs: bool = True,
        startup_timeout: int = 120,
    ):
        super().__init__()
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.trigger_start_time = trigger_start_time
        self.base_container_name = base_container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.should_delete_pod = should_delete_pod
        self.get_logs = get_logs
        self.startup_timeout = startup_timeout

        self._hook: AsyncKubernetesHook | None = None
        self._since_time = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes KubernetesCreatePodTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger",
            {
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "base_container_name": self.base_container_name,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "config_file": self.config_file,
                "in_cluster": self.in_cluster,
                "should_delete_pod": self.should_delete_pod,
                "get_logs": self.get_logs,
                "startup_timeout": self.startup_timeout,
                "trigger_start_time": self.trigger_start_time,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets current pod status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        self.log.info("Checking pod %r in namespace %r.", self.pod_name, self.pod_namespace)
        while True:
            try:
                pod = await hook.get_pod(
                    name=self.pod_name,
                    namespace=self.pod_namespace,
                )

                pod_status = pod.status.phase
                self.log.debug("Pod %s status: %s", self.pod_name, pod_status)

                container_state = self.define_container_state(pod)
                self.log.debug("Container %s status: %s", self.base_container_name, container_state)

                if container_state == ContainerState.TERMINATED:
                    yield TriggerEvent(
                        {
                            "name": self.pod_name,
                            "namespace": self.pod_namespace,
                            "status": "success",
                            "message": "All containers inside pod have started successfully.",
                        }
                    )
                    return
                elif self.should_wait(pod_phase=pod_status, container_state=container_state):
                    self.log.info("Container is not completed and still working.")

                    if pod_status == PodPhase.PENDING and container_state == ContainerState.UNDEFINED:
                        delta = datetime.now(tz=pytz.UTC) - self.trigger_start_time
                        if delta.total_seconds() >= self.startup_timeout:
                            message = (
                                f"Pod took longer than {self.startup_timeout} seconds to start. "
                                "Check the pod events in kubernetes to determine why."
                            )
                            yield TriggerEvent(
                                {
                                    "name": self.pod_name,
                                    "namespace": self.pod_namespace,
                                    "status": "timeout",
                                    "message": message,
                                }
                            )
                            return

                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent(
                        {
                            "name": self.pod_name,
                            "namespace": self.pod_namespace,
                            "status": "failed",
                            "message": pod.status.message,
                        }
                    )
                    return
            except CancelledError:
                # That means that task was marked as failed
                if self.get_logs:
                    self.log.info("Outputting container logs...")
                    await self._get_async_hook().read_logs(
                        name=self.pod_name,
                        namespace=self.pod_namespace,
                    )
                if self.should_delete_pod:
                    self.log.info("Deleting pod...")
                    await self._get_async_hook().delete_pod(
                        name=self.pod_name,
                        namespace=self.pod_namespace,
                    )
                yield TriggerEvent(
                    {
                        "name": self.pod_name,
                        "namespace": self.pod_namespace,
                        "status": "cancelled",
                        "message": "Pod execution was cancelled",
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
                    }
                )
                return

    def _get_async_hook(self) -> AsyncKubernetesHook:
        if self._hook is None:
            self._hook = AsyncKubernetesHook(
                conn_id=self.kubernetes_conn_id,
                in_cluster=self.in_cluster,
                config_file=self.config_file,
                cluster_context=self.cluster_context,
            )
        return self._hook

    def define_container_state(self, pod: V1Pod) -> ContainerState:
        pod_containers = pod.status.container_statuses

        if pod_containers is None:
            return ContainerState.UNDEFINED

        container = [c for c in pod_containers if c.name == self.base_container_name][0]

        for state in (ContainerState.RUNNING, ContainerState.WAITING, ContainerState.TERMINATED):
            state_obj = getattr(container.state, state)
            if state_obj is not None:
                if state != ContainerState.TERMINATED:
                    return state
                else:
                    return ContainerState.TERMINATED if state_obj.exit_code == 0 else ContainerState.FAILED
        return ContainerState.UNDEFINED

    @staticmethod
    def should_wait(pod_phase: PodPhase, container_state: ContainerState) -> bool:
        return (
            container_state == ContainerState.WAITING
            or container_state == ContainerState.RUNNING
            or (container_state == ContainerState.UNDEFINED and pod_phase == PodPhase.PENDING)
        )
