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
from datetime import datetime
from typing import Any, AsyncIterator, Sequence

from google.cloud.container_v1.types import Operation

try:
    from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
except ImportError:
    # preserve backward compatibility for older versions of cncf.kubernetes provider
    from airflow.providers.cncf.kubernetes.triggers.kubernetes_pod import KubernetesPodTrigger
from airflow.providers.google.cloud.hooks.kubernetes_engine import GKEAsyncHook, GKEPodAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GKEStartPodTrigger(KubernetesPodTrigger):
    """
    Trigger for checking pod status until it finishes its job.

    :param pod_name: The name of the pod.
    :param pod_namespace: The namespace of the pod.
    :param cluster_url: The URL pointed to the cluster.
    :param ssl_ca_cert: SSL certificate that is used for authentication to the pod.
    :param cluster_context: Context that points to kubernetes cluster.
    :param poll_interval: Polling period in seconds to check for the status.
    :param trigger_start_time: time in Datetime format when the trigger was started
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param should_delete_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param startup_timeout: timeout in seconds to start up the pod.
    :param base_container_name: The name of the base container in the pod. This container's logs
        will appear as part of this task's logs if get_logs is True. Defaults to None. If None,
        will consult the class variable BASE_CONTAINER_NAME (which defaults to "base") for the base
        container name to use.
    """

    def __init__(
        self,
        pod_name: str,
        pod_namespace: str,
        cluster_url: str,
        ssl_ca_cert: str,
        base_container_name: str,
        trigger_start_time: datetime,
        cluster_context: str | None = None,
        poll_interval: float = 2,
        in_cluster: bool | None = None,
        should_delete_pod: bool = True,
        get_logs: bool = True,
        startup_timeout: int = 120,
        *args,
        **kwargs,
    ):
        super().__init__(
            pod_name,
            pod_namespace,
            trigger_start_time,
            base_container_name,
            *args,
            **kwargs,
        )
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.trigger_start_time = trigger_start_time
        self.base_container_name = base_container_name
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster
        self.should_delete_pod = should_delete_pod
        self.get_logs = get_logs
        self.startup_timeout = startup_timeout

        self._cluster_url = cluster_url
        self._ssl_ca_cert = ssl_ca_cert

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.kubernetes_engine.GKEStartPodTrigger",
            {
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "cluster_url": self._cluster_url,
                "ssl_ca_cert": self._ssl_ca_cert,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "in_cluster": self.in_cluster,
                "should_delete_pod": self.should_delete_pod,
                "get_logs": self.get_logs,
                "startup_timeout": self.startup_timeout,
                "trigger_start_time": self.trigger_start_time,
                "base_container_name": self.base_container_name,
            },
        )

    def _get_async_hook(self) -> GKEPodAsyncHook:  # type: ignore[override]
        return GKEPodAsyncHook(
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
        )


class GKEOperationTrigger(BaseTrigger):
    """Trigger which checks status of the operation."""

    def __init__(
        self,
        operation_name: str,
        project_id: str | None,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        poll_interval: int = 10,
    ):
        super().__init__()

        self.operation_name = operation_name
        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_interval = poll_interval

        self._hook: GKEAsyncHook | None = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes GKEOperationTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.kubernetes_engine.GKEOperationTrigger",
            {
                "operation_name": self.operation_name,
                "project_id": self.project_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Gets operation status and yields corresponding event."""
        hook = self._get_hook()
        while True:
            try:
                operation = await hook.get_operation(
                    operation_name=self.operation_name,
                    project_id=self.project_id,
                )

                status = operation.status
                if status == Operation.Status.DONE:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Operation is successfully ended.",
                            "operation_name": operation.name,
                        }
                    )
                    return

                elif status == Operation.Status.RUNNING or status == Operation.Status.PENDING:
                    self.log.info("Operation is still running.")
                    self.log.info("Sleeping for %ss...", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)

                else:
                    yield TriggerEvent(
                        {
                            "status": "failed",
                            "message": f"Operation has failed with status: {operation.status}",
                        }
                    )
                    return
            except Exception as e:
                self.log.exception("Exception occurred while checking operation status")
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": str(e),
                    }
                )
                return

    def _get_hook(self) -> GKEAsyncHook:
        if self._hook is None:
            self._hook = GKEAsyncHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        return self._hook
