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
import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncIterator, Sequence

from google.cloud.container_v1.types import Operation
from packaging.version import parse as parse_version

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction, PodManager
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.providers.google.cloud.hooks.kubernetes_engine import (
    GKEAsyncHook,
    GKEKubernetesAsyncHook,
    GKEKubernetesHook,
)
from airflow.providers_manager import ProvidersManager
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from datetime import datetime

    from kubernetes_asyncio.client import V1Job


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
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param startup_timeout: timeout in seconds to start up the pod.
    :param base_container_name: The name of the base container in the pod. This container's logs
        will appear as part of this task's logs if get_logs is True. Defaults to None. If None,
        will consult the class variable BASE_CONTAINER_NAME (which defaults to "base") for the base
        container name to use.
    :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
        If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
        only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
    :param should_delete_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True (default), delete the
        pod; if False, leave the pod.
        Deprecated - use `on_finish_action` instead.
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
        get_logs: bool = True,
        startup_timeout: int = 120,
        on_finish_action: str = "delete_pod",
        should_delete_pod: bool | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
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
        self.get_logs = get_logs
        self.startup_timeout = startup_timeout
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

        if should_delete_pod is not None:
            warnings.warn(
                "`should_delete_pod` parameter is deprecated, please use `on_finish_action`",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            self.on_finish_action = (
                OnFinishAction.DELETE_POD
                if should_delete_pod
                else OnFinishAction.KEEP_POD
            )
            self.should_delete_pod = should_delete_pod
        else:
            self.on_finish_action = OnFinishAction(on_finish_action)
            self.should_delete_pod = self.on_finish_action == OnFinishAction.DELETE_POD

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
                "get_logs": self.get_logs,
                "startup_timeout": self.startup_timeout,
                "trigger_start_time": self.trigger_start_time,
                "base_container_name": self.base_container_name,
                "should_delete_pod": self.should_delete_pod,
                "on_finish_action": self.on_finish_action.value,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "logging_interval": self.logging_interval,
                "last_log_time": self.last_log_time,
            },
        )

    @cached_property
    def hook(self) -> GKEKubernetesAsyncHook:  # type: ignore[override]
        return GKEKubernetesAsyncHook(
            cluster_url=self._cluster_url,
            ssl_ca_cert=self._ssl_ca_cert,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            enable_tcp_keepalive=True,
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
        """Serialize GKEOperationTrigger arguments and classpath."""
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
        """Get operation status and yields corresponding event."""
        hook = self._get_hook()
        try:
            while True:
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
                elif status in (Operation.Status.RUNNING, Operation.Status.PENDING):
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

    def _get_hook(self) -> GKEAsyncHook:
        if self._hook is None:
            self._hook = GKEAsyncHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        return self._hook


class GKEJobTrigger(BaseTrigger):
    """GKEJobTrigger run on the trigger worker to check the state of Job."""

    def __init__(
        self,
        cluster_url: str,
        ssl_ca_cert: str,
        job_name: str,
        job_namespace: str,
        pod_name: str,
        pod_namespace: str,
        base_container_name: str,
        gcp_conn_id: str = "google_cloud_default",
        poll_interval: float = 2,
        impersonation_chain: str | Sequence[str] | None = None,
        get_logs: bool = True,
        do_xcom_push: bool = False,
    ) -> None:
        super().__init__()
        self.cluster_url = cluster_url
        self.ssl_ca_cert = ssl_ca_cert
        self.job_name = job_name
        self.job_namespace = job_namespace
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.base_container_name = base_container_name
        self.gcp_conn_id = gcp_conn_id
        self.poll_interval = poll_interval
        self.impersonation_chain = impersonation_chain
        self.get_logs = get_logs
        self.do_xcom_push = do_xcom_push

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize KubernetesCreateJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.kubernetes_engine.GKEJobTrigger",
            {
                "cluster_url": self.cluster_url,
                "ssl_ca_cert": self.ssl_ca_cert,
                "job_name": self.job_name,
                "job_namespace": self.job_namespace,
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "base_container_name": self.base_container_name,
                "gcp_conn_id": self.gcp_conn_id,
                "poll_interval": self.poll_interval,
                "impersonation_chain": self.impersonation_chain,
                "get_logs": self.get_logs,
                "do_xcom_push": self.do_xcom_push,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current job status and yield a TriggerEvent."""
        if self.get_logs or self.do_xcom_push:
            pod = await self.hook.get_pod(
                name=self.pod_name, namespace=self.pod_namespace
            )
        if self.do_xcom_push:
            kubernetes_provider = ProvidersManager().providers[
                "apache-airflow-providers-cncf-kubernetes"
            ]
            kubernetes_provider_name = kubernetes_provider.data["package-name"]
            kubernetes_provider_version = kubernetes_provider.version
            min_version = "8.4.1"
            if parse_version(kubernetes_provider_version) < parse_version(min_version):
                raise AirflowException(
                    "You are trying to use do_xcom_push in `GKEStartJobOperator` with the provider "
                    f"package {kubernetes_provider_name}=={kubernetes_provider_version} which doesn't "
                    f"support this feature. Please upgrade it to version higher than or equal to {min_version}."
                )
            await self.hook.wait_until_container_complete(
                name=self.pod_name,
                namespace=self.pod_namespace,
                container_name=self.base_container_name,
                poll_interval=self.poll_interval,
            )
            self.log.info("Checking if xcom sidecar container is started.")
            await self.hook.wait_until_container_started(
                name=self.pod_name,
                namespace=self.pod_namespace,
                container_name=PodDefaults.SIDECAR_CONTAINER_NAME,
                poll_interval=self.poll_interval,
            )
            self.log.info("Extracting result from xcom sidecar container.")
            loop = asyncio.get_running_loop()
            xcom_result = await loop.run_in_executor(
                None, self.pod_manager.extract_xcom, pod
            )
        job: V1Job = await self.hook.wait_until_job_complete(
            name=self.job_name,
            namespace=self.job_namespace,
            poll_interval=self.poll_interval,
        )
        job_dict = job.to_dict()
        error_message = self.hook.is_job_failed(job=job)
        status = "error" if error_message else "success"
        message = (
            f"Job failed with error: {error_message}"
            if error_message
            else "Job completed successfully"
        )
        yield TriggerEvent(
            {
                "name": job.metadata.name,
                "namespace": job.metadata.namespace,
                "pod_name": pod.metadata.name if self.get_logs else None,
                "pod_namespace": pod.metadata.namespace if self.get_logs else None,
                "status": status,
                "message": message,
                "job": job_dict,
                "xcom_result": xcom_result if self.do_xcom_push else None,
            }
        )

    @cached_property
    def hook(self) -> GKEKubernetesAsyncHook:
        return GKEKubernetesAsyncHook(
            cluster_url=self.cluster_url,
            ssl_ca_cert=self.ssl_ca_cert,
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    @cached_property
    def pod_manager(self) -> PodManager:
        sync_hook = GKEKubernetesHook(
            gcp_conn_id=self.gcp_conn_id,
            cluster_url=self.cluster_url,
            ssl_ca_cert=self.ssl_ca_cert,
            impersonation_chain=self.impersonation_chain,
        )
        return PodManager(kube_client=sync_hook.core_v1_client)
