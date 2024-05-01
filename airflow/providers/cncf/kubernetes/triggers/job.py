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

from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from kubernetes.client import V1Job


class KubernetesJobTrigger(BaseTrigger):
    """
    KubernetesJobTrigger run on the trigger worker to check the state of Job.

    :param job_name: The name of the job.
    :param job_namespace: The namespace of the job.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param cluster_context: Context that points to kubernetes cluster.
    :param config_file: Path to kubeconfig file.
    :param poll_interval: Polling period in seconds to check for the status.
    :param in_cluster: run kubernetes client with in_cluster configuration.
    """

    def __init__(
        self,
        job_name: str,
        job_namespace: str,
        kubernetes_conn_id: str | None = None,
        poll_interval: float = 10.0,
        cluster_context: str | None = None,
        config_file: str | None = None,
        in_cluster: bool | None = None,
    ):
        super().__init__()
        self.job_name = job_name
        self.job_namespace = job_namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize KubernetesCreateJobTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger",
            {
                "job_name": self.job_name,
                "job_namespace": self.job_namespace,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "config_file": self.config_file,
                "in_cluster": self.in_cluster,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current job status and yield a TriggerEvent."""
        job: V1Job = await self.hook.wait_until_job_complete(name=self.job_name, namespace=self.job_namespace)
        job_dict = job.to_dict()
        error_message = self.hook.is_job_failed(job=job)
        yield TriggerEvent(
            {
                "name": job.metadata.name,
                "namespace": job.metadata.namespace,
                "status": "error" if error_message else "success",
                "message": f"Job failed with error: {error_message}"
                if error_message
                else "Job completed successfully",
                "job": job_dict,
            }
        )

    @cached_property
    def hook(self) -> AsyncKubernetesHook:
        return AsyncKubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
