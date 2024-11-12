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
from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook, KubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import provide_session
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from kubernetes.client import V1Job


class KubernetesJobTrigger(BaseTrigger):
    """
    KubernetesJobTrigger run on the trigger worker to check the state of Job.

    :param job_name: The name of the job.
    :param job_namespace: The namespace of the job.
    :param pod_name: The name of the Pod.
    :param pod_namespace: The namespace of the Pod.
    :param base_container_name: The name of the base container in the pod.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param cluster_context: Context that points to kubernetes cluster.
    :param config_file: Path to kubeconfig file.
    :param poll_interval: Polling period in seconds to check for the status.
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param get_logs: get the stdout of the base container as logs of the tasks.
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    """

    def __init__(
        self,
        job_name: str,
        job_namespace: str,
        pod_name: str,
        pod_namespace: str,
        base_container_name: str,
        kubernetes_conn_id: str | None = None,
        poll_interval: float = 10.0,
        cluster_context: str | None = None,
        config_file: str | None = None,
        in_cluster: bool | None = None,
        get_logs: bool = True,
        do_xcom_push: bool = False,
    ):
        super().__init__()
        self.job_name = job_name
        self.job_namespace = job_namespace
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.base_container_name = base_container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.get_logs = get_logs
        self.do_xcom_push = do_xcom_push

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize KubernetesCreateJobTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger",
            {
                "job_name": self.job_name,
                "job_namespace": self.job_namespace,
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "base_container_name": self.base_container_name,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "config_file": self.config_file,
                "in_cluster": self.in_cluster,
                "get_logs": self.get_logs,
                "do_xcom_push": self.do_xcom_push,
            },
        )
    
    @provide_session
    def get_task_instance(self, session: Session) -> TaskInstance:
        """
        Get the task instance for the current task.

        :param session: Sqlalchemy session
        """
        query = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.task_instance.dag_id,
            TaskInstance.task_id == self.task_instance.task_id,
            TaskInstance.run_id == self.task_instance.run_id,
            TaskInstance.map_index == self.task_instance.map_index,
        )
        task_instance = query.one_or_none()
        if task_instance is None:
            raise AirflowException(
                "TaskInstance with dag_id: %s,task_id: %s, run_id: %s and map_index: %s is not found",
                self.task_instance.dag_id,
                self.task_instance.task_id,
                self.task_instance.run_id,
                self.task_instance.map_index,
            )
        return task_instance

    def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to cancel the external job which is being executed by this trigger.

        This is to avoid the case that `asyncio.CancelledError` is called because the trigger itself is stopped.
        Because in those cases, we should NOT cancel the external job.
        """
        # Database query is needed to get the latest state of the task instance.
        task_instance = self.get_task_instance()  # type: ignore[call-arg]
        return task_instance.state != TaskInstanceState.DEFERRED


    async def run(self) -> AsyncIterator[TriggerEvent]:  # type: ignore[override]
        """Get current job status and yield a TriggerEvent."""
        if self.get_logs or self.do_xcom_push:
            pod = await self.hook.get_pod(name=self.pod_name, namespace=self.pod_namespace)
        if self.do_xcom_push:
            await self.hook.wait_until_container_complete(
                name=self.pod_name, namespace=self.pod_namespace, container_name=self.base_container_name
            )
            self.log.info("Checking if xcom sidecar container is started.")
            await self.hook.wait_until_container_started(
                name=self.pod_name,
                namespace=self.pod_namespace,
                container_name=PodDefaults.SIDECAR_CONTAINER_NAME,
            )
            self.log.info("Extracting result from xcom sidecar container.")
            loop = asyncio.get_running_loop()
            xcom_result = await loop.run_in_executor(None, self.pod_manager.extract_xcom, pod)
        try:
            job: V1Job = await self.hook.wait_until_job_complete(name=self.job_name, namespace=self.job_namespace)
            job_dict = job.to_dict()
            error_message = self.hook.is_job_failed(job=job)

            yield TriggerEvent(
                {
                    "name": job.metadata.name,
                    "namespace": job.metadata.namespace,
                    "pod_name": pod.metadata.name if self.get_logs else None,
                    "pod_namespace": pod.metadata.namespace if self.get_logs else None,
                    "status": "error" if error_message else "success",
                    "message": f"Job failed with error: {error_message}"
                    if error_message
                    else "Job completed successfully",
                    "job": job_dict,
                    "xcom_result": xcom_result if self.do_xcom_push else None,
                }
            )
        except asyncio.CancelledError:
            try:
                if self.safe_to_cancel():
                    self.log.info("Deleting job from kubernetes cluster")
                    resp = self.sync_hook.batch_v1_client.delete_namespaced_job(
                        name=self.job_name,
                        namespace=self.job_namespace,
                    )
                    self.log.info("Deleting job response: %s", resp)
            except Exception as e:
                self.log.error("Error during cancellation handling: %s", e)
                raise AirflowException("Error during cancellation handling: %s", e)

    @cached_property
    def sync_hook(self) -> KubernetesHook:
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
      
    @cached_property
    def hook(self) -> AsyncKubernetesHook:
        return AsyncKubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self.sync_hook.core_v1_client)
