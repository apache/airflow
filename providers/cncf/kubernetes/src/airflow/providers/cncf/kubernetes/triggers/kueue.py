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
"""Wait for a Kubernetes Job run through Kueue."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any

from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.hooks.kubernetes import (
    JOB_FINAL_STATUS_CONDITION_TYPES,
    AsyncKubernetesHook,
    KubernetesHook,
)
from airflow.providers.cncf.kubernetes.utils.container import container_is_running, container_is_terminated
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, XComRetrievalError
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from kubernetes.client import CoreV1Api, models as k8s

# Set by the Job controller on the pods it creates for a Job
JOB_NAME_LABEL = "batch.kubernetes.io/job-name"


class KueueJobState(str, Enum):
    """State of a Job run through Kueue."""

    SUSPENDED = "suspended"
    RUNNING = "running"
    FINISHED = "finished"


JOB_STATE_MESSAGES = {
    KueueJobState.SUSPENDED: "is suspended: waiting for admission by Kueue, or preempted by Kueue",
    KueueJobState.RUNNING: "is admitted by Kueue: its pods are running",
    KueueJobState.FINISHED: "is finished",
}


def get_job_state(job: k8s.V1Job) -> KueueJobState:
    """Return the state of a Job run through Kueue; finished means Complete or Failed."""
    conditions = (job.status.conditions if job.status else None) or []
    if any(c.type in JOB_FINAL_STATUS_CONDITION_TYPES and c.status == "True" for c in conditions):
        return KueueJobState.FINISHED
    # The Job is created suspended; Kueue resumes it on admission and suspends it again on preemption
    if job.spec and job.spec.suspend:
        return KueueJobState.SUSPENDED
    return KueueJobState.RUNNING


def build_job_event(job: k8s.V1Job, xcom_results: dict[str, str]) -> dict[str, Any]:
    """
    Build the event of a finished Job, as passed to ``KubernetesStartKueueJobOperator.execute_complete``.

    :param job: the finished Job.
    :param xcom_results: the raw XCom read from each pod, by pod name.
    """
    error_message = KubernetesHook.is_job_failed(job=job)
    return {
        "name": job.metadata.name,
        "namespace": job.metadata.namespace,
        "status": "error" if error_message else "success",
        "message": f"Job failed with error: {error_message}"
        if error_message
        else "Job completed successfully",
        "job": job.to_dict(),
        "xcom_result": xcom_results,
    }


def get_succeeded_pod_xcoms(pods: Iterable[k8s.V1Pod], xcom_results: dict[str, str]) -> list[str]:
    """Return the XCom read from the pods that succeeded, skipping failed pods and preempted runs."""
    return [
        xcom_results[pod.metadata.name]
        for pod in pods
        if pod.metadata.name in xcom_results and pod.status and pod.status.phase == "Succeeded"
    ]


def collect_xcom_results(
    client: CoreV1Api,
    pod_manager: PodManager,
    job_name: str,
    namespace: str,
    base_container_name: str,
    xcom_results: dict[str, str],
) -> None:
    """
    Read into ``xcom_results`` the XCom of each pod of the Job whose base container has ended.

    Reading the XCom also stops the XCom sidecar: until then the pod, and so the Job, cannot finish.
    Pods already read are skipped, so this is called at each poll.
    """
    pods = client.list_namespaced_pod(
        namespace=namespace, label_selector=f"{JOB_NAME_LABEL}={job_name}"
    ).items
    for pod in pods:
        if (
            pod.metadata.name in xcom_results
            or not container_is_terminated(pod, base_container_name)
            or not container_is_running(pod, PodDefaults.SIDECAR_CONTAINER_NAME)
        ):
            continue
        try:
            xcom_results[pod.metadata.name] = pod_manager.extract_xcom(pod, ignore_kill_failure=False)
        except XComRetrievalError as e:
            pod_manager.log.warning("Cannot read the XCom of the pod %s: %s", pod.metadata.name, e)
        except ApiException as e:
            if e.status != 404:
                raise
            pod_manager.log.warning("The pod %s was deleted before its XCom was read", pod.metadata.name)


class KubernetesKueueJobTrigger(BaseTrigger):
    """
    Wait until a Job run through Kueue is finished, whatever Kueue does with it meanwhile.

    Unlike :class:`~airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger`, it does not wait
    on pods found when the Job started: Kueue may preempt the Job (its pods are deleted) and admit it again
    (new pods start) any number of times. Only the Job itself is polled, and the XCom are read from the
    pods the Job has at each poll.

    :param job_name: The name of the job.
    :param job_namespace: The namespace of the job.
    :param base_container_name: The name of the base container in the pod.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param cluster_context: Context that points to kubernetes cluster.
    :param config_file: Path to kubeconfig file.
    :param poll_interval: Polling period in seconds to check for the status.
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param do_xcom_push: If True, the content of the file
        /airflow/xcom/return.json in the container will also be pushed to an
        XCom when the container completes.
    """

    def __init__(
        self,
        job_name: str,
        job_namespace: str,
        base_container_name: str,
        kubernetes_conn_id: str | None = None,
        poll_interval: float = 10.0,
        cluster_context: str | None = None,
        config_file: str | None = None,
        in_cluster: bool | None = None,
        do_xcom_push: bool = False,
    ):
        super().__init__()
        self.job_name = job_name
        self.job_namespace = job_namespace
        self.base_container_name = base_container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.do_xcom_push = do_xcom_push

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize KubernetesKueueJobTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.kueue.KubernetesKueueJobTrigger",
            {
                "job_name": self.job_name,
                "job_namespace": self.job_namespace,
                "base_container_name": self.base_container_name,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "config_file": self.config_file,
                "in_cluster": self.in_cluster,
                "do_xcom_push": self.do_xcom_push,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll the Job until it is finished and yield a TriggerEvent."""
        state: KueueJobState | None = None
        xcom_results: dict[str, str] = {}
        while True:
            job = await self.hook.get_job_status(name=self.job_name, namespace=self.job_namespace)
            if (new_state := get_job_state(job)) != state:
                state = new_state
                self.log.info("The job '%s' %s", self.job_name, JOB_STATE_MESSAGES[state])
            if state == KueueJobState.FINISHED:
                yield TriggerEvent(build_job_event(job, xcom_results))
                return
            if state == KueueJobState.RUNNING and self.do_xcom_push:
                await asyncio.to_thread(self._collect_xcom_results, xcom_results)
            await asyncio.sleep(self.poll_interval)

    def _collect_xcom_results(self, xcom_results: dict[str, str]) -> None:
        # Run in a thread: the sync client loads its connection and kube config on first use
        collect_xcom_results(
            self.sync_hook.core_v1_client,
            self.pod_manager,
            self.job_name,
            self.job_namespace,
            self.base_container_name,
            xcom_results,
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
    def sync_hook(self) -> KubernetesHook:
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    @cached_property
    def pod_manager(self) -> PodManager:
        return PodManager(kube_client=self.sync_hook.core_v1_client)
