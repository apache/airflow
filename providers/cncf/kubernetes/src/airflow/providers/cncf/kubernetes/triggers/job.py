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
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from kubernetes.client.rest import ApiException as SyncApiException
from kubernetes_asyncio.client.exceptions import ApiException as AsyncApiException

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook, KubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from kubernetes.client import V1Job


WaitOutcome = Literal["ready", "job_done", "pod_missing"]
XComExtractOutcome = Literal["success", "missing", "error", "timeout"]
K8S_API_EXCEPTIONS = (SyncApiException, AsyncApiException)
MIN_POST_JOB_XCOM_TIMEOUT = 0.1


@dataclass(frozen=True, slots=True)
class PodXComAttempt:
    """Outcome of a single best-effort XCom extraction attempt for one pod."""

    pod_name: str
    outcome: XComExtractOutcome
    result: str | None = None


@dataclass(frozen=True, slots=True)
class PostJobXComSummary:
    """Aggregated counters for post-job best-effort XCom extraction attempts."""

    total: int
    succeeded: int
    skipped_missing: int
    timed_out: int
    failed_other: int


class KubernetesJobTrigger(BaseTrigger):
    """
    KubernetesJobTrigger run on the trigger worker to check the state of Job.

    :param job_name: The name of the job.
    :param job_namespace: The namespace of the job.
    :param pod_name: The name of the Pod. Parameter is deprecated, please use pod_names instead.
    :param pod_names: The name of the Pods.
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
        pod_names: list[str],
        pod_namespace: str,
        base_container_name: str,
        pod_name: str | None = None,
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
        if pod_name is not None:
            self._pod_name = pod_name
            self.pod_names = [
                self.pod_name,
            ]
        else:
            self.pod_names = pod_names
        self.pod_namespace = pod_namespace
        self.base_container_name = base_container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        if poll_interval <= 0:
            raise ValueError("Invalid value for poll_interval. Expected value greater than 0")
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.get_logs = get_logs
        self.do_xcom_push = do_xcom_push

    @property
    def pod_name(self):
        warnings.warn(
            "`pod_name` parameter is deprecated, please use `pod_names`",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return self._pod_name

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize KubernetesCreateJobTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger",
            {
                "job_name": self.job_name,
                "job_namespace": self.job_namespace,
                "pod_names": self.pod_names,
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

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current job status and yield a TriggerEvent."""
        xcom_results: list[str] | None = None

        job_task = asyncio.create_task(
            self.hook.wait_until_job_complete(
                name=self.job_name,
                namespace=self.job_namespace,
                poll_interval=self.poll_interval,
            )
        )
        try:
            if self.do_xcom_push:
                xcom_results = await self._collect_xcom_results(
                    job_task=job_task,
                )

            job: V1Job = await job_task
        finally:
            if not job_task.done():
                job_task.cancel()
                with suppress(asyncio.CancelledError):
                    await job_task

        job_dict = job.to_dict()
        error_message = self.hook.is_job_failed(job=job)
        yield TriggerEvent(
            {
                "name": job.metadata.name,
                "namespace": job.metadata.namespace,
                "pod_names": [pod_name for pod_name in self.pod_names] if self.get_logs else None,
                "pod_namespace": self.pod_namespace if self.get_logs else None,
                "status": "error" if error_message else "success",
                "message": f"Job failed with error: {error_message}"
                if error_message
                else "Job completed successfully",
                "job": job_dict,
                "xcom_result": xcom_results if self.do_xcom_push else None,
            }
        )

    async def _wait_until_container_state_or_job_done(
        self,
        pod_name: str,
        container_name: str,
        wait_method: Any,
        job_task: asyncio.Task[V1Job],
        state_label: str,
    ) -> WaitOutcome:
        wait_task = asyncio.create_task(
            wait_method(
                name=pod_name,
                namespace=self.pod_namespace,
                container_name=container_name,
                poll_interval=self.poll_interval,
            )
        )

        try:
            while not wait_task.done() and not job_task.done():
                # This timeout controls how frequently we re-check "job done vs container state" and does
                # not enforce container-level readiness timeout.
                await asyncio.wait(
                    {wait_task, job_task},
                    timeout=self.poll_interval,
                    return_when=asyncio.FIRST_COMPLETED,
                )

            if wait_task.done():
                try:
                    await wait_task
                    return "ready"
                except K8S_API_EXCEPTIONS as err:
                    if self._is_not_found_error(err):
                        self.log.info(
                            "Pod '%s' no longer exists while waiting for container '%s' state '%s'; skipping.",
                            pod_name,
                            container_name,
                            state_label,
                        )
                        return "pod_missing"
                    raise

            self.log.info(
                "Job '%s' finished before pod '%s' container '%s' reached state '%s'; stopping XCom wait.",
                self.job_name,
                pod_name,
                container_name,
                state_label,
            )
            return "job_done"
        finally:
            if not wait_task.done():
                wait_task.cancel()
                with suppress(asyncio.CancelledError):
                    await wait_task

    async def _collect_xcom_results(
        self,
        job_task: asyncio.Task[V1Job],
    ) -> list[str]:
        pre_job_results, post_job_pod_names = await self._collect_xcom_until_job_done(job_task=job_task)
        if not post_job_pod_names:
            return pre_job_results

        attempts = await self._collect_xcom_after_job_done_best_effort(
            pod_names=post_job_pod_names,
        )
        summary = self._summarize_post_job_attempts(attempts)
        self.log.info(
            "Best-effort XCom collection summary after job completion: "
            "total=%d, succeeded=%d, skipped_missing=%d, timed_out=%d, failed_other=%d",
            summary.total,
            summary.succeeded,
            summary.skipped_missing,
            summary.timed_out,
            summary.failed_other,
        )
        # Only successful extraction attempts carry a concrete XCom payload.
        pre_job_results.extend(attempt.result for attempt in attempts if attempt.result is not None)
        return pre_job_results

    async def _collect_xcom_until_job_done(
        self,
        job_task: asyncio.Task[V1Job],
    ) -> tuple[list[str], list[str]]:
        xcom_results: list[str] = []
        post_job_pod_names: list[str] = []

        for pod_index, pod_name in enumerate(self.pod_names):
            try:
                pod = await self.hook.get_pod(name=pod_name, namespace=self.pod_namespace)
            except K8S_API_EXCEPTIONS as err:
                if self._is_not_found_error(err):
                    self.log.info("Pod '%s' no longer exists; skipping XCom extraction.", pod_name)
                    continue
                raise

            completion_outcome = await self._wait_until_container_state_or_job_done(
                pod_name=pod_name,
                container_name=self.base_container_name,
                wait_method=self.hook.wait_until_container_complete,
                job_task=job_task,
                state_label="completed",
            )
            if completion_outcome == "job_done":
                post_job_pod_names = self.pod_names[pod_index:]
                break
            if completion_outcome == "pod_missing":
                continue

            self.log.info("Checking if xcom sidecar container is started.")
            sidecar_outcome = await self._wait_until_container_state_or_job_done(
                pod_name=pod_name,
                container_name=PodDefaults.SIDECAR_CONTAINER_NAME,
                wait_method=self.hook.wait_until_container_started,
                job_task=job_task,
                state_label="running",
            )
            if sidecar_outcome == "job_done":
                post_job_pod_names = self.pod_names[pod_index:]
                break
            if sidecar_outcome == "pod_missing":
                continue

            self.log.info("Extracting result from xcom sidecar container.")
            loop = asyncio.get_running_loop()
            xcom_results.append(await loop.run_in_executor(None, self.pod_manager.extract_xcom, pod))

        return xcom_results, post_job_pod_names

    async def _collect_xcom_after_job_done_best_effort(
        self,
        pod_names: list[str],
    ) -> list[PodXComAttempt]:
        if not pod_names:
            return []

        # Post-job extraction is best-effort: bound each pod attempt so stale pods cannot block trigger
        # completion. Keep a small floor so tiny poll intervals do not cause immediate timeout.
        per_pod_timeout = max(self.poll_interval, MIN_POST_JOB_XCOM_TIMEOUT)
        max_concurrency = min(5, len(pod_names))
        semaphore = asyncio.Semaphore(max_concurrency)

        self.log.info(
            "Job is done; collecting XCom best-effort for %d pod(s) with per-pod timeout %.2f seconds and max concurrency %d.",
            len(pod_names),
            per_pod_timeout,
            max_concurrency,
        )

        async def extract_one_pod(pod_name: str) -> PodXComAttempt:
            async with semaphore:
                try:
                    return await asyncio.wait_for(
                        self._extract_xcom_for_pod_best_effort(pod_name=pod_name),
                        timeout=per_pod_timeout,
                    )
                except asyncio.TimeoutError:
                    self.log.warning(
                        "Timed out extracting XCom from pod '%s' after job completion; skipping.",
                        pod_name,
                    )
                    return PodXComAttempt(pod_name=pod_name, outcome="timeout")

        return await asyncio.gather(*(extract_one_pod(pod_name) for pod_name in pod_names))

    async def _extract_xcom_for_pod_best_effort(self, pod_name: str) -> PodXComAttempt:
        try:
            pod = await self.hook.get_pod(name=pod_name, namespace=self.pod_namespace)
        except K8S_API_EXCEPTIONS as err:
            if self._is_not_found_error(err):
                self.log.info("Pod '%s' no longer exists; skipping XCom extraction.", pod_name)
                return PodXComAttempt(pod_name=pod_name, outcome="missing")
            raise

        self.log.info("Extracting result from xcom sidecar container (best-effort).")
        loop = asyncio.get_running_loop()
        try:
            result = await loop.run_in_executor(None, self.pod_manager.extract_xcom, pod)
            return PodXComAttempt(pod_name=pod_name, outcome="success", result=result)
        except Exception as err:
            if self._is_not_found_error(err):
                self.log.info("Pod '%s' no longer exists during XCom extraction; skipping.", pod_name)
                return PodXComAttempt(pod_name=pod_name, outcome="missing")
            self.log.warning("Unable to extract XCom from pod '%s': %r. Skipping.", pod_name, err)
            return PodXComAttempt(pod_name=pod_name, outcome="error")

    @staticmethod
    def _summarize_post_job_attempts(attempts: list[PodXComAttempt]) -> PostJobXComSummary:
        return PostJobXComSummary(
            total=len(attempts),
            succeeded=sum(1 for attempt in attempts if attempt.outcome == "success"),
            skipped_missing=sum(1 for attempt in attempts if attempt.outcome == "missing"),
            timed_out=sum(1 for attempt in attempts if attempt.outcome == "timeout"),
            failed_other=sum(1 for attempt in attempts if attempt.outcome == "error"),
        )

    @staticmethod
    def _is_not_found_error(err: Exception) -> bool:
        return getattr(err, "status", None) == 404

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
        sync_hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        return PodManager(kube_client=sync_hook.core_v1_client)
