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
import traceback
from datetime import timedelta
from typing import Any, AsyncIterator

from kubernetes_asyncio.client import CoreV1Api
from pendulum import DateTime

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase, container_is_running
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone


class PodLaunchTimeoutException(AirflowException):
    """When pod does not leave the ``Pending`` phase within specified timeout."""


class WaitContainerTrigger(BaseTrigger):
    """
    First, waits for pod ``pod_name`` to reach running state within ``pending_phase_timeout``.
    Next, waits for ``container_name`` to reach a terminal state.

    :param kubernetes_conn_id: Airflow connection ID to use
    :param hook_params: kwargs for hook
    :param container_name: container to wait for
    :param pod_name: name of pod to monitor
    :param pod_namespace: pod namespace
    :param pending_phase_timeout: max time in seconds to wait for pod to leave pending phase
    :param poll_interval: number of seconds between reading pod state
    :param logging_interval: number of seconds to wait before kicking it back to
        the operator to print latest logs. If ``None`` will wait until container done.
    :param last_log_time: where to resume logs from
    """

    def __init__(
        self,
        *,
        container_name: str,
        pod_name: str,
        pod_namespace: str,
        kubernetes_conn_id: str | None = None,
        hook_params: dict[str, Any] | None = None,
        pending_phase_timeout: float = 120,
        poll_interval: float = 5,
        logging_interval: int | None = None,
        last_log_time: DateTime | None = None,
    ):
        super().__init__()
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook_params = hook_params
        self.container_name = container_name
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.pending_phase_timeout = pending_phase_timeout
        self.poll_interval = poll_interval
        self.logging_interval = logging_interval
        self.last_log_time = last_log_time

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger",
            {
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "hook_params": self.hook_params,
                "pod_name": self.pod_name,
                "container_name": self.container_name,
                "pod_namespace": self.pod_namespace,
                "pending_phase_timeout": self.pending_phase_timeout,
                "poll_interval": self.poll_interval,
                "logging_interval": self.logging_interval,
                "last_log_time": self.last_log_time,
            },
        )

    async def get_hook(self) -> AsyncKubernetesHook:
        return AsyncKubernetesHook(conn_id=self.kubernetes_conn_id, **(self.hook_params or {}))

    async def wait_for_pod_start(self, v1_api: CoreV1Api) -> Any:
        """
        Loops until pod phase leaves ``PENDING``
        If timeout is reached, throws error.
        """
        start_time = timezone.utcnow()
        timeout_end = start_time + timedelta(seconds=self.pending_phase_timeout)
        while timeout_end > timezone.utcnow():
            pod = await v1_api.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not pod.status.phase == "Pending":
                return pod.status.phase
            await asyncio.sleep(self.poll_interval)
        raise PodLaunchTimeoutException("Pod did not leave 'Pending' phase within specified timeout")

    async def wait_for_container_completion(self, v1_api: CoreV1Api) -> "TriggerEvent":
        """
        Waits until container ``self.container_name`` is no longer in running state.
        If trigger is configured with a logging period, then will emit an event to
        resume the task for the purpose of fetching more logs.
        """
        time_begin = timezone.utcnow()
        time_get_more_logs = None
        if self.logging_interval is not None:
            time_get_more_logs = time_begin + timedelta(seconds=self.logging_interval)
        while True:
            pod = await v1_api.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not container_is_running(pod=pod, container_name=self.container_name):
                return TriggerEvent({"status": "done"})
            if time_get_more_logs and timezone.utcnow() > time_get_more_logs:
                return TriggerEvent({"status": "running", "last_log_time": self.last_log_time})
            await asyncio.sleep(self.poll_interval)

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        self.log.debug("Checking pod %r in namespace %r.", self.pod_name, self.pod_namespace)
        try:
            hook = await self.get_hook()
            async with await hook.get_api_client_async() as api_client:
                v1_api = CoreV1Api(api_client)
                state = await self.wait_for_pod_start(v1_api)
                if state in PodPhase.terminal_states:
                    event = TriggerEvent({"status": "done"})
                else:
                    event = await self.wait_for_container_completion(v1_api)
            yield event
        except Exception as e:
            description = self._format_exception_description(e)
            yield TriggerEvent(
                {
                    "status": "error",
                    "error_type": e.__class__.__name__,
                    "description": description,
                }
            )

    def _format_exception_description(self, exc: Exception) -> Any:
        if isinstance(exc, PodLaunchTimeoutException):
            return exc.args[0]

        description = f"Trigger {self.__class__.__name__} failed with exception {exc.__class__.__name__}."
        message = exc.args and exc.args[0] or ""
        if message:
            description += f"\ntrigger exception message: {message}"
        curr_traceback = traceback.format_exc()
        description += f"\ntrigger traceback:\n{curr_traceback}"
        return description
