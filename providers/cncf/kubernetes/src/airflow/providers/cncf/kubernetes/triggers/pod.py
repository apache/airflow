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
import datetime
import traceback
from collections.abc import AsyncIterator
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import tenacity
from asgiref.sync import sync_to_async

from airflow.providers.cncf.kubernetes.exceptions import KubernetesApiPermissionError
from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    AsyncPodManager,
    OnFinishAction,
    OnKillAction,
    PodLaunchTimeoutException,
    PodPhase,
)
from airflow.providers.cncf.kubernetes.version_compat import (
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_3_PLUS,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from kubernetes_asyncio.client.models import V1Pod
    from pendulum import DateTime
    from sqlalchemy.orm.session import Session

if not AIRFLOW_V_3_0_PLUS:
    from sqlalchemy import select

    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import provide_session


class ContainerState(str, Enum):
    """
    Possible container states.

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
    :param config_dict: Content of kubeconfig file in dict format.
    :param poll_interval: Polling period in seconds to check for the status.
    :param trigger_start_time: time in Datetime format when the trigger was started
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param get_logs: get the stdout of the container as logs of the tasks.
    :param startup_timeout: timeout in seconds to start up the pod.
    :param startup_check_interval: interval in seconds to check if the pod has already started.
    :param schedule_timeout: timeout in seconds to schedule pod in cluster.
    :param on_finish_action: What to do when the pod reaches its final state, or the execution is interrupted.
        If "delete_pod", the pod will be deleted regardless its state; if "delete_succeeded_pod",
        only succeeded pod will be deleted. You can set to "keep_pod" to keep the pod.
    :param on_kill_action: What to do when the trigger is cancelled (e.g. when a deferred task is
        manually marked as success/failed). If "delete_pod" (default), the pod will be deleted.
        If "keep_pod", the pod will not be deleted.
    :param termination_grace_period: Optional grace period in seconds for pod termination during cleanup.
    :param logging_interval: number of seconds to wait before kicking it back to
        the operator to print latest logs. If ``None`` will wait until container done.
    :param last_log_time: where to resume logs from
    :param trigger_kwargs: additional keyword parameters to send in the event
    """

    def __init__(
        self,
        pod_name: str,
        pod_namespace: str,
        trigger_start_time: datetime.datetime,
        base_container_name: str,
        kubernetes_conn_id: str | None = None,
        connection_extras: dict | None = None,
        poll_interval: float = 2,
        cluster_context: str | None = None,
        config_dict: dict | None = None,
        in_cluster: bool | None = None,
        get_logs: bool = True,
        startup_timeout: int = 120,
        startup_check_interval: float = 5,
        schedule_timeout: int = 120,
        on_finish_action: str = "delete_pod",
        on_kill_action: str = "delete_pod",
        termination_grace_period: int | None = None,
        last_log_time: DateTime | None = None,
        logging_interval: int | None = None,
        trigger_kwargs: dict | None = None,
    ):
        super().__init__()
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.trigger_start_time = trigger_start_time
        self.base_container_name = base_container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.connection_extras = connection_extras
        self.poll_interval = poll_interval
        self.cluster_context = cluster_context
        self.config_dict = config_dict
        self.in_cluster = in_cluster
        self.get_logs = get_logs
        self.startup_timeout = startup_timeout
        self.startup_check_interval = startup_check_interval
        self.schedule_timeout = schedule_timeout
        self.last_log_time = last_log_time
        self.logging_interval = logging_interval
        self.on_finish_action = OnFinishAction(on_finish_action)
        self.on_kill_action = OnKillAction(on_kill_action)
        self.termination_grace_period = termination_grace_period
        self.trigger_kwargs = trigger_kwargs or {}
        self._fired_event = False
        self._since_time = None

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize KubernetesCreatePodTrigger arguments and classpath."""
        return (
            "airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger",
            {
                "pod_name": self.pod_name,
                "pod_namespace": self.pod_namespace,
                "base_container_name": self.base_container_name,
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "connection_extras": self.connection_extras,
                "poll_interval": self.poll_interval,
                "cluster_context": self.cluster_context,
                "config_dict": self.config_dict,
                "in_cluster": self.in_cluster,
                "get_logs": self.get_logs,
                "startup_timeout": self.startup_timeout,
                "startup_check_interval": self.startup_check_interval,
                "schedule_timeout": self.schedule_timeout,
                "trigger_start_time": self.trigger_start_time,
                "on_finish_action": self.on_finish_action.value,
                "on_kill_action": self.on_kill_action.value,
                "termination_grace_period": self.termination_grace_period,
                "last_log_time": self.last_log_time,
                "logging_interval": self.logging_interval,
                "trigger_kwargs": self.trigger_kwargs,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Get current pod status and yield a TriggerEvent."""
        self.log.info(
            "Checking pod %r in namespace %r with poll interval %r.",
            self.pod_name,
            self.pod_namespace,
            self.poll_interval,
        )
        try:
            state = await self._wait_for_pod_start()
            if state == ContainerState.TERMINATED:
                event = TriggerEvent(
                    {
                        "status": "success",
                        "namespace": self.pod_namespace,
                        "name": self.pod_name,
                        "message": "All containers inside pod have started successfully.",
                        **self.trigger_kwargs,
                    }
                )
            elif state == ContainerState.FAILED:
                event = TriggerEvent(
                    {
                        "status": "failed",
                        "namespace": self.pod_namespace,
                        "name": self.pod_name,
                        "message": "pod failed",
                        **self.trigger_kwargs,
                    }
                )
            else:
                event = await self._wait_for_container_completion()
            self._fired_event = True
            yield event
            return
        except PodLaunchTimeoutException as e:
            message = self._format_exception_description(e)
            self._fired_event = True
            yield TriggerEvent(
                {
                    "name": self.pod_name,
                    "namespace": self.pod_namespace,
                    "status": "timeout",
                    "message": message,
                    **self.trigger_kwargs,
                }
            )
            return
        except KubernetesApiPermissionError as e:
            message = (
                "Kubernetes API permission error: The triggerer may not have sufficient permissions to monitor or delete pods. "
                "Please ensure the triggerer's service account is included in the 'pod-launcher-role' as defined in the latest Airflow Helm chart. "
                f"Original error: {e}"
            )
            self._fired_event = True
            yield TriggerEvent(
                {
                    "name": self.pod_name,
                    "namespace": self.pod_namespace,
                    "status": "error",
                    "message": message,
                    **self.trigger_kwargs,
                }
            )
            return
        except Exception as e:
            self.log.exception(
                "Unexpected error while waiting for pod %s in namespace %s",
                self.pod_name,
                self.pod_namespace,
            )
            self._fired_event = True
            yield TriggerEvent(
                {
                    "name": self.pod_name,
                    "namespace": self.pod_namespace,
                    "status": "error",
                    "message": str(e),
                    "stack_trace": traceback.format_exc(),
                    **self.trigger_kwargs,
                }
            )
            return

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

    async def _wait_for_pod_start(self) -> ContainerState:
        """Loops until pod phase leaves ``PENDING`` If timeout is reached, throws error."""
        pod = await self._get_pod()
        # Start event stream in background
        events_task = asyncio.create_task(self.pod_manager.watch_pod_events(pod, self.startup_check_interval))

        # Await pod start completion
        try:
            await self.pod_manager.await_pod_start(
                pod=pod,
                schedule_timeout=self.schedule_timeout,
                startup_timeout=self.startup_timeout,
                check_interval=self.startup_check_interval,
            )
        finally:
            # Stop watching events
            events_task.cancel()
            try:
                await events_task
            except asyncio.CancelledError:
                pass

        return self.define_container_state(await self._get_pod())

    async def _wait_for_container_completion(self) -> TriggerEvent:
        """
        Wait for container completion.

        Waits until container is no longer in running state. If trigger is configured with a logging period,
        then will emit an event to resume the task for the purpose of fetching more logs.
        """
        time_begin = datetime.datetime.now(tz=datetime.timezone.utc)
        time_get_more_logs = None
        if self.logging_interval is not None:
            time_get_more_logs = time_begin + datetime.timedelta(seconds=self.logging_interval)
        while True:
            pod = await self._get_pod()
            container_state = self.define_container_state(pod)
            if container_state == ContainerState.TERMINATED:
                return TriggerEvent(
                    {
                        "status": "success",
                        "namespace": self.pod_namespace,
                        "name": self.pod_name,
                        "last_log_time": self.last_log_time,
                        **self.trigger_kwargs,
                    }
                )
            if container_state == ContainerState.FAILED:
                return TriggerEvent(
                    {
                        "status": "failed",
                        "namespace": self.pod_namespace,
                        "name": self.pod_name,
                        "message": "Container state failed",
                        "last_log_time": self.last_log_time,
                        **self.trigger_kwargs,
                    }
                )
            self.log.debug("Container is not completed and still working.")
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            if time_get_more_logs and now >= time_get_more_logs:
                if self.get_logs and self.logging_interval:
                    self.last_log_time = await self.pod_manager.fetch_container_logs_before_current_sec(
                        pod, container_name=self.base_container_name, since_time=self.last_log_time
                    )
                    time_get_more_logs = now + datetime.timedelta(seconds=self.logging_interval)

            self.log.debug("Sleeping for %s seconds.", self.poll_interval)
            await asyncio.sleep(self.poll_interval)

    @tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(), reraise=True)
    async def _get_pod(self) -> V1Pod:
        """Get the pod from Kubernetes with retries."""
        pod = await self.hook.get_pod(name=self.pod_name, namespace=self.pod_namespace)
        # Due to AsyncKubernetesHook overriding get_pod, we need to cast the return
        # value to kubernetes_asyncio.V1Pod, because it's perceived as different type
        return cast("V1Pod", pod)

    @cached_property
    def hook(self) -> AsyncKubernetesHook:
        return AsyncKubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_dict=self.config_dict,
            cluster_context=self.cluster_context,
            connection_extras=self.connection_extras,
        )

    @cached_property
    def pod_manager(self) -> AsyncPodManager:
        return AsyncPodManager(async_hook=self.hook)

    if not AIRFLOW_V_3_0_PLUS:

        @provide_session
        def get_task_instance(self, session: Session) -> TaskInstance:
            """Get the task instance for this trigger from the database (Airflow 2.x only)."""
            task_instance = session.scalar(
                select(TaskInstance).where(
                    TaskInstance.dag_id == self.task_instance.dag_id,
                    TaskInstance.task_id == self.task_instance.task_id,
                    TaskInstance.run_id == self.task_instance.run_id,
                    TaskInstance.map_index == self.task_instance.map_index,
                )
            )
            if task_instance is None:
                raise AirflowException(
                    "TaskInstance with dag_id: %s, task_id: %s, run_id: %s and map_index: %s is not found",
                    self.task_instance.dag_id,
                    self.task_instance.task_id,
                    self.task_instance.run_id,
                    self.task_instance.map_index,
                )
            return task_instance

    async def get_task_state(self):
        """Get the current state of the task instance."""
        if AIRFLOW_V_3_0_PLUS:
            from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

            task_states_response = await sync_to_async(RuntimeTaskInstance.get_task_states)(
                dag_id=self.task_instance.dag_id,
                task_ids=[self.task_instance.task_id],
                run_ids=[self.task_instance.run_id],
                map_index=self.task_instance.map_index,
            )
            try:
                return task_states_response[self.task_instance.run_id][self.task_instance.task_id]
            except KeyError:
                raise AirflowException(
                    "TaskInstance with dag_id: %s, task_id: %s, run_id: %s and map_index: %s is not found",
                    self.task_instance.dag_id,
                    self.task_instance.task_id,
                    self.task_instance.run_id,
                    self.task_instance.map_index,
                )
        else:
            task_instance = await sync_to_async(self.get_task_instance)()  # type: ignore[call-arg]
            return task_instance.state

    async def safe_to_cancel(self) -> bool:
        """
        Whether it is safe to delete the pod during trigger cleanup.

        Used only on Airflow < 3.3.0 where the triggerer does not invoke ``on_kill()`` for user kills.
        Deletion is NOT safe when the task is still in DEFERRED state (triggerer restart).
        """
        task_state = await self.get_task_state()
        return task_state != TaskInstanceState.DEFERRED

    async def on_kill(self) -> None:
        """
        Delete the pod when the trigger is cancelled by a user action.

        The triggerer invokes this for user-initiated kills on Airflow 3.3.0+ only; on older versions
        use ``cleanup()`` and ``safe_to_cancel()`` instead.
        """
        if self._fired_event:
            self.log.debug("Skipping on_kill since an event has already been fired.")
            return

        if self.on_kill_action == OnKillAction.KEEP_POD:
            self.log.debug("Skipping on_kill since on_kill_action is set to %r.", self.on_kill_action.value)
            return

        self.log.info("Deleting pod %s in namespace %s.", self.pod_name, self.pod_namespace)
        try:
            await self.hook.delete_pod(
                name=self.pod_name,
                namespace=self.pod_namespace,
                grace_period_seconds=self.termination_grace_period,
            )
        except Exception:
            self.log.exception("Unexpected error while deleting pod %s", self.pod_name)

    async def cleanup(self) -> None:
        """
        Clean up the pod when the trigger exits.

        On Airflow 3.3.0+ pod deletion on user kill is handled in ``on_kill()`` only; this avoids
        deleting pods on triggerer restart. On older Airflow versions, ``cleanup()`` still uses
        ``safe_to_cancel()`` because ``on_kill()`` is not wired for user kills.
        """
        # TODO: Remove this Airflow < 3.3 cleanup branch (early return, ``safe_to_cancel``, and
        # related tests) once the minimum Airflow version supported by this provider is >= 3.3.
        # In Airflow 3.3+, ``BaseTrigger.on_kill()`` handles user-initiated kills; keeping the
        # legacy path for backward compatibility with older Airflow versions.
        if AIRFLOW_V_3_3_PLUS:
            return

        if self._fired_event:
            self.log.debug("Skipping cleanup since an event has already been fired.")
            return

        if self.on_kill_action == OnKillAction.KEEP_POD:
            self.log.debug("Skipping cleanup since on_kill_action is set to %r.", self.on_kill_action.value)
            return

        try:
            safe = await self.safe_to_cancel()
        except Exception:
            self.log.warning(
                "Could not determine task state during cleanup; skipping pod deletion to be safe.",
                exc_info=True,
            )
            return

        if not safe:
            self.log.debug(
                "Skipping cleanup since the task is still in deferred state (likely a triggerer restart)."
            )
            return

        self.log.info("Deleting pod %s in namespace %s.", self.pod_name, self.pod_namespace)
        try:
            await self.hook.delete_pod(
                name=self.pod_name,
                namespace=self.pod_namespace,
                grace_period_seconds=self.termination_grace_period,
            )
        except Exception:
            self.log.exception("Unexpected error while deleting pod %s", self.pod_name)

    def define_container_state(self, pod: V1Pod) -> ContainerState:
        if pod.status is None or pod.status.container_statuses is None:
            return ContainerState.UNDEFINED

        container = next(c for c in pod.status.container_statuses if c.name == self.base_container_name)

        for state in (ContainerState.RUNNING, ContainerState.WAITING, ContainerState.TERMINATED):
            state_obj = getattr(container.state, state)
            if state_obj is not None:
                if state != ContainerState.TERMINATED:
                    return state
                return ContainerState.TERMINATED if state_obj.exit_code == 0 else ContainerState.FAILED
        return ContainerState.UNDEFINED

    @staticmethod
    def should_wait(pod_phase: PodPhase, container_state: ContainerState) -> bool:
        return (
            container_state == ContainerState.WAITING
            or container_state == ContainerState.RUNNING
            or (container_state == ContainerState.UNDEFINED and pod_phase == PodPhase.PENDING)
        )
