#
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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from deprecated import deprecated

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.executors.base_executor import BaseExecutor
from airflow.providers.celery.executors.celery_executor import AIRFLOW_V_3_0_PLUS, CeleryExecutor
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

if TYPE_CHECKING:
    from airflow.callbacks.base_callback_sink import BaseCallbackSink
    from airflow.callbacks.callback_requests import CallbackRequest
    from airflow.cli.cli_config import GroupCommand
    from airflow.executors.base_executor import EventBufferValueType
    from airflow.models.taskinstance import (  # type: ignore[attr-defined]
        SimpleTaskInstance,
        TaskInstance,
    )
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor

    CommandType = Sequence[str]


class CeleryKubernetesExecutor(BaseExecutor):
    """
    CeleryKubernetesExecutor consists of CeleryExecutor and KubernetesExecutor.

    It chooses an executor to use based on the queue defined on the task.
    When the queue is the value of ``kubernetes_queue`` in section ``[celery_kubernetes_executor]``
    of the configuration (default value: `kubernetes`), KubernetesExecutor is selected to run the task,
    otherwise, CeleryExecutor is used.
    """

    supports_ad_hoc_ti_run: bool = True
    # TODO: Remove this flag once providers depend on Airflow 3.0
    supports_pickling: bool = True
    supports_sentry: bool = False

    is_local: bool = False
    is_single_threaded: bool = False
    is_production: bool = True

    serve_logs: bool = False

    callback_sink: BaseCallbackSink | None = None

    @cached_property
    @providers_configuration_loaded
    def kubernetes_queue(self) -> str:
        return conf.get("celery_kubernetes_executor", "kubernetes_queue")

    def __init__(
        self,
        celery_executor: CeleryExecutor | None = None,
        kubernetes_executor: KubernetesExecutor | None = None,
    ):
        if AIRFLOW_V_3_0_PLUS or not kubernetes_executor or not celery_executor:
            raise RuntimeError(
                f"{self.__class__.__name__} does not support Airflow 3.0+. See "
                "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html#using-multiple-executors-concurrently"
                " how to use multiple executors concurrently."
            )

        super().__init__()
        self._job_id: int | str | None = None
        self.celery_executor = celery_executor
        self.kubernetes_executor = kubernetes_executor
        self.kubernetes_executor.kubernetes_queue = self.kubernetes_queue

    @property
    def _task_event_logs(self):
        self.celery_executor._task_event_logs += self.kubernetes_executor._task_event_logs
        self.kubernetes_executor._task_event_logs.clear()
        return self.celery_executor._task_event_logs

    @_task_event_logs.setter
    def _task_event_logs(self, value):
        """Not implemented for hybrid executors."""

    @property
    def queued_tasks(self) -> dict[TaskInstanceKey, Any]:
        """Return queued tasks from celery and kubernetes executor."""
        queued_tasks = self.celery_executor.queued_tasks.copy()
        queued_tasks.update(self.kubernetes_executor.queued_tasks)

        return queued_tasks

    @queued_tasks.setter
    def queued_tasks(self, value) -> None:
        """Not implemented for hybrid executors."""

    @property
    def running(self) -> set[TaskInstanceKey]:
        """Return running tasks from celery and kubernetes executor."""
        return self.celery_executor.running.union(self.kubernetes_executor.running)

    @running.setter
    def running(self, value) -> None:
        """Not implemented for hybrid executors."""

    @property
    def job_id(self) -> int | str | None:
        """
        Inherited attribute from BaseExecutor.

        Since this is not really an executor, but a wrapper of executors
        we implemented it as property, so we can have custom setter.
        """
        return self._job_id

    @job_id.setter
    def job_id(self, value: int | str | None) -> None:
        """Expose job ID for SchedulerJob."""
        self._job_id = value
        self.kubernetes_executor.job_id = value
        self.celery_executor.job_id = value

    def start(self) -> None:
        """Start celery and kubernetes executor."""
        self.celery_executor.start()
        self.kubernetes_executor.start()

    @property
    def slots_available(self) -> int:
        """Number of new tasks this executor instance can accept."""
        return self.celery_executor.slots_available

    @property
    def slots_occupied(self):
        """Number of tasks this executor instance is currently managing."""
        return len(self.running) + len(self.queued_tasks)

    def queue_command(
        self,
        task_instance: TaskInstance,
        command: CommandType,
        priority: int = 1,
        queue: str | None = None,
    ) -> None:
        """Queues command via celery or kubernetes executor."""
        executor = self._router(task_instance)
        self.log.debug("Using executor: %s for %s", executor.__class__.__name__, task_instance.key)
        executor.queue_command(task_instance, command, priority, queue)  # type: ignore[union-attr]

    def queue_task_instance(
        self,
        task_instance: TaskInstance,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
        **kwargs,
    ) -> None:
        """Queues task instance via celery or kubernetes executor."""
        from airflow.models.taskinstance import SimpleTaskInstance  # type: ignore[attr-defined]

        executor = self._router(SimpleTaskInstance.from_ti(task_instance))
        self.log.debug(
            "Using executor: %s to queue_task_instance for %s", executor.__class__.__name__, task_instance.key
        )

        # TODO: Remove this once providers depend on Airflow 3.0
        if not hasattr(task_instance, "pickle_id"):
            del kwargs["pickle_id"]

        executor.queue_task_instance(  # type: ignore[union-attr]
            task_instance=task_instance,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            pool=pool,
            cfg_path=cfg_path,
            **kwargs,
        )

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        """Fetch task log from Kubernetes executor."""
        if ti.queue == self.kubernetes_executor.kubernetes_queue:
            return self.kubernetes_executor.get_task_log(ti=ti, try_number=try_number)
        return [], []

    def has_task(self, task_instance: TaskInstance) -> bool:
        """
        Check if a task is either queued or running in either celery or kubernetes executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        return self.celery_executor.has_task(task_instance) or self.kubernetes_executor.has_task(
            task_instance
        )

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs in celery and kubernetes executor."""
        self.celery_executor.heartbeat()
        self.kubernetes_executor.heartbeat()

    def get_event_buffer(
        self, dag_ids: list[str] | None = None
    ) -> dict[TaskInstanceKey, EventBufferValueType]:
        """
        Return and flush the event buffer from celery and kubernetes executor.

        :param dag_ids: dag_ids to return events for, if None returns all
        :return: a dict of events
        """
        cleared_events_from_celery = self.celery_executor.get_event_buffer(dag_ids)
        cleared_events_from_kubernetes = self.kubernetes_executor.get_event_buffer(dag_ids)

        return {**cleared_events_from_celery, **cleared_events_from_kubernetes}

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        """
        Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

        Anything that is not adopted will be cleared by the scheduler (and then become eligible for
        re-scheduling)

        :return: any TaskInstances that were unable to be adopted
        """
        celery_tis = [ti for ti in tis if ti.queue != self.kubernetes_queue]
        kubernetes_tis = [ti for ti in tis if ti.queue == self.kubernetes_queue]
        return [
            *self.celery_executor.try_adopt_task_instances(celery_tis),
            *self.kubernetes_executor.try_adopt_task_instances(kubernetes_tis),
        ]

    @deprecated(
        reason="Replaced by function `revoke_task`. Upgrade airflow core to make this go away.",
        category=AirflowProviderDeprecationWarning,
        action="ignore",  # ignoring since will get warning from the nested executors
    )
    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:
        celery_tis = [ti for ti in tis if ti.queue != self.kubernetes_queue]
        kubernetes_tis = [ti for ti in tis if ti.queue == self.kubernetes_queue]
        return [
            *self.celery_executor.cleanup_stuck_queued_tasks(celery_tis),
            *self.kubernetes_executor.cleanup_stuck_queued_tasks(kubernetes_tis),
        ]

    def revoke_task(self, *, ti: TaskInstance):
        if ti.queue == self.kubernetes_queue:
            try:
                self.kubernetes_executor.revoke_task(ti=ti)
            except NotImplementedError:
                self.log.warning(
                    "Your kubernetes provider version is old. Falling back to deprecated "
                    "function, `cleanup_stuck_queued_tasks`. You must upgrade k8s "
                    "provider to enable 'stuck in queue' retries and stuck in queue "
                    "event logging."
                )
                for ti_repr in self.kubernetes_executor.cleanup_stuck_queued_tasks(tis=[ti]):
                    self.log.info(
                        "task stuck in queued and will be marked failed. task_instance=%s",
                        ti_repr,
                    )
        else:
            self.celery_executor.revoke_task(ti=ti)

    def end(self) -> None:
        """End celery and kubernetes executor."""
        self.celery_executor.end()
        self.kubernetes_executor.end()

    def terminate(self) -> None:
        """Terminate celery and kubernetes executor."""
        self.celery_executor.terminate()
        self.kubernetes_executor.terminate()

    def _router(self, simple_task_instance: SimpleTaskInstance) -> CeleryExecutor | KubernetesExecutor:
        """
        Return either celery_executor or kubernetes_executor.

        :param simple_task_instance: SimpleTaskInstance
        :return: celery_executor or kubernetes_executor
        """
        if simple_task_instance.queue == self.kubernetes_queue:
            return self.kubernetes_executor
        return self.celery_executor

    def debug_dump(self) -> None:
        """Debug dump; called in response to SIGUSR2 by the scheduler."""
        self.log.info("Dumping CeleryExecutor state")
        self.celery_executor.debug_dump()
        self.log.info("Dumping KubernetesExecutor state")
        self.kubernetes_executor.debug_dump()

    def send_callback(self, request: CallbackRequest) -> None:
        """
        Send callback for execution.

        :param request: Callback request to be executed.
        """
        if not self.callback_sink:
            raise ValueError("Callback sink is not ready.")
        self.callback_sink.send(request)

    @staticmethod
    def get_cli_commands() -> list[GroupCommand]:
        from airflow.providers.celery.cli.definition import get_celery_cli_commands
        from airflow.providers.cncf.kubernetes.cli.definition import get_kubernetes_cli_commands

        return get_celery_cli_commands() + get_kubernetes_cli_commands()
