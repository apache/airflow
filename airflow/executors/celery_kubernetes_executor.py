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

from typing import Sequence

from airflow.callbacks.base_callback_sink import BaseCallbackSink
from airflow.callbacks.callback_requests import CallbackRequest
from airflow.configuration import conf
from airflow.executors.base_executor import CommandType, EventBufferValueType, QueuedTaskInstanceType
from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.kubernetes_executor import KubernetesExecutor
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance, TaskInstanceKey
from airflow.utils.log.logging_mixin import LoggingMixin


class CeleryKubernetesExecutor(LoggingMixin):
    """
    CeleryKubernetesExecutor consists of CeleryExecutor and KubernetesExecutor.
    It chooses an executor to use based on the queue defined on the task.
    When the queue is the value of ``kubernetes_queue`` in section ``[celery_kubernetes_executor]``
    of the configuration (default value: `kubernetes`), KubernetesExecutor is selected to run the task,
    otherwise, CeleryExecutor is used.
    """

    supports_ad_hoc_ti_run: bool = True
    callback_sink: BaseCallbackSink | None = None

    KUBERNETES_QUEUE = conf.get("celery_kubernetes_executor", "kubernetes_queue")

    is_local: bool = False

    def __init__(self, celery_executor: CeleryExecutor, kubernetes_executor: KubernetesExecutor):
        super().__init__()
        self._job_id: int | None = None
        self.celery_executor = celery_executor
        self.kubernetes_executor = kubernetes_executor
        self.kubernetes_executor.kubernetes_queue = self.KUBERNETES_QUEUE

    @property
    def queued_tasks(self) -> dict[TaskInstanceKey, QueuedTaskInstanceType]:
        """Return queued tasks from celery and kubernetes executor."""
        queued_tasks = self.celery_executor.queued_tasks.copy()
        queued_tasks.update(self.kubernetes_executor.queued_tasks)

        return queued_tasks

    @property
    def running(self) -> set[TaskInstanceKey]:
        """Return running tasks from celery and kubernetes executor."""
        return self.celery_executor.running.union(self.kubernetes_executor.running)

    @property
    def job_id(self) -> int | None:
        """
        Inherited attribute from BaseExecutor.

        Since this is not really an executor, but a wrapper of executors
        we implemented it as property, so we can have custom setter.
        """
        return self._job_id

    @job_id.setter
    def job_id(self, value: int | None) -> None:
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
        executor.queue_command(task_instance, command, priority, queue)

    def queue_task_instance(
        self,
        task_instance: TaskInstance,
        mark_success: bool = False,
        pickle_id: str | None = None,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
    ) -> None:
        """Queues task instance via celery or kubernetes executor."""
        executor = self._router(SimpleTaskInstance.from_ti(task_instance))
        self.log.debug(
            "Using executor: %s to queue_task_instance for %s", executor.__class__.__name__, task_instance.key
        )
        executor.queue_task_instance(
            task_instance,
            mark_success,
            pickle_id,
            ignore_all_deps,
            ignore_depends_on_past,
            ignore_task_deps,
            ignore_ti_state,
            pool,
            cfg_path,
        )

    def has_task(self, task_instance: TaskInstance) -> bool:
        """
        Checks if a task is either queued or running in either celery or kubernetes executor.

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
        celery_tis = [ti for ti in tis if ti.queue != self.KUBERNETES_QUEUE]
        kubernetes_tis = [ti for ti in tis if ti.queue == self.KUBERNETES_QUEUE]
        return [
            *self.celery_executor.try_adopt_task_instances(celery_tis),
            *self.kubernetes_executor.try_adopt_task_instances(kubernetes_tis),
        ]

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
        if simple_task_instance.queue == self.KUBERNETES_QUEUE:
            return self.kubernetes_executor
        return self.celery_executor

    def debug_dump(self) -> None:
        """Called in response to SIGUSR2 by the scheduler."""
        self.log.info("Dumping CeleryExecutor state")
        self.celery_executor.debug_dump()
        self.log.info("Dumping KubernetesExecutor state")
        self.kubernetes_executor.debug_dump()

    def send_callback(self, request: CallbackRequest) -> None:
        """Sends callback for execution.

        :param request: Callback request to be executed.
        """
        if not self.callback_sink:
            raise ValueError("Callback sink is not ready.")
        self.callback_sink.send(request)
