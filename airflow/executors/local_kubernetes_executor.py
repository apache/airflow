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
from typing import Dict, List, Optional, Set, Union

from airflow.configuration import conf
from airflow.executors.base_executor import CommandType, EventBufferValueType, QueuedTaskInstanceType
from airflow.executors.kubernetes_executor import KubernetesExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstance, TaskInstanceKey
from airflow.utils.log.logging_mixin import LoggingMixin


class LocalKubernetesExecutor(LoggingMixin):
    """
    LocalKubernetesExecutor consists of LocalExecutor and KubernetesExecutor.
    It chooses the executor to use based on the queue defined on the task.
    When the task's queue is the value of ``kubernetes_queue`` in section ``[local_kubernetes_executor]``
    of the configuration (default value: `kubernetes`), KubernetesExecutor is selected to run the task,
    otherwise, LocalExecutor is used.
    """

    supports_ad_hoc_ti_run: bool = True

    KUBERNETES_QUEUE = conf.get('local_kubernetes_executor', 'kubernetes_queue')

    def __init__(self, local_executor: LocalExecutor, kubernetes_executor: KubernetesExecutor):
        super().__init__()
        self._job_id: Optional[str] = None
        self.local_executor = local_executor
        self.kubernetes_executor = kubernetes_executor

    @property
    def queued_tasks(self) -> Dict[TaskInstanceKey, QueuedTaskInstanceType]:
        """Return queued tasks from local and kubernetes executor"""
        queued_tasks = self.local_executor.queued_tasks.copy()
        queued_tasks.update(self.kubernetes_executor.queued_tasks)

        return queued_tasks

    @property
    def running(self) -> Set[TaskInstanceKey]:
        """Return running tasks from local and kubernetes executor"""
        return self.local_executor.running.union(self.kubernetes_executor.running)

    @property
    def job_id(self) -> Optional[str]:
        """
        This is a class attribute in BaseExecutor but since this is not really an executor, but a wrapper
        of executors we implement as property so we can have custom setter.
        """
        return self._job_id

    @job_id.setter
    def job_id(self, value: Optional[str]) -> None:
        """job_id is manipulated by SchedulerJob.  We must propagate the job_id to wrapped executors."""
        self._job_id = value
        self.kubernetes_executor.job_id = value
        self.local_executor.job_id = value

    def start(self) -> None:
        self.log.info("Starting local and Kubernetes Executor")
        """Start local and kubernetes executor"""
        self.local_executor.start()
        self.kubernetes_executor.start()

    @property
    def slots_available(self) -> int:
        """Number of new tasks this executor instance can accept"""
        return self.local_executor.slots_available

    def queue_command(
        self,
        task_instance: TaskInstance,
        command: CommandType,
        priority: int = 1,
        queue: Optional[str] = None,
    ) -> None:
        """Queues command via local or kubernetes executor"""
        executor = self._router(task_instance)
        self.log.debug("Using executor: %s for %s", executor.__class__.__name__, task_instance.key)
        executor.queue_command(task_instance, command, priority, queue)

    def queue_task_instance(
        self,
        task_instance: TaskInstance,
        mark_success: bool = False,
        pickle_id: Optional[str] = None,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        pool: Optional[str] = None,
        cfg_path: Optional[str] = None,
    ) -> None:
        """Queues task instance via local or kubernetes executor"""
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
        Checks if a task is either queued or running in either local or kubernetes executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        return self.local_executor.has_task(task_instance) or self.kubernetes_executor.has_task(task_instance)

    def heartbeat(self) -> None:
        """Heartbeat sent to trigger new jobs in local and kubernetes executor"""
        self.local_executor.heartbeat()
        self.kubernetes_executor.heartbeat()

    def get_event_buffer(
        self, dag_ids: Optional[List[str]] = None
    ) -> Dict[TaskInstanceKey, EventBufferValueType]:
        """
        Returns and flush the event buffer from local and kubernetes executor

        :param dag_ids: dag_ids to return events for, if None returns all
        :return: a dict of events
        """
        cleared_events_from_local = self.local_executor.get_event_buffer(dag_ids)
        cleared_events_from_kubernetes = self.kubernetes_executor.get_event_buffer(dag_ids)

        return {**cleared_events_from_local, **cleared_events_from_kubernetes}

    def try_adopt_task_instances(self, tis: List[TaskInstance]) -> List[TaskInstance]:
        """
        Try to adopt running task instances that have been abandoned by a SchedulerJob dying.

        Anything that is not adopted will be cleared by the scheduler (and then become eligible for
        re-scheduling)

        :return: any TaskInstances that were unable to be adopted
        :rtype: list[airflow.models.TaskInstance]
        """
        local_tis = []
        kubernetes_tis = []
        abandoned_tis = []
        for ti in tis:
            if ti.queue == self.KUBERNETES_QUEUE:
                kubernetes_tis.append(ti)
            else:
                local_tis.append(ti)
        abandoned_tis.extend(self.local_executor.try_adopt_task_instances(local_tis))
        abandoned_tis.extend(self.kubernetes_executor.try_adopt_task_instances(kubernetes_tis))
        return abandoned_tis

    def end(self) -> None:
        """End local and kubernetes executor"""
        self.local_executor.end()
        self.kubernetes_executor.end()

    def terminate(self) -> None:
        """Terminate local and kubernetes executor"""
        self.local_executor.terminate()
        self.kubernetes_executor.terminate()

    def _router(self, simple_task_instance: SimpleTaskInstance) -> Union[LocalExecutor, KubernetesExecutor]:
        """
        Return either local_executor or kubernetes_executor

        :param simple_task_instance: SimpleTaskInstance
        :return: local_executor or kubernetes_executor
        :rtype: Union[LocalExecutor, KubernetesExecutor]
        """
        if simple_task_instance.queue == self.KUBERNETES_QUEUE:
            return self.kubernetes_executor
        return self.local_executor

    def debug_dump(self) -> None:
        """Called in response to SIGUSR2 by the scheduler"""
        self.log.info("Dumping LocalExecutor state")
        self.local_executor.debug_dump()
        self.log.info("Dumping KubernetesExecutor state")
        self.kubernetes_executor.debug_dump()
