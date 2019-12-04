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
"""Multiple  executor."""
from itertools import chain
from typing import Any, Dict, Iterable, Optional, Set

from airflow.executors.base_executor import BaseExecutor, BaseExecutorProtocol, CommandType
from airflow.models import TaskInstance
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKeyType


class MultipleExecutors(BaseExecutorProtocol):
    """
    This executor can run multiple executors under the hood.
    """
    def __init__(self, main_executor: BaseExecutor, additional_executors_dict: Dict[str, BaseExecutor]):
        super().__init__()
        self.main_executor = main_executor
        self.additional_executors_dict = additional_executors_dict
        self.executor_set: Set[BaseExecutor] = set(additional_executors_dict.values())
        self.executor_set.add(main_executor)
        self.log.info("Multiple executor")
        self.log.info("Main executor %s", str(main_executor))
        self.log.info("Additional executor dict %s", str(self.additional_executors_dict))
        self.log.info("Executor set %s", str(self.executor_set))

    def _get_executor_for_queue(self, queue: Optional[str]) -> BaseExecutor:
        executor = None
        if queue:
            executor = self.additional_executors_dict.get(queue)
        return self.main_executor if not executor else executor

    def start(self):
        for executor in self.executor_set:
            executor.start()

    def has_task(self, task_instance: TaskInstance) -> bool:
        executor = self._get_executor_for_queue(task_instance.queue)
        return executor.has_task(task_instance=task_instance)

    def sync(self) -> None:
        for executor in self.executor_set:
            executor.sync()

    def queue_command(self, simple_task_instance: SimpleTaskInstance, command: CommandType, priority: int = 1,
                      queue: Optional[str] = None) -> None:
        executor = self._get_executor_for_queue(queue)
        executor.queue_command(simple_task_instance=simple_task_instance,
                               command=command,
                               priority=priority,
                               queue=queue)

    def queue_task_instance(self, task_instance: TaskInstance, mark_success: bool = False,
                            pickle_id: Optional[str] = None, ignore_all_deps: bool = False,
                            ignore_depends_on_past: bool = False, ignore_task_deps: bool = False,
                            ignore_ti_state: bool = False, pool: Optional[str] = None,
                            cfg_path: Optional[str] = None) -> None:
        executor = self._get_executor_for_queue(task_instance.queue)
        executor.queue_task_instance(task_instance=task_instance, mark_success=mark_success,
                                     pickle_id=pickle_id, ignore_all_deps=ignore_all_deps,
                                     ignore_depends_on_past=ignore_depends_on_past,
                                     ignore_task_deps=ignore_task_deps, ignore_ti_state=ignore_ti_state,
                                     pool=pool, cfg_path=cfg_path)

    def heartbeat(self) -> None:
        for executor in self.executor_set:
            executor.heartbeat()

    def execute_async(self, key: TaskInstanceKeyType, command: CommandType, queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:
        executor = self._get_executor_for_queue(queue)
        executor.execute_async(key=key, command=command, queue=queue, executor_config=executor_config)

    def end(self) -> None:
        for executor in self.executor_set:
            executor.end()

    def terminate(self):
        for executor in self.executor_set:
            executor.terminate()

    def is_task_queued(self, task_instance_key: TaskInstanceKeyType) -> bool:
        for executor in self.executor_set:
            if executor.is_task_queued(task_instance_key=task_instance_key):
                return True
        return False

    def is_task_running(self, task_instance_key: TaskInstanceKeyType) -> bool:
        for executor in self.executor_set:
            if executor.is_task_running(task_instance_key=task_instance_key):
                return True
        return False

    @property
    def queued_tasks_keys(self) -> Iterable[TaskInstanceKeyType]:
        return chain(*[executor.queued_tasks_keys for executor in self.executor_set])

    def get_event_buffer(self, dag_ids=None) -> Dict[TaskInstanceKeyType, Optional[str]]:
        cleared_events: Dict[TaskInstanceKeyType, Optional[str]] = {}
        for executor in self.executor_set:
            cleared_events.update(executor.get_event_buffer(dag_ids=dag_ids))
        return cleared_events
