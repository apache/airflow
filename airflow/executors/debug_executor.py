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
"""
This module contains DebugExecutor that is a single
process executor meaning it does not use multiprocessing.
"""

import threading
from typing import List, Optional, Tuple

from airflow import conf
from airflow.executors.base_executor import BaseExecutor
from airflow.models.queue_task_run import QueueTaskRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceKeyType
from airflow.utils.state import State


class DebugExecutor(BaseExecutor):
    """
    This executor is meant for debugging purposes. It can be used with SQLite.

    It executes one task instance at time. Additionally to support working
    with sensors, all sensors ``mode`` will be automatically set to "reschedule".
    """

    _terminated = threading.Event()

    def __init__(self):
        super().__init__()
        self.tasks_to_run: List[Tuple[TaskInstance, QueueTaskRun]] = []
        self.fail_fast = conf.getboolean("debug", "fail_fast")

    def execute_async(self, *args, **kwargs) -> None:
        """
        The method is replaced by custom trigger_task implementation.
        """

    def sync(self) -> None:
        task_succeeded = True
        while self.tasks_to_run:
            ti, queue_task_run = self.tasks_to_run.pop(0)
            if self.fail_fast and not task_succeeded:
                self.log.info("Setting %s to %s", ti.key, State.UPSTREAM_FAILED)
                ti.set_state(State.UPSTREAM_FAILED)
                self.change_state(ti.key, State.UPSTREAM_FAILED)
                continue

            if self._terminated.is_set():
                self.log.info(
                    "Executor is terminated! Stopping %s to %s", ti.key, State.FAILED
                )
                ti.set_state(State.FAILED)
                self.change_state(ti.key, State.FAILED)
                continue

            task_succeeded = self._run_task(ti, queue_task_run)

    def _run_task(self, ti: TaskInstance, queue_task_run: QueueTaskRun) -> bool:
        self.log.debug("Executing task: %s", ti)
        key = ti.key
        try:
            ti._run_raw_task(  # pylint: disable=protected-access
                job_id=ti.job_id, mark_success=queue_task_run.mark_success, pool=queue_task_run.pool
            )
            self.change_state(key, State.SUCCESS)
            return True
        except Exception as e:  # pylint: disable=broad-except
            self.change_state(key, State.FAILED)
            self.log.exception("Failed to execute task: %s.", str(e))
            return False

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
        """
        Queues task instance.
        """
        self._queue_command(
            task_instance,
            task_instance.get_queue_task_run(
                pickle_id=pickle_id,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state,
                pool=pool,
                cfg_path=cfg_path
            ),
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue,
        )

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Triggers tasks. Instead of calling exec_async we just
        add task instance to tasks_to_run queue.

        :param open_slots: Number of open slots
        """
        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],  # pylint: disable=unnecessary-comprehension
            key=lambda x: x[1][1],
            reverse=True,
        )
        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, _, ti) = sorted_queue.pop(0)
            self.queued_tasks.pop(key)
            self.running.add(key)
            if not isinstance(ti, TaskInstance):
                raise ValueError(f"Expected TaskInstance, but found {type(ti)} type")
            self.tasks_to_run.append((ti, command))

    def end(self) -> None:
        """
        When the method is called we just set states of queued tasks
        to UPSTREAM_FAILED marking them as not executed.
        """
        for ti, _ in self.tasks_to_run:
            self.log.info("Setting %s to %s", ti.key, State.UPSTREAM_FAILED)
            ti.set_state(State.UPSTREAM_FAILED)
            self.change_state(ti.key, State.UPSTREAM_FAILED)

    def terminate(self) -> None:
        self._terminated.set()

    def change_state(self, key: TaskInstanceKeyType, state: str) -> None:
        self.log.debug("Popping %s from executor task queue.", key)
        self.running.remove(key)
        self.event_buffer[key] = state
