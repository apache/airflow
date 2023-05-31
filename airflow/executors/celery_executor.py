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
"""CeleryExecutor.

.. seealso::
    For more information on how the CeleryExecutor works, take a look at the guide:
    :ref:`executor:CeleryExecutor`
"""
from __future__ import annotations

import logging
import math
import operator
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple

from celery import states as celery_states

from airflow.configuration import conf
from airflow.exceptions import AirflowTaskTimeout
from airflow.executors.base_executor import BaseExecutor
from airflow.stats import Stats
from airflow.utils.state import State

log = logging.getLogger(__name__)


CELERY_SEND_ERR_MSG_HEADER = "Error sending Celery task"


if TYPE_CHECKING:
    from celery import Task

    from airflow.executors.base_executor import CommandType, TaskTuple
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

    # Task instance that is sent over Celery queues
    # TaskInstanceKey, Command, queue_name, CallableTask
    TaskInstanceInCelery = Tuple[TaskInstanceKey, CommandType, Optional[str], Task]


# PEP562
def __getattr__(name):
    # This allows us to make the Celery app accessible through the
    # celery_executor module without the time cost of its import and
    # construction
    if name == "app":
        from airflow.executors.celery_executor_utils import app

        return app
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


"""
To start the celery worker, run the command:
airflow celery worker
"""


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow.

    It allows distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """

    supports_ad_hoc_ti_run: bool = True
    supports_sentry: bool = True

    def __init__(self):
        super().__init__()

        # Celery doesn't support bulk sending the tasks (which can become a bottleneck on bigger clusters)
        # so we use a multiprocessing pool to speed this up.
        # How many worker processes are created for checking celery task state.
        self._sync_parallelism = conf.getint("celery", "SYNC_PARALLELISM")
        if self._sync_parallelism == 0:
            self._sync_parallelism = max(1, cpu_count() - 1)
        from airflow.executors.celery_executor_utils import BulkStateFetcher

        self.bulk_state_fetcher = BulkStateFetcher(self._sync_parallelism)
        self.tasks = {}
        self.task_publish_retries: Counter[TaskInstanceKey] = Counter()
        self.task_publish_max_retries = conf.getint("celery", "task_publish_max_retries", fallback=3)

    def start(self) -> None:
        self.log.debug("Starting Celery Executor using %s processes for syncing", self._sync_parallelism)

    def _num_tasks_per_send_process(self, to_send_count: int) -> int:
        """
        How many Celery tasks should each worker process send.

        :return: Number of tasks that should be sent per process
        """
        return max(1, int(math.ceil(1.0 * to_send_count / self._sync_parallelism)))

    def _process_tasks(self, task_tuples: list[TaskTuple]) -> None:
        from airflow.executors.celery_executor_utils import execute_command

        task_tuples_to_send = [task_tuple[:3] + (execute_command,) for task_tuple in task_tuples]
        first_task = next(t[3] for t in task_tuples_to_send)

        # Celery state queries will stuck if we do not use one same backend
        # for all tasks.
        cached_celery_backend = first_task.backend

        key_and_async_results = self._send_tasks_to_celery(task_tuples_to_send)
        self.log.debug("Sent all tasks.")
        from airflow.executors.celery_executor_utils import ExceptionWithTraceback

        for key, _, result in key_and_async_results:
            if isinstance(result, ExceptionWithTraceback) and isinstance(
                result.exception, AirflowTaskTimeout
            ):
                retries = self.task_publish_retries[key]
                if retries < self.task_publish_max_retries:
                    Stats.incr("celery.task_timeout_error")
                    self.log.info(
                        "[Try %s of %s] Task Timeout Error for Task: (%s).",
                        self.task_publish_retries[key] + 1,
                        self.task_publish_max_retries,
                        key,
                    )
                    self.task_publish_retries[key] = retries + 1
                    continue
            self.queued_tasks.pop(key)
            self.task_publish_retries.pop(key, None)
            if isinstance(result, ExceptionWithTraceback):
                self.log.error(CELERY_SEND_ERR_MSG_HEADER + ": %s\n%s\n", result.exception, result.traceback)
                self.event_buffer[key] = (State.FAILED, None)
            elif result is not None:
                result.backend = cached_celery_backend
                self.running.add(key)
                self.tasks[key] = result

                # Store the Celery task_id in the event buffer. This will get "overwritten" if the task
                # has another event, but that is fine, because the only other events are success/failed at
                # which point we don't need the ID anymore anyway
                self.event_buffer[key] = (State.QUEUED, result.task_id)

                # If the task runs _really quickly_ we may already have a result!
                self.update_task_state(key, result.state, getattr(result, "info", None))

    def _send_tasks_to_celery(self, task_tuples_to_send: list[TaskInstanceInCelery]):
        from airflow.executors.celery_executor_utils import send_task_to_executor

        if len(task_tuples_to_send) == 1 or self._sync_parallelism == 1:
            # One tuple, or max one process -> send it in the main thread.
            return list(map(send_task_to_executor, task_tuples_to_send))

        # Use chunks instead of a work queue to reduce context switching
        # since tasks are roughly uniform in size
        chunksize = self._num_tasks_per_send_process(len(task_tuples_to_send))
        num_processes = min(len(task_tuples_to_send), self._sync_parallelism)

        with ProcessPoolExecutor(max_workers=num_processes) as send_pool:
            key_and_async_results = list(
                send_pool.map(send_task_to_executor, task_tuples_to_send, chunksize=chunksize)
            )
        return key_and_async_results

    def sync(self) -> None:
        if not self.tasks:
            self.log.debug("No task to query celery, skipping sync")
            return
        self.update_all_task_states()

    def debug_dump(self) -> None:
        """Called in response to SIGUSR2 by the scheduler."""
        super().debug_dump()
        self.log.info(
            "executor.tasks (%d)\n\t%s", len(self.tasks), "\n\t".join(map(repr, self.tasks.items()))
        )

    def update_all_task_states(self) -> None:
        """Updates states of the tasks."""
        self.log.debug("Inquiring about %s celery task(s)", len(self.tasks))
        state_and_info_by_celery_task_id = self.bulk_state_fetcher.get_many(self.tasks.values())

        self.log.debug("Inquiries completed.")
        for key, async_result in list(self.tasks.items()):
            state, info = state_and_info_by_celery_task_id.get(async_result.task_id)
            if state:
                self.update_task_state(key, state, info)

    def change_state(self, key: TaskInstanceKey, state: str, info=None) -> None:
        super().change_state(key, state, info)
        self.tasks.pop(key, None)

    def update_task_state(self, key: TaskInstanceKey, state: str, info: Any) -> None:
        """Updates state of a single task."""
        try:
            if state == celery_states.SUCCESS:
                self.success(key, info)
            elif state in (celery_states.FAILURE, celery_states.REVOKED):
                self.fail(key, info)
            elif state == celery_states.STARTED:
                pass
            elif state == celery_states.PENDING:
                pass
            else:
                self.log.info("Unexpected state for %s: %s", key, state)
        except Exception:
            self.log.exception("Error syncing the Celery executor, ignoring it.")

    def end(self, synchronous: bool = False) -> None:
        if synchronous:
            while any(task.state not in celery_states.READY_STATES for task in self.tasks.values()):
                time.sleep(5)
        self.sync()

    def terminate(self):
        pass

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        # See which of the TIs are still alive (or have finished even!)
        #
        # Since Celery doesn't store "SENT" state for queued commands (if we create an AsyncResult with a made
        # up id it just returns PENDING state for it), we have to store Celery's task_id against the TI row to
        # look at in future.
        #
        # This process is not perfect -- we could have sent the task to celery, and crashed before we were
        # able to record the AsyncResult.task_id in the TaskInstance table, in which case we won't adopt the
        # task (it'll either run and update the TI state, or the scheduler will clear and re-queue it. Either
        # way it won't get executed more than once)
        #
        # (If we swapped it around, and generated a task_id for Celery, stored that in TI and enqueued that
        # there is also still a race condition where we could generate and store the task_id, but die before
        # we managed to enqueue the command. Since neither way is perfect we always have to deal with this
        # process not being perfect.)
        from celery.result import AsyncResult

        celery_tasks = {}
        not_adopted_tis = []

        for ti in tis:
            if ti.external_executor_id is not None:
                celery_tasks[ti.external_executor_id] = (AsyncResult(ti.external_executor_id), ti)
            else:
                not_adopted_tis.append(ti)

        if not celery_tasks:
            # Nothing to adopt
            return tis

        states_by_celery_task_id = self.bulk_state_fetcher.get_many(
            list(map(operator.itemgetter(0), celery_tasks.values()))
        )

        adopted = []
        cached_celery_backend = next(iter(celery_tasks.values()))[0].backend

        for celery_task_id, (state, info) in states_by_celery_task_id.items():
            result, ti = celery_tasks[celery_task_id]
            result.backend = cached_celery_backend

            # Set the correct elements of the state dicts, then update this
            # like we just queried it.
            self.tasks[ti.key] = result
            self.running.add(ti.key)
            self.update_task_state(ti.key, state, info)
            adopted.append(f"{ti} in state {state}")

        if adopted:
            task_instance_str = "\n\t".join(adopted)
            self.log.info(
                "Adopted the following %d tasks from a dead executor\n\t%s", len(adopted), task_instance_str
            )

        return not_adopted_tis

    def cleanup_stuck_queued_tasks(self, tis: list[TaskInstance]) -> list[str]:
        """
        Handle remnants of tasks that were failed because they were stuck in queued.
        Tasks can get stuck in queued. If such a task is detected, it will be marked
        as `UP_FOR_RETRY` if the task instance has remaining retries or marked as `FAILED`
        if it doesn't.

        :param tis: List of Task Instances to clean up
        :return: List of readable task instances for a warning message
        """
        readable_tis = []
        from airflow.executors.celery_executor_utils import app

        for ti in tis:
            readable_tis.append(repr(ti))
            task_instance_key = ti.key
            self.fail(task_instance_key, None)
            celery_async_result = self.tasks.pop(task_instance_key, None)
            if celery_async_result:
                try:
                    app.control.revoke(celery_async_result.task_id)
                except Exception as ex:
                    self.log.error("Error revoking task instance %s from celery: %s", task_instance_key, ex)
        return readable_tis
