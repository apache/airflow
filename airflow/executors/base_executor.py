# -*- coding: utf-8 -*-
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

from builtins import range

from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State


PARALLELISM = configuration.conf.getint('core', 'PARALLELISM')


class BaseExecutor(LoggingMixin):

    def __init__(self, parallelism=PARALLELISM):
        """
        Class to derive in order to interface with executor-type systems
        like Celery, Mesos, Yarn and the likes.

        :param parallelism: how many jobs should run at one time. Set to
            ``0`` for infinity
        :type parallelism: int
        """
        self.parallelism = parallelism
        self.queued_tasks = {}
        self.running = {}
        self.event_buffer = {}

    def start(self):  # pragma: no cover
        """
        Executors may need to get things started. For example LocalExecutor
        starts N workers.
        """
        pass

    def queue_command(self, task_instance, command, priority=1, queue=None):
        key = task_instance.key
        if key not in self.queued_tasks and key not in self.running:
            self.log.info("Adding to queue: %s", command)
            self.queued_tasks[key] = (command, priority, queue, task_instance)
        else:
            self.log.info("could not queue task {}".format(key))

    def queue_task_instance(
            self,
            task_instance,
            mark_success=False,
            pickle_id=None,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            pool=None,
            cfg_path=None):
        pool = pool or task_instance.pool

        # TODO (edgarRd): AIRFLOW-1985:
        # cfg_path is needed to propagate the config values if using impersonation
        # (run_as_user), given that there are different code paths running tasks.
        # For a long term solution we need to address AIRFLOW-1986
        command = task_instance.command_as_list(
            local=True,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            pool=pool,
            pickle_id=pickle_id,
            cfg_path=cfg_path)
        self.queue_command(
            task_instance,
            command,
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue)

    def has_task(self, task_instance):
        """
        Checks if a task is either queued or running in this executor

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        if task_instance.key in self.queued_tasks or task_instance.key in self.running:
            return True

    def sync(self):
        """
        Sync will get called periodically by the heartbeat method.
        Executors should override this to perform gather statuses.
        """
        pass

    def heartbeat(self):
        # Triggering new jobs
        if not self.parallelism:
            open_slots = len(self.queued_tasks)
        else:
            open_slots = self.parallelism - len(self.running)

        self.log.debug("%s running task instances", len(self.running))
        self.log.debug("%s in queue", len(self.queued_tasks))
        self.log.debug("%s open slots", open_slots)

        sorted_queue = sorted(
            [(k, v) for k, v in self.queued_tasks.items()],
            key=lambda x: x[1][1],
            reverse=True)
        for i in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, ti) = sorted_queue.pop(0)
            # TODO(jlowin) without a way to know what Job ran which tasks,
            # there is a danger that another Job started running a task
            # that was also queued to this executor. This is the last chance
            # to check if that happened. The most probable way is that a
            # Scheduler tried to run a task that was originally queued by a
            # Backfill. This fix reduces the probability of a collision but
            # does NOT eliminate it.
            self.queued_tasks.pop(key)
            ti.refresh_from_db()
            if ti.state != State.RUNNING:
                self.running[key] = command
                self.execute_async(key=key,
                                   command=command,
                                   queue=queue,
                                   executor_config=ti.executor_config)
            else:
                self.log.info(
                    'Task is already running, not sending to '
                    'executor: {}'.format(key))

        # Calling child class sync method
        self.log.debug("Calling the %s sync method", self.__class__)
        self.sync()

    def change_state(self, key, state):
        self.log.debug("Changing state: {}".format(key))
        self.running.pop(key)
        self.event_buffer[key] = state

    def fail(self, key):
        self.change_state(key, State.FAILED)

    def success(self, key):
        self.change_state(key, State.SUCCESS)

    def get_event_buffer(self, dag_ids=None):
        """
        Returns and flush the event buffer. In case dag_ids is specified
        it will only return and flush events for the given dag_ids. Otherwise
        it returns and flushes all

        :param dag_ids: to dag_ids to return events for, if None returns all
        :return: a dict of events
        """
        cleared_events = dict()
        if dag_ids is None:
            cleared_events = self.event_buffer
            self.event_buffer = dict()
        else:
            for key in list(self.event_buffer.keys()):
                dag_id, _, _, _ = key
                if dag_id in dag_ids:
                    cleared_events[key] = self.event_buffer.pop(key)

        return cleared_events

    def execute_async(self,
                      key,
                      command,
                      queue=None,
                      executor_config=None):  # pragma: no cover
        """
        This method will execute the command asynchronously.
        """
        raise NotImplementedError()

    def end(self):  # pragma: no cover
        """
        This method is called when the caller is done submitting job and
        wants to wait synchronously for the job submitted previously to be
        all done.
        """
        raise NotImplementedError()

    def terminate(self):
        """
        This method is called when the daemon receives a SIGTERM
        """
        raise NotImplementedError()
