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
#

import os
import signal

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job import BaseJob
from airflow.models import Log, TaskReschedule
from airflow.stats import Stats
from airflow.task.task_runner import get_task_runner
from airflow.ti_deps.dep_context import REQUEUEABLE_DEPS, RUNNING_DEPS, DepContext
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.session import provide_session
from airflow.utils.state import State


class LocalTaskJob(BaseJob):

    __mapper_args__ = {
        'polymorphic_identity': 'LocalTaskJob'
    }

    def __init__(
            self,
            task_instance,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            pickle_id=None,
            pool=None,
            *args, **kwargs):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False

        super().__init__(*args, **kwargs)

    def _execute(self):
        self.task_runner = get_task_runner(self)

        def signal_handler(signum, frame):
            """Setting kill signal handler"""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.on_kill()
            raise AirflowException("LocalTaskJob received SIGTERM signal")
        signal.signal(signal.SIGTERM, signal_handler)

        if not self._check_and_change_state_before_execution():
            self.log.info("Task is not able to be run")
            return

        try:
            self.task_runner.start()

            heartbeat_time_limit = conf.getint('scheduler',
                                               'scheduler_zombie_task_threshold')
            while True:
                # Monitor the task to see if it's done
                return_code = self.task_runner.return_code()
                if return_code is not None:
                    self.log.info("Task exited with return code %s", return_code)
                    return

                self.heartbeat()

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                # This can only really happen if the worker can't read the DB for a long time
                time_since_last_heartbeat = (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.log.error("Heartbeat time limited exceeded!")
                    raise AirflowException("Time since last heartbeat({:.2f}s) "
                                           "exceeded limit ({}s)."
                                           .format(time_since_last_heartbeat,
                                                   heartbeat_time_limit))
        finally:
            self.on_kill()

    @provide_session
    def _check_and_change_state_before_execution(
        self,
        verbose: bool = True,
        test_mode: bool = False,
        session=None
    ) -> bool:
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: bool
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task_instance.task
        self.task_instance.pool = self.pool or task.pool
        self.task_instance.test_mode = test_mode
        self.task_instance.refresh_from_db(session=session, lock_for_update=True)
        self.task_instance.job_id = self.job_id
        self.task_instance.hostname = get_hostname()
        self.operator = task.__class__.__name__

        if not self.ignore_all_deps and not self.ignore_ti_state and \
           self.task_instance.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        # TODO: Logging needs cleanup, not clear what is being printed
        hr = "\n" + ("-" * 80)  # Line break

        if not self.mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=self.ignore_all_deps,
                ignore_ti_state=self.ignore_ti_state,
                ignore_depends_on_past=self.ignore_depends_on_past,
                ignore_task_deps=self.ignore_task_deps)
            if not self.task_instance.are_dependencies_met(
                dep_context=non_requeueable_dep_context,
                session=session,
                verbose=True
            ):
                session.commit()
                return False

            # For reporting purposes, we report based on 1-indexed,
            # not 0-indexed lists (i.e. Attempt 1 instead of
            # Attempt 0 for the first attempt).
            # Set the task start date. In case it was re-scheduled use the initial
            # start date that is recorded in task_reschedule table
            self.task_instance.start_date = timezone.utcnow()
            task_reschedules = TaskReschedule.find_for_task_instance(self.task_instance, session)
            if task_reschedules:
                self.task_instance.start_date = task_reschedules[0].start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=self.ignore_all_deps,
                ignore_depends_on_past=self.ignore_depends_on_past,
                ignore_task_deps=self.ignore_task_deps,
                ignore_ti_state=self.ignore_ti_state)
            if not self.task_instance.are_dependencies_met(
                dep_context=dep_context,
                session=session,
                verbose=True
            ):
                self.task_instance.state = State.NONE
                self.log.warning(hr)
                self.log.warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to NONE.", self.task_instance.try_number, self.task_instance.max_tries + 1
                )
                self.log.warning(hr)
                self.task_instance.queued_dttm = timezone.utcnow()
                session.merge(self.task_instance)
                session.commit()
                return False

        # print status message
        self.log.info(hr)
        self.log.info("Starting attempt %s of %s",
                      self.task_instance.try_number, self.task_instance.max_tries + 1
                      )
        self.log.info(hr)
        self.task_instance._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self.task_instance))
        self.task_instance.state = State.RUNNING
        self.task_instance.pid = os.getpid()
        self.task_instance.end_date = None
        if not test_mode:
            session.merge(self.task_instance)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()  # type: ignore
        if verbose:
            if self.mark_success:
                self.log.info(
                    "Marking success for %s on %s",
                    self.task_instance.task, self.task_instance.execution_date
                )
            else:
                self.log.info(
                    "Executing %s on %s", self.task_instance.task, self.task_instance.execution_date
                )
        return True

    def on_kill(self):
        self.task_runner.terminate()
        self.task_runner.on_finish()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally"""

        if self.terminating:
            # ensure termination if processes are created later
            self.task_runner.terminate()
            return

        self.task_instance.refresh_from_db()
        ti = self.task_instance

        if ti.state == State.RUNNING:
            fqdn = get_hostname()
            same_hostname = fqdn == ti.hostname
            if not same_hostname:
                self.log.warning("The recorded hostname %s "
                                 "does not match this instance's hostname "
                                 "%s", ti.hostname, fqdn)
                raise AirflowException("Hostname of job runner does not match")

            current_pid = os.getpid()
            same_process = ti.pid == current_pid
            if not same_process:
                self.log.warning("Recorded pid %s does not match "
                                 "the current pid %s", ti.pid, current_pid)
                raise AirflowException("PID of job runner does not match")
        elif (
                self.task_runner.return_code() is None and
                hasattr(self.task_runner, 'process')
        ):
            self.log.warning(
                "State of this instance has been externally set to %s. "
                "Taking the poison pill.",
                ti.state
            )
            if ti.state == State.FAILED and ti.task.on_failure_callback:
                context = ti.get_template_context()
                ti.task.on_failure_callback(context)
            self.task_runner.terminate()
            self.terminating = True
