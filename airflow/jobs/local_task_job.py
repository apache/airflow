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
import signal
from typing import Optional

import psutil
from sqlalchemy.exc import OperationalError

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job import BaseJob
from airflow.listeners.events import register_task_instance_state_events
from airflow.listeners.listener import get_listener_manager
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.sentry import Sentry
from airflow.stats import Stats
from airflow.task.task_runner import get_task_runner
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import State


class LocalTaskJob(BaseJob):
    """LocalTaskJob runs a single task instance."""

    __mapper_args__ = {'polymorphic_identity': 'LocalTaskJob'}

    def __init__(
        self,
        task_instance: TaskInstance,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        pickle_id: Optional[str] = None,
        pool: Optional[str] = None,
        external_executor_id: Optional[str] = None,
        *args,
        **kwargs,
    ):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        self.external_executor_id = external_executor_id

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False

        super().__init__(*args, **kwargs)

    def _execute(self):
        self._enable_task_listeners()
        self.task_runner = get_task_runner(self)

        def signal_handler(signum, frame):
            """Setting kill signal handler"""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.task_runner.terminate()
            self.handle_task_exit(128 + signum)
            return

        signal.signal(signal.SIGTERM, signal_handler)

        if not self.task_instance.check_and_change_state_before_execution(
            mark_success=self.mark_success,
            ignore_all_deps=self.ignore_all_deps,
            ignore_depends_on_past=self.ignore_depends_on_past,
            ignore_task_deps=self.ignore_task_deps,
            ignore_ti_state=self.ignore_ti_state,
            job_id=self.id,
            pool=self.pool,
            external_executor_id=self.external_executor_id,
        ):
            self.log.info("Task is not able to be run")
            return

        try:
            self.task_runner.start()

            heartbeat_time_limit = conf.getint('scheduler', 'scheduler_zombie_task_threshold')

            # task callback invocation happens either here or in
            # self.heartbeat() instead of taskinstance._run_raw_task to
            # avoid race conditions
            #
            # When self.terminating is set to True by heartbeat_callback, this
            # loop should not be restarted. Otherwise self.handle_task_exit
            # will be invoked and we will end up with duplicated callbacks
            while not self.terminating:
                # Monitor the task to see if it's done. Wait in a syscall
                # (`os.wait`) for as long as possible so we notice the
                # subprocess finishing as quick as we can
                max_wait_time = max(
                    0,  # Make sure this value is never negative,
                    min(
                        (
                            heartbeat_time_limit
                            - (timezone.utcnow() - self.latest_heartbeat).total_seconds() * 0.75
                        ),
                        self.heartrate,
                    ),
                )

                return_code = self.task_runner.return_code(timeout=max_wait_time)
                if return_code is not None:
                    self.handle_task_exit(return_code)
                    return

                self.heartbeat()

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                # This can only really happen if the worker can't read the DB for a long time
                time_since_last_heartbeat = (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr('local_task_job_prolonged_heartbeat_failure', 1, 1)
                    self.log.error("Heartbeat time limit exceeded!")
                    raise AirflowException(
                        f"Time since last heartbeat({time_since_last_heartbeat:.2f}s) exceeded limit "
                        f"({heartbeat_time_limit}s)."
                    )
        finally:
            self.on_kill()

    def handle_task_exit(self, return_code: int) -> None:
        """Handle case where self.task_runner exits by itself or is externally killed"""
        # Without setting this, heartbeat may get us
        self.terminating = True
        self.log.info("Task exited with return code %s", return_code)
        self.task_instance.refresh_from_db()

        if self.task_instance.state == State.RUNNING:
            # This is for a case where the task received a SIGKILL
            # while running or the task runner received a sigterm
            self.task_instance.handle_failure(error=None)
        # We need to check for error file
        # in case it failed due to runtime exception/error
        error = None
        if self.task_instance.state != State.SUCCESS:
            error = self.task_runner.deserialize_run_error()
        self.task_instance._run_finished_callback(error=error)
        if not self.task_instance.test_mode:
            if conf.getboolean('scheduler', 'schedule_after_task_execution', fallback=True):
                self._run_mini_scheduler_on_child_tasks()
            self._update_dagrun_state_for_paused_dag()

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
                self.log.error(
                    "The recorded hostname %s does not match this instance's hostname %s",
                    ti.hostname,
                    fqdn,
                )
                raise AirflowException("Hostname of job runner does not match")
            current_pid = self.task_runner.process.pid
            recorded_pid = ti.pid
            same_process = recorded_pid == current_pid

            if ti.run_as_user or self.task_runner.run_as_user:
                recorded_pid = psutil.Process(ti.pid).ppid()
                same_process = recorded_pid == current_pid

            if recorded_pid is not None and not same_process:
                self.log.warning(
                    "Recorded pid %s does not match the current pid %s", recorded_pid, current_pid
                )
                raise AirflowException("PID of job runner does not match")
        elif self.task_runner.return_code() is None and hasattr(self.task_runner, 'process'):
            if ti.state == State.SKIPPED:
                # A DagRun timeout will cause tasks to be externally marked as skipped.
                dagrun = ti.get_dagrun(session=session)
                execution_time = (dagrun.end_date or timezone.utcnow()) - dagrun.start_date
                dagrun_timeout = ti.task.dag.dagrun_timeout
                if dagrun_timeout and execution_time > dagrun_timeout:
                    self.log.warning("DagRun timed out after %s.", str(execution_time))
            self.log.warning(
                "State of this instance has been externally set to %s. Terminating instance.", ti.state
            )
            self.task_runner.terminate()
            if ti.state == State.SUCCESS:
                error = None
            else:
                # if ti.state is not set by taskinstance.handle_failure, then
                # error file will not be populated and it must be updated by
                # external source such as web UI
                error = self.task_runner.deserialize_run_error() or "task marked as failed externally"
            ti._run_finished_callback(error=error)
            self.terminating = True

    @provide_session
    @Sentry.enrich_errors
    def _run_mini_scheduler_on_child_tasks(self, session=None) -> None:
        try:
            # Re-select the row with a lock
            dag_run = with_row_locks(
                session.query(DagRun).filter_by(
                    dag_id=self.dag_id,
                    run_id=self.task_instance.run_id,
                ),
                session=session,
            ).one()

            task = self.task_instance.task
            assert task.dag  # For Mypy.

            # Get a partial DAG with just the specific tasks we want to examine.
            # In order for dep checks to work correctly, we include ourself (so
            # TriggerRuleDep can check the state of the task we just executed).
            partial_dag = task.dag.partial_subset(
                task.downstream_task_ids,
                include_downstream=True,
                include_upstream=False,
                include_direct_upstream=True,
            )

            dag_run.dag = partial_dag
            info = dag_run.task_instance_scheduling_decisions(session)

            skippable_task_ids = {
                task_id for task_id in partial_dag.task_ids if task_id not in task.downstream_task_ids
            }

            schedulable_tis = [ti for ti in info.schedulable_tis if ti.task_id not in skippable_task_ids]
            for schedulable_ti in schedulable_tis:
                if not hasattr(schedulable_ti, "task"):
                    schedulable_ti.task = task.dag.get_task(schedulable_ti.task_id)

            num = dag_run.schedule_tis(schedulable_tis)
            self.log.info("%d downstream tasks scheduled from follow-on schedule check", num)

            session.commit()
        except OperationalError as e:
            # Any kind of DB error here is _non fatal_ as this block is just an optimisation.
            self.log.info(
                "Skipping mini scheduling run due to exception: %s",
                e.statement,
                exc_info=True,
            )
            session.rollback()

    @provide_session
    def _update_dagrun_state_for_paused_dag(self, session=None):
        """
        Checks for paused dags with DagRuns in the running state and
        update the DagRun state if possible
        """
        dag = self.task_instance.task.dag
        if dag.get_is_paused():
            dag_run = self.task_instance.get_dagrun(session=session)
            if dag_run:
                dag_run.dag = dag
                dag_run.update_state(session=session, execute_callbacks=True)

    @staticmethod
    def _enable_task_listeners():
        """
        Check if we have any registered listeners, then register sqlalchemy hooks for
        TI state change if we do.
        """
        if get_listener_manager().has_listeners:
            register_task_instance_state_events()
