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

import signal

import psutil

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.jobs.base_job import BaseJob
from airflow.models.taskinstance import TaskInstance, TaskReturnCode
from airflow.stats import Stats
from airflow.task.task_runner import get_task_runner
from airflow.utils import timezone
from airflow.utils.log.file_task_handler import _set_task_deferred_context_var
from airflow.utils.net import get_hostname
from airflow.utils.platform import IS_WINDOWS
from airflow.utils.session import provide_session
from airflow.utils.state import State

SIGSEGV_MESSAGE = """
******************************************* Received SIGSEGV *******************************************
SIGSEGV (Segmentation Violation) signal indicates Segmentation Fault error which refers to
an attempt by a program/library to write or read outside its allocated memory.

In Python environment usually this signal refers to libraries which use low level C API.
Make sure that you use use right libraries/Docker Images
for your architecture (Intel/ARM) and/or Operational System (Linux/macOS).

Suggested way to debug
======================
  - Set environment variable 'PYTHONFAULTHANDLER' to 'true'.
  - Start airflow services.
  - Restart failed airflow task.
  - Check 'scheduler' and 'worker' services logs for additional traceback
    which might contain information about module/library where actual error happen.

Known Issues
============

Note: Only Linux-based distros supported as "Production" execution environment for Airflow.

macOS
-----
 1. Due to limitations in Apple's libraries not every process might 'fork' safe.
    One of the general error is unable to query the macOS system configuration for network proxies.
    If your are not using a proxy you could disable it by set environment variable 'no_proxy' to '*'.
    See: https://github.com/python/cpython/issues/58037 and https://bugs.python.org/issue30385#msg293958
********************************************************************************************************"""


class LocalTaskJob(BaseJob):
    """LocalTaskJob runs a single task instance."""

    __mapper_args__ = {"polymorphic_identity": "LocalTaskJob"}

    def __init__(
        self,
        task_instance: TaskInstance,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        pickle_id: str | None = None,
        pool: str | None = None,
        external_executor_id: str | None = None,
        *args,
        **kwargs,
    ):
        self.task_instance = task_instance
        self.dag_id = task_instance.dag_id
        self.ignore_all_deps = ignore_all_deps
        self.ignore_depends_on_past = ignore_depends_on_past
        self.wait_for_past_depends_before_skipping = wait_for_past_depends_before_skipping
        self.ignore_task_deps = ignore_task_deps
        self.ignore_ti_state = ignore_ti_state
        self.pool = pool
        self.pickle_id = pickle_id
        self.mark_success = mark_success
        self.external_executor_id = external_executor_id

        # terminating state is used so that a job don't try to
        # terminate multiple times
        self.terminating = False

        self._state_change_checks = 0

        super().__init__(*args, **kwargs)

    def _execute(self) -> int | None:
        self.task_runner = get_task_runner(self)

        def signal_handler(signum, frame):
            """Setting kill signal handler."""
            self.log.error("Received SIGTERM. Terminating subprocesses")
            self.task_runner.terminate()
            self.handle_task_exit(128 + signum)

        def segfault_signal_handler(signum, frame):
            """Setting sigmentation violation signal handler"""
            self.log.critical(SIGSEGV_MESSAGE)
            self.task_runner.terminate()
            self.handle_task_exit(128 + signum)
            raise AirflowException("Segmentation Fault detected.")

        def sigusr2_debug_handler(signum, frame):
            import sys
            import threading
            import traceback

            id2name = {th.ident: th.name for th in threading.enumerate()}
            for threadId, stack in sys._current_frames().items():
                print(id2name[threadId])
                traceback.print_stack(f=stack)

        signal.signal(signal.SIGSEGV, segfault_signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        if not IS_WINDOWS:
            # This is not supported on Windows systems
            signal.signal(signal.SIGUSR2, sigusr2_debug_handler)

        if not self.task_instance.check_and_change_state_before_execution(
            mark_success=self.mark_success,
            ignore_all_deps=self.ignore_all_deps,
            ignore_depends_on_past=self.ignore_depends_on_past,
            wait_for_past_depends_before_skipping=self.wait_for_past_depends_before_skipping,
            ignore_task_deps=self.ignore_task_deps,
            ignore_ti_state=self.ignore_ti_state,
            job_id=self.id,
            pool=self.pool,
            external_executor_id=self.external_executor_id,
        ):
            self.log.info("Task is not able to be run")
            return None

        return_code = None
        try:
            self.task_runner.start()

            heartbeat_time_limit = conf.getint("scheduler", "scheduler_zombie_task_threshold")

            # LocalTaskJob should not run callbacks, which are handled by TaskInstance._run_raw_task
            # 1, LocalTaskJob does not parse DAG, thus cannot run callbacks
            # 2, The run_as_user of LocalTaskJob is likely not same as the TaskInstance._run_raw_task.
            # When run_as_user is specified, the process owner of the LocalTaskJob must be sudoable.
            # It is not secure to run callbacks with sudoable users.

            # If _run_raw_task receives SIGKILL, scheduler will mark it as zombie and invoke callbacks
            # If LocalTaskJob receives SIGTERM, LocalTaskJob passes SIGTERM to _run_raw_task
            # If the state of task_instance is changed, LocalTaskJob sends SIGTERM to _run_raw_task
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
                    return return_code

                self.heartbeat()

                # If it's been too long since we've heartbeat, then it's possible that
                # the scheduler rescheduled this task, so kill launched processes.
                # This can only really happen if the worker can't read the DB for a long time
                time_since_last_heartbeat = (timezone.utcnow() - self.latest_heartbeat).total_seconds()
                if time_since_last_heartbeat > heartbeat_time_limit:
                    Stats.incr("local_task_job_prolonged_heartbeat_failure", 1, 1)
                    self.log.error("Heartbeat time limit exceeded!")
                    raise AirflowException(
                        f"Time since last heartbeat({time_since_last_heartbeat:.2f}s) exceeded limit "
                        f"({heartbeat_time_limit}s)."
                    )
            return return_code
        finally:
            self.on_kill()

    def handle_task_exit(self, return_code: int) -> None:
        """
        Handle case where self.task_runner exits by itself or is externally killed.

        Don't run any callbacks.
        """
        # Without setting this, heartbeat may get us
        self.terminating = True
        self._log_return_code_metric(return_code)
        is_deferral = return_code == TaskReturnCode.DEFERRED.value
        if is_deferral:
            self.log.info("Task exited with return code %s (task deferral)", return_code)
            _set_task_deferred_context_var()
        else:
            self.log.info("Task exited with return code %s", return_code)

        if not self.task_instance.test_mode and not is_deferral:
            if conf.getboolean("scheduler", "schedule_after_task_execution", fallback=True):
                self.task_instance.schedule_downstream_tasks()

    def on_kill(self):
        self.task_runner.terminate()
        self.task_runner.on_finish()

    @provide_session
    def heartbeat_callback(self, session=None):
        """Self destruct task if state has been moved away from running externally."""
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

            if recorded_pid is not None and (ti.run_as_user or self.task_runner.run_as_user):
                # when running as another user, compare the task runner pid to the parent of
                # the recorded pid because user delegation becomes an extra process level.
                # However, if recorded_pid is None, pass that through as it signals the task
                # runner process has already completed and been cleared out. `psutil.Process`
                # uses the current process if the parameter is None, which is not what is intended
                # for comparison.
                recorded_pid = psutil.Process(ti.pid).ppid()
                same_process = recorded_pid == current_pid

            if recorded_pid is not None and not same_process:
                self.log.warning(
                    "Recorded pid %s does not match the current pid %s", recorded_pid, current_pid
                )
                raise AirflowException("PID of job runner does not match")
        elif self.task_runner.return_code() is None and hasattr(self.task_runner, "process"):
            if ti.state == State.SKIPPED:
                # A DagRun timeout will cause tasks to be externally marked as skipped.
                dagrun = ti.get_dagrun(session=session)
                execution_time = (dagrun.end_date or timezone.utcnow()) - dagrun.start_date
                dagrun_timeout = ti.task.dag.dagrun_timeout
                if dagrun_timeout and execution_time > dagrun_timeout:
                    self.log.warning("DagRun timed out after %s.", str(execution_time))

            # potential race condition, the _run_raw_task commits `success` or other state
            # but task_runner does not exit right away due to slow process shutdown or any other reasons
            # let's do a throttle here, if the above case is true, the handle_task_exit will handle it
            if self._state_change_checks >= 1:  # defer to next round of heartbeat
                self.log.warning(
                    "State of this instance has been externally set to %s. Terminating instance.", ti.state
                )
                self.terminating = True
            self._state_change_checks += 1

    def _log_return_code_metric(self, return_code: int):
        Stats.incr(
            f"local_task_job.task_exit.{self.id}.{self.dag_id}.{self.task_instance.task_id}.{return_code}"
        )
