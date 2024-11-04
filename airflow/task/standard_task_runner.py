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
"""Standard task runner."""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from typing import TYPE_CHECKING

import psutil
from setproctitle import setproctitle

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.models.taskinstance import TaskReturnCode
from airflow.settings import CAN_FORK
from airflow.stats import Stats
from airflow.utils.configuration import tmp_configuration_copy
from airflow.utils.dag_parsing_context import _airflow_parsing_context_manager
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.platform import IS_WINDOWS, getuser
from airflow.utils.process_utils import reap_process_group, set_new_process_group

if TYPE_CHECKING:
    from airflow.jobs.local_task_job_runner import LocalTaskJobRunner


class StandardTaskRunner(LoggingMixin):
    """
    Runs Airflow task instances via CLI.

    Invoke the `airflow tasks run` command with raw mode enabled in a subprocess.

    :param job_runner: The LocalTaskJobRunner associated with the task runner
    """

    def __init__(self, job_runner: LocalTaskJobRunner):
        self.job_runner = job_runner
        super().__init__(job_runner.task_instance)
        self._task_instance = job_runner.task_instance

        popen_prepend = []
        if self._task_instance.run_as_user:
            self.run_as_user: str | None = self._task_instance.run_as_user
        else:
            try:
                self.run_as_user = conf.get("core", "default_impersonation")
            except AirflowConfigException:
                self.run_as_user = None

        # Add sudo commands to change user if we need to. Needed to handle SubDagOperator
        # case using a SequentialExecutor.
        self.log.debug("Planning to run as the %s user", self.run_as_user)
        if self.run_as_user and (self.run_as_user != getuser()):
            # We want to include any environment variables now, as we won't
            # want to have to specify them in the sudo call - they would show
            # up in `ps` that way! And run commands now, as the other user
            # might not be able to run the cmds to get credentials
            cfg_path = tmp_configuration_copy(chmod=0o600, include_env=True, include_cmds=True)

            # Give ownership of file to user; only they can read and write
            subprocess.check_call(["sudo", "chown", self.run_as_user, cfg_path], close_fds=True)

            # propagate PYTHONPATH environment variable
            pythonpath_value = os.environ.get("PYTHONPATH", "")
            popen_prepend = ["sudo", "-E", "-H", "-u", self.run_as_user]

            if pythonpath_value:
                popen_prepend.append(f"PYTHONPATH={pythonpath_value}")

        else:
            # Always provide a copy of the configuration file settings. Since
            # we are running as the same user, and can pass through environment
            # variables then we don't need to include those in the config copy
            # - the runner can read/execute those values as it needs
            cfg_path = tmp_configuration_copy(chmod=0o600, include_env=False, include_cmds=False)

        self._cfg_path = cfg_path
        self._command = popen_prepend + self._task_instance.command_as_list(
            raw=True,
            pickle_id=self.job_runner.pickle_id,
            mark_success=self.job_runner.mark_success,
            pool=self.job_runner.pool,
            cfg_path=cfg_path,
        )
        self.process = None
        self._rc = None
        if TYPE_CHECKING:
            assert self._task_instance.task
        self.dag = self._task_instance.task.dag

    def start(self):
        if CAN_FORK and not self.run_as_user:
            self.process = self._start_by_fork()
        else:
            self.process = self._start_by_exec()

        if self.process:
            resource_monitor = threading.Thread(target=self._read_task_utilization)
            resource_monitor.daemon = True
            resource_monitor.start()

    def _start_by_exec(self) -> psutil.Process:
        subprocess = self.run_command()
        self.process = psutil.Process(subprocess.pid)
        return self.process

    def _start_by_fork(self):
        pid = os.fork()
        if pid:
            self.log.info("Started process %d to run task", pid)
            return psutil.Process(pid)
        else:
            from airflow.api_internal.internal_api_call import InternalApiConfig
            from airflow.configuration import conf

            if conf.getboolean("core", "database_access_isolation", fallback=False):
                InternalApiConfig.set_use_internal_api("Forked task runner")
            # Start a new process group
            set_new_process_group()

            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

            from airflow import settings
            from airflow.cli.cli_parser import get_parser
            from airflow.sentry import Sentry

            if not InternalApiConfig.get_use_internal_api():
                # Force a new SQLAlchemy session. We can't share open DB handles
                # between process. The cli code will re-create this as part of its
                # normal startup
                settings.engine.pool.dispose()
                settings.engine.dispose()

            parser = get_parser()
            # [1:] - remove "airflow" from the start of the command
            args = parser.parse_args(self._command[1:])

            self.log.info("Running: %s", self._command)
            self.log.info("Task %s", self._task_instance.task_id)

            proc_title = "airflow task runner: {0.dag_id} {0.task_id} {0.execution_date_or_run_id}"
            setproctitle(proc_title.format(args))
            return_code = 0
            try:
                with _airflow_parsing_context_manager(
                    dag_id=self._task_instance.dag_id,
                    task_id=self._task_instance.task_id,
                ):
                    ret = args.func(args, dag=self.dag)
                    return_code = 0
                    if isinstance(ret, TaskReturnCode):
                        return_code = ret.value
            except Exception:
                return_code = 1

                self.log.exception(
                    "Failed to execute task_id=%s pid=%r", self._task_instance.task_id, os.getpid()
                )
            except SystemExit as sys_ex:
                # Someone called sys.exit() in the fork - mistakenly. You should not run sys.exit() in
                # the fork because you can mistakenly execute atexit that were set by the parent process
                # before fork happened
                return_code = sys_ex.code
            except BaseException:
                # while we want to handle Also Base exceptions here - we do not want to log them (this
                # is the default behaviour anyway. Setting the return code here to 2 to indicate that
                # this had happened.
                return_code = 2
            finally:
                try:
                    # Explicitly flush any pending exception to Sentry and logging if enabled
                    Sentry.flush()
                    logging.shutdown()
                except BaseException:
                    # also make sure to silently ignore ALL POSSIBLE exceptions thrown in the flush/shutdown,
                    # otherwise os._exit() might never be called. We could have used `except:` but
                    # except BaseException is more explicit (and linters do not comply).
                    pass
            # We run os._exit() making sure it is not run within the `finally` clause.
            # We cannot run os._exit() in finally clause, because during finally clause processing, the
            # Exception handled is held in memory as well as stack trace and possibly some objects that
            # might need to be finalized. Running os._exit() inside the `finally` clause might cause effects
            # similar to https://github.com/apache/airflow/issues/22404. There Temporary file has not been
            # deleted at os._exit()
            os._exit(return_code)

    def return_code(self, timeout: float = 0) -> int | None:
        # We call this multiple times, but we can only wait on the process once
        if self._rc is not None or not self.process:
            return self._rc

        try:
            self._rc = self.process.wait(timeout=timeout)
            self.process = None
        except psutil.TimeoutExpired:
            pass

        return self._rc

    def terminate(self):
        if self.process is None:
            return

        # Reap the child process - it may already be finished
        _ = self.return_code(timeout=0)

        if self.process and self.process.is_running():
            rcs = reap_process_group(self.process.pid, self.log)
            self._rc = rcs.get(self.process.pid)

        self.process = None

        if self._rc is None:
            # Something else reaped it before we had a chance, so let's just "guess" at an error code.
            self._rc = -signal.SIGKILL

        if self._rc == -signal.SIGKILL:
            self.log.error(
                (
                    "TI %s was killed before it finished (likely due to running out of memory)",
                    "For more information, see https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#LocalTaskJob-killed",
                ),
                self._task_instance.id,
            )

    def get_process_pid(self) -> int:
        if self.process is None:
            raise RuntimeError("Process is not started yet")
        return self.process.pid

    def _read_task_utilization(self):
        dag_id = self._task_instance.dag_id
        task_id = self._task_instance.task_id

        try:
            while True:
                with self.process.oneshot():
                    mem_usage = self.process.memory_percent()
                    cpu_usage = self.process.cpu_percent()

                    Stats.gauge(f"task.mem_usage.{dag_id}.{task_id}", mem_usage)
                    Stats.gauge(f"task.cpu_usage.{dag_id}.{task_id}", cpu_usage)
                    time.sleep(5)
        except (psutil.NoSuchProcess, psutil.AccessDenied, AttributeError):
            self.log.info("Process not found (most likely exited), stop collecting metrics")
            return

    def _read_task_logs(self, stream):
        while True:
            line = stream.readline()
            if isinstance(line, bytes):
                line = line.decode("utf-8")
            if not line:
                break
            self.log.info(
                "Task %s %s",
                self._task_instance.task_id,
                line.rstrip("\n"),
            )

    def run_command(self, run_with=None) -> subprocess.Popen:
        """
        Run the task command.

        :param run_with: list of tokens to run the task command with e.g. ``['bash', '-c']``
        :return: the process that was run
        """
        run_with = run_with or []
        full_cmd = run_with + self._command

        self.log.info("Running on host: %s", get_hostname())
        self.log.info("Running: %s", full_cmd)
        with _airflow_parsing_context_manager(
            dag_id=self._task_instance.dag_id,
            task_id=self._task_instance.task_id,
        ):
            if IS_WINDOWS:
                proc = subprocess.Popen(
                    full_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    close_fds=True,
                    env=os.environ.copy(),
                )
            else:
                proc = subprocess.Popen(
                    full_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                    close_fds=True,
                    env=os.environ.copy(),
                    preexec_fn=os.setsid,
                )

        # Start daemon thread to read subprocess logging output
        log_reader = threading.Thread(
            target=self._read_task_logs,
            args=(proc.stdout,),
        )
        log_reader.daemon = True
        log_reader.start()
        return proc

    def on_finish(self) -> None:
        """Execute when this is done running."""
        if self._cfg_path and os.path.isfile(self._cfg_path):
            if self.run_as_user:
                subprocess.call(["sudo", "rm", self._cfg_path], close_fds=True)
            else:
                os.remove(self._cfg_path)
