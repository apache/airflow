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
import threading
import time
from typing import TYPE_CHECKING

import psutil
from setproctitle import setproctitle

from airflow.models.taskinstance import TaskReturnCode
from airflow.settings import CAN_FORK
from airflow.stats import Stats
from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.dag_parsing_context import _airflow_parsing_context_manager
from airflow.utils.process_utils import reap_process_group, set_new_process_group

if TYPE_CHECKING:
    from airflow.jobs.local_task_job_runner import LocalTaskJobRunner


class StandardTaskRunner(BaseTaskRunner):
    """Standard runner for all tasks."""

    def __init__(self, job_runner: LocalTaskJobRunner):
        super().__init__(job_runner=job_runner)
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

            # We prefer the job_id passed on the command-line because at this time, the
            # task instance may not have been updated.
            job_id = getattr(args, "job_id", self._task_instance.job_id)
            self.log.info("Running: %s", self._command)
            self.log.info("Job %s: Subtask %s", job_id, self._task_instance.task_id)

            proc_title = (
                "airflow task runner: {0.dag_id} {0.task_id} {0.execution_date_or_run_id}"
            )
            if job_id is not None:
                proc_title += " {0.job_id}"
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
            except Exception as exc:
                return_code = 1

                self.log.exception(
                    "Failed to execute job %s for task %s (%s; %r)",
                    job_id,
                    self._task_instance.task_id,
                    exc,
                    os.getpid(),
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
                    "Job %s was killed before it finished (likely due to running out of memory)",
                    "For more information, see https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#LocalTaskJob-killed",
                ),
                self._task_instance.job_id,
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
            self.log.info(
                "Process not found (most likely exited), stop collecting metrics"
            )
            return
