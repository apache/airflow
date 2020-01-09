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

import os

import psutil
from setproctitle import setproctitle

from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import reap_process_group

CAN_FORK = hasattr(os, 'fork')


class StandardTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task by invoking through the Bash shell.
    """
    def __init__(self, local_task_job):
        super(StandardTaskRunner, self).__init__(local_task_job)
        self._rc = None
        self.dag = local_task_job.task_instance.task.dag

    def start(self):
        if CAN_FORK and not self.run_as_user:
            self.process = self._start_by_fork()
        else:
            self.process = self._start_by_exec()

    def _start_by_exec(self):
        subprocess = self.run_command()
        return psutil.Process(subprocess.pid)

    def _start_by_fork(self):
        pid = os.fork()
        if pid:
            self.log.info("Started process %d to run task", pid)
            return psutil.Process(pid)
        else:
            from airflow.bin.cli import get_parser
            import signal
            import airflow.settings as settings

            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            # Start a new process group
            os.setpgid(0, 0)

            # Force a new SQLAlchemy session. We can't share open DB handles
            # between process. The cli code will re-create this as part of its
            # normal startup
            settings.engine.pool.dispose()
            settings.engine.dispose()

            parser = get_parser()
            # [1:] - remove "airflow" from the start of the command
            args = parser.parse_args(self._command[1:])

            proc_title = "airflow task runner: {0.dag_id} {0.task_id} {0.execution_date}"
            if hasattr(args, "job_id"):
                proc_title += " {0.job_id}"
            setproctitle(proc_title.format(args))

            try:
                args.func(args, dag=self.dag)
                os._exit(0)
            except Exception:
                os._exit(1)

    def return_code(self, timeout=0):
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

        if self.process.is_running():
            rcs = reap_process_group(self.process.pid, self.log)
            self._rc = rcs.get(self.process.pid)

        self.process = None

        if self._rc is None:
            # Something else reaped it before we had a chance, so let's just "guess" at an error code.
            self._rc = -9
