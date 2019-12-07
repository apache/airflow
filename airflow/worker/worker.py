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
This module contains Airflow worker.
"""

import os
import signal
import socket
import sys
from multiprocessing import Process
from time import sleep
from typing import Dict, Tuple

import psutil
import setproctitle

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.worker.task_worker import TaskWorker
from airflow.worker.utils import WORKER_PID_FILE_LOCATION, add_signal_handler

AIRFLOW_WORKER_NAME = "airflow-worker"


class Worker(LoggingMixin):
    """
    This class implements an Airflow Worker that runs many TaskWorkers.
    The number of TaskWorkers can be configured in airflow.cfg by setting
    worker.concurrency number.

    :param concurrency: Number of TaskWorkers to spawn.
    :type concurrency: int
    """

    def __init__(self, concurrency: int):
        self.name = f"{AIRFLOW_WORKER_NAME}-{socket.getfqdn()}"  # airflow-worker-hostname
        self.should_be_running = True
        self.processes: Dict[int, Tuple[Process, TaskWorker]] = {}
        self.task_workers = [TaskWorker for _ in range(concurrency)]

    def start(self) -> None:
        """
        Starts main loop that monitors TaskWorkers and tries to
        revive terminated workers.
        """
        # Create pid file and change hostname
        self._make_pid_file()

        # Add signal handlers for main worker
        add_signal_handler(self.exit_gracefully)

        # Set process name
        setproctitle.setproctitle(self.name)

        # Start children processes with TaskWorkers
        for wid, worker in enumerate(self.task_workers):
            app = worker()
            proc = Process(target=app.start, daemon=True)
            proc.start()
            self.processes[wid] = (proc, app)

        # The main loop to monitor and revive children
        while self.should_be_running:
            for wid, (proc, app) in self.processes.items():
                self.revive_process(wid, proc, app)
            sleep(0.5)

    def revive_process(self, wid: int, process: Process, app: TaskWorker) -> None:
        """
        Checks if process is still alive, if not then creates new one.
        """
        if not process.is_alive() and self.should_be_running:
            proc = Process(target=app.start, daemon=True)
            proc.start()
            self.processes[wid] = (proc, app)

    def exit_gracefully(self, signum, frame) -> None:  # pylint: disable=unused-argument
        """
        Provides graceful exit of the worker.
        """
        self.log.info("Gracefully stopping: %s", self.name)
        self.should_be_running = False
        sys.exit(0)

    def _make_pid_file(self) -> None:
        with open(WORKER_PID_FILE_LOCATION, "+w") as f:
            f.write(self.name)

    @staticmethod
    def _remove_pid_file() -> str:
        with open(WORKER_PID_FILE_LOCATION, "r") as f:
            pid = f.read()
        os.remove(WORKER_PID_FILE_LOCATION)
        return pid

    @classmethod
    def stop(cls) -> None:
        """
        Sends SIGTERM signal to all Airflow workers on single machine.
        """
        pid = cls._remove_pid_file()
        for proc in psutil.process_iter():
            if proc.name() == pid:
                proc.send_signal(signal.SIGTERM)
