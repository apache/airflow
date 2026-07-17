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

import logging
import os
import signal
import sys
from pathlib import Path

import psutil
from lockfile.pidlockfile import (
    read_pid_from_pidfile,
    remove_existing_pidfile,
    write_pid_to_pidfile as write_pid,
)

from airflow.utils import cli as cli_utils
from airflow.utils.platform import IS_WINDOWS

EDGE_WORKER_PROCESS_NAME = "edge-worker"

logger = logging.getLogger(__name__)


def _status_signal() -> signal.Signals:
    if IS_WINDOWS:
        return signal.SIGBREAK  # type: ignore[attr-defined]
    return signal.SIGUSR2


SIG_STATUS = _status_signal()


def write_pid_to_pidfile(pid_file_path: str):
    """Write PIDs for Edge Workers to disk, handling existing PID files."""
    if Path(pid_file_path).exists():
        # Handle existing PID files on disk
        logger.info("An existing PID file has been found: %s.", pid_file_path)
        pid_stored_in_pid_file = read_pid_from_pidfile(pid_file_path)
        if os.getpid() == pid_stored_in_pid_file:
            raise SystemExit("A PID file has already been written")
        # PID file was written by dead or already running instance
        if psutil.pid_exists(pid_stored_in_pid_file):
            # case 1: another instance uses the same path for its PID file
            raise SystemExit(
                f"The PID file {pid_file_path} contains the PID of another running process. "
                "Configuration issue: edge worker instance must use different PID file paths!"
            )
        # case 2: previous instance crashed without cleaning up its PID file
        logger.warning("PID file is orphaned. Cleaning up.")
        remove_existing_pidfile(pid_file_path)
    logger.debug("PID file written to %s.", pid_file_path)
    write_pid(pid_file_path)


def pid_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[0]


def get_pid(pid_file: str | None) -> int:
    pid = read_pid_from_pidfile(pid_file_path(pid_file))
    if not pid:
        logger.warning("Could not find PID of worker.")
        sys.exit(1)
    return pid


def status_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[1]


def maintenance_marker_file_path(pid_file: str | None) -> str:
    return cli_utils.setup_locations(process=EDGE_WORKER_PROCESS_NAME, pid=pid_file)[1][:-4] + ".in"
