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
"""Airflow worker sub-commands"""

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import conf
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging
from airflow.worker.worker import Worker

BANNER = r"""
     _    _       __ _                __        __         _
    / \  (_)_ __ / _| | _____      __ \ \      / /__  _ __| | _____ _ __
   / _ \ | | '__| |_| |/ _ \ \ /\ / /  \ \ /\ / / _ \| '__| |/ / _ \ '__|
  / ___ \| | |  |  _| | (_) \ V  V /    \ V  V / (_) | |  |   <  __/ |
 /_/   \_\_|_|  |_| |_|\___/ \_/\_/      \_/\_/ \___/|_|  |_|\_\___|_|
"""


@cli_utils.action_logging
def start(args):
    """
    Starts Airflow worker.
    """
    concurrency: int = conf.getint("worker", "concurrency")
    print(BANNER, end="\n")
    print(f"CONCURRENCY: {concurrency}", end="\n")
    worker = Worker(concurrency)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations(
            "consumer", args.pid, args.stdout, args.stderr, args.log_file
        )
        handle = setup_logging(log_file)
        with open(stdout, "w+") as stdout, open(stderr, "w+") as stderr:
            with daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                files_preserve=[handle],
                stdout=stdout,
                stderr=stderr,
            ):
                worker.start()
    else:
        worker.start()


@cli_utils.action_logging
def stop(args):
    """
    Stops Airflow workers by sending SIGTERM.
    """
    print("Stopping worker")
    Worker.stop()
