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
"""Triggerer command."""
from __future__ import annotations

import signal
from contextlib import contextmanager
from functools import partial
from multiprocessing import Process
from typing import Generator

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.configuration import conf
from airflow.jobs.job import Job, run_job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging, sigint_handler, sigquit_handler
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.serve_logs import serve_logs


@contextmanager
def _serve_logs(skip_serve_logs: bool = False) -> Generator[None, None, None]:
    """Starts serve_logs sub-process."""
    sub_proc = None
    if skip_serve_logs is False:
        port = conf.getint("logging", "trigger_log_server_port", fallback=8794)
        sub_proc = Process(target=partial(serve_logs, port=port))
        sub_proc.start()
    try:
        yield
    finally:
        if sub_proc:
            sub_proc.terminate()


@cli_utils.action_cli
@providers_configuration_loaded
def triggerer(args):
    """Starts Airflow Triggerer."""
    settings.MASK_SECRETS_IN_LOGS = True
    print(settings.HEADER)
    triggerer_heartrate = conf.getfloat("triggerer", "JOB_HEARTBEAT_SEC")
    triggerer_job_runner = TriggererJobRunner(job=Job(heartrate=triggerer_heartrate), capacity=args.capacity)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations(
            "triggerer", args.pid, args.stdout, args.stderr, args.log_file
        )
        handle = setup_logging(log_file)
        with open(stdout, "a") as stdout_handle, open(stderr, "a") as stderr_handle:
            stdout_handle.truncate(0)
            stderr_handle.truncate(0)

            daemon_context = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                files_preserve=[handle],
                stdout=stdout_handle,
                stderr=stderr_handle,
                umask=int(settings.DAEMON_UMASK, 8),
            )
            with daemon_context, _serve_logs(args.skip_serve_logs):
                run_job(job=triggerer_job_runner.job, execute_callable=triggerer_job_runner._execute)
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        with _serve_logs(args.skip_serve_logs):
            run_job(job=triggerer_job_runner.job, execute_callable=triggerer_job_runner._execute)
