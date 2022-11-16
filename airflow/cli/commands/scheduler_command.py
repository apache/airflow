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
"""Scheduler command."""
from __future__ import annotations

import signal
from multiprocessing import Process

import daemon
from daemon.pidfile import TimeoutPIDLockFile

from airflow import settings
from airflow.configuration import conf
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils import cli as cli_utils
from airflow.utils.cli import process_subdir, setup_locations, setup_logging, sigint_handler, sigquit_handler
from airflow.utils.scheduler_health import serve_health_check


def _create_scheduler_job(args):
    job = SchedulerJob(
        subdir=process_subdir(args.subdir),
        num_runs=args.num_runs,
        do_pickle=args.do_pickle,
    )
    return job


def _run_scheduler_job(args):
    skip_serve_logs = args.skip_serve_logs
    job = _create_scheduler_job(args)
    logs_sub_proc = _serve_logs(skip_serve_logs)
    enable_health_check = conf.getboolean("scheduler", "ENABLE_HEALTH_CHECK")
    health_sub_proc = _serve_health_check(enable_health_check)
    try:
        job.run()
    finally:
        if logs_sub_proc:
            logs_sub_proc.terminate()
        if health_sub_proc:
            health_sub_proc.terminate()


@cli_utils.action_cli
def scheduler(args):
    """Starts Airflow Scheduler."""
    print(settings.HEADER)

    if args.daemon:
        pid, stdout, stderr, log_file = setup_locations(
            "scheduler", args.pid, args.stdout, args.stderr, args.log_file
        )
        handle = setup_logging(log_file)
        with open(stdout, "a") as stdout_handle, open(stderr, "a") as stderr_handle:
            stdout_handle.truncate(0)
            stderr_handle.truncate(0)

            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                files_preserve=[handle],
                stdout=stdout_handle,
                stderr=stderr_handle,
                umask=int(settings.DAEMON_UMASK, 8),
            )
            with ctx:
                _run_scheduler_job(args=args)
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        _run_scheduler_job(args=args)


def _serve_logs(skip_serve_logs: bool = False) -> Process | None:
    """Starts serve_logs sub-process."""
    from airflow.configuration import conf
    from airflow.utils.serve_logs import serve_logs

    if conf.get("core", "executor") in ["LocalExecutor", "SequentialExecutor"]:
        if skip_serve_logs is False:
            sub_proc = Process(target=serve_logs)
            sub_proc.start()
            return sub_proc
    return None


def _serve_health_check(enable_health_check: bool = False) -> Process | None:
    """Starts serve_health_check sub-process."""
    if enable_health_check:
        sub_proc = Process(target=serve_health_check)
        sub_proc.start()
        return sub_proc
    return None
