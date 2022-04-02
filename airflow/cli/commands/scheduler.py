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

"""Scheduler command"""
import signal
from multiprocessing import Process
from typing import Optional

import daemon
import rich_click as click
from daemon.pidfile import TimeoutPIDLockFile
from rich.console import Console

from airflow import settings
from airflow.cli import (
    airflow_cmd,
    click_daemon,
    click_log_file,
    click_pid,
    click_stderr,
    click_stdout,
    click_subdir,
)
from airflow.configuration import conf
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.utils import cli as cli_utils
from airflow.utils.cli import process_subdir, setup_locations, setup_logging, sigint_handler, sigquit_handler


def _create_scheduler_job(subdir, num_runs, do_pickle):
    job = SchedulerJob(
        subdir=process_subdir(subdir),
        num_runs=num_runs,
        do_pickle=do_pickle,
    )
    return job


def _run_scheduler_job(subdir, num_runs, do_pickle, skip_serve_logs):
    job = _create_scheduler_job(subdir, num_runs, do_pickle)
    sub_proc = _serve_logs(skip_serve_logs)
    try:
        job.run()
    finally:
        if sub_proc:
            sub_proc.terminate()


@airflow_cmd.command('scheduler')
@click_daemon
@click.option(
    "-p",
    "--do-pickle",
    is_flag=True,
    default=False,
    help=(
        "Attempt to pickle the DAG object to send over to the workers, instead of letting workers "
        "run their version of the code"
    ),
)
@click_log_file
@click.option(
    "-n",
    "--num-runs",
    type=int,
    default=conf.get('scheduler', 'num_runs'),
    help="Set the number of runs to execute before exiting",
)
@click_pid
@click.option(
    "-s",
    "--skip-serve-logs",
    is_flag=True,
    default=False,
    help="Don't start the serve logs process along with the workers",
)
@click_stderr
@click_stdout
@click_subdir
@cli_utils.action_cli
def scheduler(ctx, daemon_, do_pickle, log_file, num_runs, pid, skip_serve_logs, stderr, stdout, subdir):
    """Starts Airflow Scheduler"""
    console = Console()
    console.print(settings.HEADER)

    if daemon_:
        pid, stdout, stderr, log_file = setup_locations("scheduler", pid, stdout, stderr, log_file)
        handle = setup_logging(log_file)
        with open(stdout, 'w+') as stdout_handle, open(stderr, 'w+') as stderr_handle:
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                files_preserve=[handle],
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
            with ctx:
                _run_scheduler_job(subdir, num_runs, do_pickle, skip_serve_logs)
    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        _run_scheduler_job(subdir, num_runs, do_pickle, skip_serve_logs)


def _serve_logs(skip_serve_logs: bool = False) -> Optional[Process]:
    """Starts serve_logs sub-process"""
    from airflow.configuration import conf
    from airflow.utils.serve_logs import serve_logs

    if conf.get("core", "executor") in ["LocalExecutor", "SequentialExecutor"]:
        if skip_serve_logs is False:
            sub_proc = Process(target=serve_logs)
            sub_proc.start()
            return sub_proc
    return None
