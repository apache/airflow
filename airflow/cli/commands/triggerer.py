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

"""Triggerer command"""
import signal

import daemon
import rich_click as click
from daemon.pidfile import TimeoutPIDLockFile
from rich.console import Console

from airflow import settings
from airflow.cli import airflow_cmd, click_daemon, click_log_file, click_pid, click_stderr, click_stdout
from airflow.jobs.triggerer_job import TriggererJob
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging, sigint_handler, sigquit_handler


@airflow_cmd.command('triggerer')
@click.option(
    "--capacity",
    type=click.IntRange(min=1),
    help="The maximum number of triggers that a Triggerer will run at one time",
)
@click_daemon
@click_log_file
@click_pid
@click_stderr
@click_stdout
@cli_utils.action_cli
def triggerer(ctx, capacity, daemon_, log_file, pid, stderr, stdout):
    """Starts Airflow Triggerer"""
    console = Console()
    settings.MASK_SECRETS_IN_LOGS = True
    console.print(settings.HEADER)
    job = TriggererJob(capacity=capacity)

    if daemon_:
        pid, stdout, stderr, log_file = setup_locations("triggerer", pid, stdout, stderr, log_file)
        handle = setup_logging(log_file)
        with open(stdout, 'w+') as stdout_handle, open(stderr, 'w+') as stderr_handle:
            ctx = daemon.DaemonContext(
                pidfile=TimeoutPIDLockFile(pid, -1),
                files_preserve=[handle],
                stdout=stdout_handle,
                stderr=stderr_handle,
            )
            with ctx:
                job.run()

    else:
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
        signal.signal(signal.SIGQUIT, sigquit_handler)
        job.run()
