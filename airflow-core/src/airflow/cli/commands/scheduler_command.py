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

import logging
from argparse import Namespace
from contextlib import contextmanager
from multiprocessing import Process

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.configuration import conf
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.job import Job, run_job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.scheduler_health import serve_health_check

log = logging.getLogger(__name__)


def _run_scheduler_job(args) -> None:
    job_runner = SchedulerJobRunner(job=Job(), num_runs=args.num_runs)
    enable_health_check = conf.getboolean("scheduler", "ENABLE_HEALTH_CHECK")
    with _serve_logs(args.skip_serve_logs), _serve_health_check(enable_health_check):
        run_job(job=job_runner.job, execute_callable=job_runner._execute)


@cli_utils.action_cli
@providers_configuration_loaded
def scheduler(args: Namespace):
    """Start Airflow Scheduler."""
    print(settings.HEADER)

    run_command_with_daemon_option(
        args=args,
        process_name="scheduler",
        callback=lambda: _run_scheduler_job(args),
        should_setup_logging=True,
    )


@contextmanager
def _serve_logs(skip_serve_logs: bool = False):
    from airflow.utils.serve_logs import serve_logs

    sub_proc = None
    executor_class, _ = ExecutorLoader.import_default_executor_cls()
    if executor_class.serve_logs:
        if skip_serve_logs is False:
            sub_proc = Process(target=serve_logs)
            sub_proc.start()
    try:
        yield
    finally:
        if sub_proc:
            sub_proc.terminate()


@contextmanager
def _serve_health_check(enable_health_check: bool = False):
    """Start serve_health_check sub-process."""
    sub_proc = None
    if enable_health_check:
        sub_proc = Process(target=serve_health_check)
        sub_proc.start()
    try:
        yield
    finally:
        if sub_proc:
            sub_proc.terminate()
