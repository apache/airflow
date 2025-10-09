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

from collections.abc import Generator
from contextlib import contextmanager
from functools import partial
from multiprocessing import Process

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.configuration import conf
from airflow.jobs.job import Job, run_job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@contextmanager
def _serve_logs(skip_serve_logs: bool = False) -> Generator[None, None, None]:
    """Start serve_logs sub-process."""
    from airflow.utils.serve_logs import serve_logs

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


def triggerer_run(skip_serve_logs: bool, capacity: int, triggerer_heartrate: float):
    with _serve_logs(skip_serve_logs):
        triggerer_job_runner = TriggererJobRunner(job=Job(heartrate=triggerer_heartrate), capacity=capacity)
        run_job(job=triggerer_job_runner.job, execute_callable=triggerer_job_runner._execute)


@cli_utils.action_cli
@providers_configuration_loaded
def triggerer(args):
    """Start Airflow Triggerer."""
    from airflow.sdk._shared.secrets_masker import SecretsMasker

    SecretsMasker.enable_log_masking()

    print(settings.HEADER)
    triggerer_heartrate = conf.getfloat("triggerer", "JOB_HEARTBEAT_SEC")

    run_command_with_daemon_option(
        args=args,
        process_name="triggerer",
        callback=lambda: triggerer_run(args.skip_serve_logs, args.capacity, triggerer_heartrate),
        should_setup_logging=True,
    )
