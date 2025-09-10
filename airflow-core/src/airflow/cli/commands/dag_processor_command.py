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
"""DagProcessor command."""

from __future__ import annotations

import logging
from typing import Any

from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, run_job
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


def _create_dag_processor_job_runner(args: Any) -> DagProcessorJobRunner:
    """Create DagFileProcessorProcess instance."""
    if args.bundle_name:
        cli_utils.validate_dag_bundle_arg(args.bundle_name)
    return DagProcessorJobRunner(
        job=Job(),
        processor=DagFileProcessorManager(
            max_runs=args.num_runs,
            bundle_names_to_parse=args.bundle_name,
        ),
    )


@cli_utils.action_cli
@providers_configuration_loaded
def dag_processor(args):
    """Start Airflow Dag Processor Job."""
    job_runner = _create_dag_processor_job_runner(args)

    run_command_with_daemon_option(
        args=args,
        process_name="dag-processor",
        callback=lambda: run_job(job=job_runner.job, execute_callable=job_runner._execute),
        should_setup_logging=True,
    )
