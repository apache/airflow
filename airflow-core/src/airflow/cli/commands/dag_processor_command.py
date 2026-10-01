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
from airflow.configuration import conf
from airflow.dag_processing.bundles.manager import _get_configured_bundle_team_names
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.dag_processor_job_runner import DagProcessorJobRunner
from airflow.jobs.job import Job, run_job
from airflow.utils import cli as cli_utils
from airflow.utils.memray_utils import MemrayTraceComponents, enable_memray_trace
from airflow.utils.process_utils import set_component_mp_start_method
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


def _get_team_names(bundle_names: list[str] | None) -> list[str]:
    """
    Return the teams this Dag processor serves, sorted and de-duplicated.

    Teams are resolved from the bundle configuration rather than the metadata DB: the job row is
    written before ``sync_bundles()`` runs, so a DB lookup would see no rows on a fresh deployment
    and stale rows right after a bundle is reassigned in config. Config is the source of truth, and
    is what ``airflow_health.py`` resolves teams from too.

    A processor started without ``--bundle-name`` parses every configured bundle, so it serves
    every configured team. A bundle mapped to no team contributes no team, so a processor parsing
    only team-less (or unknown) bundles is not team-scoped and serves the empty list. Outside
    multi-team mode team scoping is disabled entirely, matching ``DagFileProcessorManager``.
    """
    if not conf.getboolean("core", "multi_team"):
        return []

    configured = _get_configured_bundle_team_names()
    names = bundle_names or list(configured)
    return sorted({team for name in names if (team := configured.get(name)) is not None})


def _create_dag_processor_job_runner(args: Any) -> DagProcessorJobRunner:
    """Create DagFileProcessorProcess instance."""
    if args.bundle_name:
        cli_utils.validate_dag_bundle_arg(args.bundle_name)
    return DagProcessorJobRunner(
        job=Job(bundle_names=args.bundle_name, team_names=_get_team_names(args.bundle_name)),
        processor=DagFileProcessorManager(
            max_runs=args.num_runs,
            bundle_names_to_parse=args.bundle_name,
        ),
    )


@enable_memray_trace(component=MemrayTraceComponents.dag_processor)
def _run_dag_processor_job(job_runner: DagProcessorJobRunner) -> None:
    run_job(job=job_runner.job, execute_callable=job_runner._execute)


@cli_utils.action_cli
@providers_configuration_loaded
def dag_processor(args):
    """Start Airflow Dag Processor Job."""
    set_component_mp_start_method("dag_processor")
    job_runner = _create_dag_processor_job_runner(args)

    if cli_utils.should_enable_hot_reload(args):
        from airflow.cli.hot_reload import run_with_reloader

        run_with_reloader(
            lambda: _run_dag_processor_job(job_runner),
            process_name="dag-processor",
        )
        return

    run_command_with_daemon_option(
        args=args,
        process_name="dag-processor",
        callback=lambda: _run_dag_processor_job(job_runner),
        should_setup_logging=True,
    )
