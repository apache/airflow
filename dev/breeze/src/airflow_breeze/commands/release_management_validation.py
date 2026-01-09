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

import sys
from pathlib import Path

import click

from airflow_breeze.commands.release_management_group import release_management_group
from airflow_breeze.utils.airflow_release_validator import AirflowReleaseValidator
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.release_validator import CheckType


@release_management_group.command(
    name="validate-rc-by-pmc",
    help="Validate release candidate for PMC voting",
)
@click.option(
    "--distribution",
    type=click.Choice(["airflow", "airflowctl", "providers", "python-client"]),
    default="airflow",
    help="Distribution type to validate",
)
@click.option(
    "--version",
    required=True,
    help="Release candidate version",
)
@click.option(
    "--task-sdk-version",
    help="Task SDK version",
)
@click.option(
    "--path-to-airflow-svn",
    "-p",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True, path_type=Path),
    envvar="PATH_TO_AIRFLOW_SVN",
    help="Path to directory where release files are checked out from SVN (e.g., ~/code/asf-dist/dev/airflow)",
)
@click.option(
    "--checks",
    help="Comma separated list of checks to run. Available: svn, reproducible-build, signatures, checksums, licenses. All by default.",
)
def validate_rc_by_pmc(
    distribution: str,
    version: str,
    task_sdk_version: str | None,
    path_to_airflow_svn: Path,
    checks: str | None,
):
    """
    Validate a release candidate for PMC voting.

    This command performs the validation checks required by the PMCs for a release.

    Examples:
        breeze release-management validate-rc-by-pmc \
            --distribution airflow \
            --version 3.1.3rc1 \
            --task-sdk-version 1.1.3rc1 \
            --path-to-airflow-svn ../asf-dist/dev/airflow \
            --checks signatures,checksums
    """
    airflow_repo_root = Path.cwd()

    if not (airflow_repo_root / "airflow-core").exists():
        console_print("[red]Error: Must be run from Airflow repository root[/red]")
        sys.exit(1)

    check_list = None
    if checks:
        try:
            check_list = [CheckType(c.strip()) for c in checks.split(",")]
        except ValueError as e:
            console_print(f"[red]Invalid check type: {e}[/red]")
            console_print(f"Available checks: {', '.join([c.value for c in CheckType])}")
            sys.exit(1)

    if distribution == "airflow":
        validator = AirflowReleaseValidator(
            version=version,
            path_to_airflow_svn=path_to_airflow_svn,
            airflow_repo_root=airflow_repo_root,
            task_sdk_version=task_sdk_version,
        )
    elif distribution == "airflowctl":
        console_print("[yellow]airflowctl validation not yet implemented[/yellow]")
        sys.exit(1)
    elif distribution == "providers":
        console_print("[yellow]providers validation not yet implemented[/yellow]")
        sys.exit(1)
    else:
        console_print(f"[red]Unknown distribution: {distribution}[/red]")
        sys.exit(1)

    if not validator.validate(checks=check_list):
        console_print(f"[red]Validation failed for {distribution} {version}[/red]")
        sys.exit(1)
