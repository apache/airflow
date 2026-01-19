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
from airflow_breeze.utils.providers_release_validator import ProvidersReleaseValidator, parse_packages_file
from airflow_breeze.utils.release_validator import CheckType


@release_management_group.command(
    name="verify-rc-by-pmc",
    help=(
        "Verify a release candidate for PMC voting (EXPERIMENTAL).\n"
        "\n"
        "Runs the standard PMC verification steps (SVN files, signatures, checksums, licenses, reproducible builds) "
        "with extra safety/ergonomics.\n"
        "\n"
        "Note: This command is experimental; breaking changes might happen without notice. "
        "It is recommended to also follow the manual verification steps and compare results.\n"
        "\n"
        "Implementation note: for reproducible builds the automation uses a git worktree (detached) "
        "so it can build from the release tag while still using the latest Breeze code from your checkout."
    ),
)
@click.option(
    "--distribution",
    type=click.Choice(["airflow", "airflowctl", "providers", "python-client"]),
    default="airflow",
    help="Distribution type to verify",
)
@click.option(
    "--version",
    help="Release candidate version (required for airflow, airflowctl, python-client). E.g., 3.1.6rc1",
)
@click.option(
    "--task-sdk-version",
    help="Task SDK version (for airflow distribution)",
)
@click.option(
    "--release-date",
    help="Release date in YYYY-MM-DD format (required for providers)",
)
@click.option(
    "--packages-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True, path_type=Path),
    help="File containing list of packages to verify (required for providers). "
    "Should contain PyPI URLs from the VOTE email, e.g., "
    "https://pypi.org/project/apache-airflow-providers-google/19.4.0rc2/",
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
    help=(
        "Comma-separated list of checks to run. "
        "Available: svn, reproducible-build, signatures, checksums, licenses. "
        "Defaults to all checks."
    ),
)
@click.option(
    "--download-gpg-keys",
    is_flag=True,
    help="Download and import ASF KEYS for signature verification.",
)
@click.option(
    "--update-svn/--no-update-svn",
    is_flag=True,
    default=True,
    help="Run 'svn update' before verification to fetch the latest release files. Default: True.",
)
@click.option(
    "--docker-install-check",
    is_flag=True,
    help="Verify provider installation by building and running Dockerfile.pmc (providers only)",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Show detailed verification output",
)
def verify_rc_by_pmc(
    distribution: str,
    version: str | None,
    task_sdk_version: str | None,
    release_date: str | None,
    packages_file: Path | None,
    path_to_airflow_svn: Path,
    checks: str | None,
    download_gpg_keys: bool,
    update_svn: bool,
    docker_install_check: bool,
    verbose: bool,
):
    """Verify a release candidate for PMC voting.

    This is intended to automate (not replace) the manual verification steps described in the
    release guides.

    Notes:

        - Experimental: breaking changes may happen without notice. It is recommended to also follow
            the manual verification steps and compare results.
        - Reproducible build verification uses a detached git worktree for the release tag. This avoids
            changing your current checkout and ensures the build runs using the latest Breeze code.

    Practical requirements:

    - Run from the Airflow git repository root (must contain the airflow-core/ directory).
    - Ensure you have a full SVN checkout of the relevant release directory.
    - Some checks may require external tools (e.g. gpg, java for Apache RAT, hatch for builds).

    Examples:

    Verify Airflow + Task SDK RC (run all checks):

                breeze release-management verify-rc-by-pmc \\
                    --distribution airflow \\
                    --version 3.1.3rc1 \\
                    --task-sdk-version 1.1.3rc1 \\
                    --path-to-airflow-svn ~/asf-dist/dev/airflow \\
                    --verbose

    Verify only signatures + checksums:

                breeze release-management verify-rc-by-pmc \\
                    --distribution airflow \\
                    --version 3.1.3rc1 \\
                    --task-sdk-version 1.1.3rc1 \\
                    --path-to-airflow-svn ~/asf-dist/dev/airflow \\
                    --checks signatures,checksums

    Verify a Providers RC:

                breeze release-management verify-rc-by-pmc \\
                    --distribution providers \\
                    --release-date 2026-01-17 \\
                    --packages-file ./dev/packages.txt \\
                    --path-to-airflow-svn ~/asf-dist/dev/airflow \\
                    --verbose
        """
    airflow_repo_root = Path.cwd()

    if not (airflow_repo_root / "airflow-core").exists():
        console_print("[red]Error: Must be run from Airflow repository root[/red]")
        sys.exit(1)

    # Parse checks if provided
    check_list = None
    if checks:
        try:
            check_list = [CheckType(c.strip()) for c in checks.split(",")]
        except ValueError as e:
            console_print(f"[red]Invalid check type: {e}[/red]")
            console_print(f"Available checks: {', '.join([c.value for c in CheckType])}")
            sys.exit(1)

    # Validate required options based on distribution type
    if distribution == "providers":
        if not release_date:
            console_print("[red]Error: --release-date is required for providers verification[/red]")
            sys.exit(1)
        if not packages_file:
            console_print("[red]Error: --packages-file is required for providers verification[/red]")
            console_print(
                "[yellow]Hint: Create a packages.txt file with PyPI URLs from the VOTE email, e.g.:[/yellow]"
            )
            console_print("  https://pypi.org/project/apache-airflow-providers-google/19.4.0rc2/")
            sys.exit(1)

        packages = parse_packages_file(packages_file)
        console_print(f"[info]Loaded {len(packages)} packages from {packages_file}[/info]")
        for name, ver in packages:
            console_print(f"  - {name} {ver}")

        validator = ProvidersReleaseValidator(
            release_date=release_date,
            svn_path=path_to_airflow_svn,
            airflow_repo_root=airflow_repo_root,
            packages=packages,
            download_gpg_keys=download_gpg_keys,
            update_svn=update_svn,
            docker_install_check=docker_install_check,
            verbose=verbose,
        )
    elif distribution == "airflow":
        if not version:
            console_print("[red]Error: --version is required for airflow verification[/red]")
            sys.exit(1)

        validator = AirflowReleaseValidator(
            version=version,
            svn_path=path_to_airflow_svn,
            airflow_repo_root=airflow_repo_root,
            task_sdk_version=task_sdk_version,
            download_gpg_keys=download_gpg_keys,
            update_svn=update_svn,
            verbose=verbose,
        )
    elif distribution == "airflowctl":
        if not version:
            console_print("[red]Error: --version is required for airflowctl verification[/red]")
            sys.exit(1)
        console_print("[yellow]airflowctl verification not yet implemented[/yellow]")
        sys.exit(1)
    elif distribution == "python-client":
        if not version:
            console_print("[red]Error: --version is required for python-client verification[/red]")
            sys.exit(1)
        console_print("[yellow]python-client verification not yet implemented[/yellow]")
        sys.exit(1)
    else:
        console_print(f"[red]Unknown distribution: {distribution}[/red]")
        sys.exit(1)

    if not validator.validate(checks=check_list):
        console_print(f"[red]Verification failed for {distribution}[/red]")
        sys.exit(1)


# Deprecated alias for backwards compatibility
@release_management_group.command(
    name="validate-rc-by-pmc",
    hidden=True,
    help="[DEPRECATED: use verify-rc-by-pmc] Validate a release candidate for PMC voting.",
)
@click.option(
    "--distribution",
    type=click.Choice(["airflow", "airflowctl", "providers", "python-client"]),
    default="airflow",
    help="Distribution type to verify",
)
@click.option("--version", help="Release candidate version")
@click.option("--task-sdk-version", help="Task SDK version")
@click.option("--release-date", help="Release date in YYYY-MM-DD format")
@click.option(
    "--packages-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True, path_type=Path),
    help="File containing list of packages",
)
@click.option(
    "--path-to-airflow-svn",
    "-p",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True, path_type=Path),
    envvar="PATH_TO_AIRFLOW_SVN",
    help="Path to SVN checkout",
)
@click.option("--checks", help="Comma-separated list of checks to run")
@click.option("--download-gpg-keys", is_flag=True, help="Download ASF KEYS")
@click.option("--update-svn/--no-update-svn", is_flag=True, default=True, help="Run 'svn update'")
@click.option("--docker-install-check", is_flag=True, help="Docker install check")
@click.option("--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def validate_rc_by_pmc(ctx: click.Context, **kwargs):
    """Deprecated alias for verify-rc-by-pmc."""
    console_print(
        "[yellow]Warning: 'validate-rc-by-pmc' is deprecated and will be removed in a future release. "
        "Use 'verify-rc-by-pmc' instead.[/yellow]"
    )
    ctx.invoke(verify_rc_by_pmc, **kwargs)
