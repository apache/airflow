#!/usr/bin/env python3
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
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

from airflow_breeze.shell.shell_params import ShellParams

try:
    # We handle ImportError so that click autocomplete works
    import rich_click as click

    click.rich_click.SHOW_METAVARS_COLUMN = False
    click.rich_click.APPEND_METAVARS_HELP = True
    click.rich_click.STYLE_ERRORS_SUGGESTION = "bright_blue italic"
    click.rich_click.ERRORS_SUGGESTION = "\nTry running the '--help' flag for more information.\n"
    click.rich_click.ERRORS_EPILOGUE = (
        "\nTo find out more, visit [bright_blue]https://github.com/apache/airflow/blob/main/BREEZE.rst[/]\n"
    )
    click.rich_click.OPTION_GROUPS = {
        "Breeze2": [
            {
                "name": "Basic flags for the default (shell) command",
                "options": [
                    "--python",
                    "--backend",
                    "--use-airflow-version",
                    "--postgres-version",
                    "--mysql-version",
                    "--mssql-version",
                    "--forward-credentials",
                    "--db-reset",
                ],
            },
            {
                "name": "Advanced flags for the default (shell) command",
                "options": [
                    "--force-build",
                    "--mount-sources",
                    "--integration",
                ],
            },
        ],
        "Breeze2 shell": [
            {
                "name": "Basic flags",
                "options": [
                    "--python",
                    "--backend",
                    "--use-airflow-version",
                    "--postgres-version",
                    "--mysql-version",
                    "--mssql-version",
                    "--forward-credentials",
                    "--db-reset",
                ],
            },
            {
                "name": "Advanced flag for running",
                "options": [
                    "--force-build",
                    "--mount-sources",
                    "--integration",
                ],
            },
        ],
        "Breeze2 start-airflow": [
            {
                "name": "Basic flags",
                "options": [
                    "--python",
                    "--backend",
                    "--use-airflow-version",
                    "--postgres-version",
                    "--mysql-version",
                    "--mssql-version",
                    "--load-example-dags",
                    "--load-default-connections",
                    "--forward-credentials",
                    "--db-reset",
                ],
            },
            {
                "name": "Advanced flag for running",
                "options": [
                    "--force-build",
                    "--mount-sources",
                    "--integration",
                ],
            },
        ],
        "Breeze2 build-image": [
            {
                "name": "Basic usage",
                "options": [
                    "--python",
                    "--upgrade-to-newer-dependencies",
                    "--debian-version",
                    "--image-tag",
                    "--docker-cache",
                    "--github-repository",
                ],
            },
            {
                "name": "Advanced options (for power users)",
                "options": [
                    "--install-providers-from-sources",
                    "--additional-extras",
                    "--additional-dev-apt-deps",
                    "--additional-runtime-apt-deps",
                    "--additional-python-deps",
                    "--additional-dev-apt-command",
                    "--runtime-apt-command",
                    "--additional-dev-apt-env",
                    "--additional-runtime-apt-env",
                    "--additional-runtime-apt-command",
                    "--dev-apt-command",
                    "--dev-apt-deps",
                    "--runtime-apt-deps",
                ],
            },
            {
                "name": "Preparing cache (for maintainers)",
                "options": [
                    "--platform",
                    "--prepare-buildx-cache",
                ],
            },
        ],
        "Breeze2 build-prod-image": [
            {
                "name": "Basic usage",
                "options": [
                    "--python",
                    "--install-airflow-version",
                    "--upgrade-to-newer-dependencies",
                    "--debian-version",
                    "--image-tag",
                    "--docker-cache",
                    "--github-repository",
                ],
            },
            {
                "name": "Options for customizing images",
                "options": [
                    "--install-providers-from-sources",
                    "--extras",
                    "--additional-extras",
                    "--additional-dev-apt-deps",
                    "--additional-runtime-apt-deps",
                    "--additional-python-deps",
                    "--additional-dev-apt-command",
                    "--runtime-apt-command",
                    "--additional-dev-apt-env",
                    "--additional-runtime-apt-env",
                    "--additional-runtime-apt-command",
                    "--dev-apt-command",
                    "--dev-apt-deps",
                    "--runtime-apt-deps",
                ],
            },
            {
                "name": "Customization options (for specific customization needs)",
                "options": [
                    "--install-from-docker-context-files",
                    "--cleanup-docker-context-files",
                    "--disable-mysql-client-installation",
                    "--disable-mssql-client-installation",
                    "--disable-postgres-client-installation",
                    "--disable-airflow-repo-cache",
                    "--disable-pypi",
                    "--install-airflow-reference",
                    "--installation-method",
                ],
            },
            {
                "name": "Preparing cache (for maintainers)",
                "options": [
                    "--platform",
                    "--prepare-buildx-cache",
                ],
            },
        ],
        "Breeze2 static-check": [
            {
                "name": "Pre-commit flags",
                "options": [
                    "--type",
                    "--files",
                    "--all-files",
                    "--show-diff-on-failure",
                    "--last-commit",
                ],
            },
        ],
        "Breeze2 build-docs": [
            {
                "name": "Doc flags",
                "options": [
                    "--docs-only",
                    "--spellcheck-only",
                    "--package-filter",
                ],
            },
        ],
        "Breeze2 stop": [
            {
                "name": "Stop flags",
                "options": [
                    "--preserve-volumes",
                ],
            },
        ],
        "Breeze2 setup-autocomplete": [
            {
                "name": "Setup autocomplete flags",
                "options": [
                    "--force-setup",
                ],
            },
        ],
        "Breeze2 config": [
            {
                "name": "Config flags",
                "options": [
                    "--python",
                    "--backend",
                    "--cheatsheet",
                    "--asciiart",
                ],
            },
        ],
    }

    click.rich_click.COMMAND_GROUPS = {
        "Breeze2": [
            {
                "name": "Developer tools",
                "commands": [
                    "shell",
                    "start-airflow",
                    "stop",
                    "build-image",
                    "build-prod-image",
                    "build-docs",
                    "static-check",
                ],
            },
            {
                "name": "Configuration & maintenance",
                "commands": ["cleanup", "setup-autocomplete", "config", "version"],
            },
        ]
    }


except ImportError:
    import click  # type: ignore[no-redef]

from click import Context

from airflow_breeze.build_image.ci.build_ci_image import build_image
from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.build_image.prod.build_prod_image import build_production_image
from airflow_breeze.global_constants import (
    ALLOWED_BACKENDS,
    ALLOWED_BUILD_CACHE,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_EXECUTORS,
    ALLOWED_INSTALLATION_METHODS,
    ALLOWED_INTEGRATIONS,
    ALLOWED_MOUNT_OPTIONS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_PLATFORMS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    MOUNT_SELECTED,
    get_available_packages,
)
from airflow_breeze.pre_commit_ids import PRE_COMMIT_LIST
from airflow_breeze.shell.enter_shell import enter_shell
from airflow_breeze.utils.cache import delete_cache, touch_cache_file, write_to_cache_file
from airflow_breeze.utils.console import console
from airflow_breeze.utils.docker_command_utils import (
    check_docker_resources,
    construct_env_variables_docker_compose_command,
    get_extra_docker_flags,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    BUILD_CACHE_DIR,
    create_directories,
    find_airflow_sources_root,
)
from airflow_breeze.utils.run_utils import check_pre_commit_installed, run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE

NAME = "Breeze2"
VERSION = "0.0.1"

find_airflow_sources_root()

option_verbose = click.option(
    "-v", "--verbose", is_flag=True, help="Print verbose information about performed steps.", envvar='VERBOSE'
)

option_dry_run = click.option(
    "-D",
    "--dry-run",
    is_flag=True,
    help="If dry-run is set, commands are only printed, not executed.",
    envvar='DRY_RUN',
)


option_python = click.option(
    '-p',
    '--python',
    type=click.Choice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    help='Python version to use.',
    envvar='PYTHON_MAJOR_MINOR_VERSION',
)

option_backend = click.option(
    '-b',
    '--backend',
    help="Database backend to use.",
    type=click.Choice(ALLOWED_BACKENDS),
)

option_integration = click.option(
    '--integration',
    help="Integration(s) to enable when running (can be more than one).",
    type=click.Choice(ALLOWED_INTEGRATIONS),
    multiple=True,
)

option_postgres_version = click.option(
    '-P', '--postgres-version', help="Version of Postgres.", type=click.Choice(ALLOWED_POSTGRES_VERSIONS)
)

option_mysql_version = click.option(
    '-M', '--mysql-version', help="Version of MySQL.", type=click.Choice(ALLOWED_MYSQL_VERSIONS)
)

option_mssql_version = click.option(
    '-S', '--mssql-version', help="Version of MsSQL.", type=click.Choice(ALLOWED_MSSQL_VERSIONS)
)

option_executor = click.option(
    '--executor',
    help='Executor to use in a kubernetes cluster. Default is KubernetesExecutor.',
    type=click.Choice(ALLOWED_EXECUTORS),
)

option_forward_credentials = click.option(
    '-f', '--forward-credentials', help="Forward local credentials to container when running.", is_flag=True
)

option_use_airflow_version = click.option(
    '-a',
    '--use-airflow-version',
    help="Use (reinstall) specified Airflow version after entering the container.",
    envvar='USE_AIRFLOW_VERSION',
)

option_mount_sources = click.option(
    '--mount-sources',
    type=click.Choice(ALLOWED_MOUNT_OPTIONS),
    default=ALLOWED_MOUNT_OPTIONS[0],
    help="Choose which local sources should be mounted (default = selected)",
)

option_force_build = click.option('--force-build', help="Force image build before running.", is_flag=True)

option_db_reset = click.option(
    '-d',
    '--db-reset',
    help="Resets DB when entering the container.",
    is_flag=True,
    envvar='DB_RESET',
)


@click.group(invoke_without_command=True, context_settings={'help_option_names': ['-h', '--help']})
@option_verbose
@option_dry_run
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_mount_sources
@option_integration
@option_db_reset
@click.pass_context
def main(ctx: Context, **kwargs):
    if not ctx.invoked_subcommand:
        ctx.forward(shell, extra_args={})


option_docker_cache = click.option(
    '-c',
    '--docker-cache',
    help='Cache option for image used during the build.',
    type=click.Choice(ALLOWED_BUILD_CACHE),
)

option_github_repository = click.option(
    '-g',
    '--github-repository',
    help='GitHub repository used to pull, push images. Default: apache/airflow.',
    envvar='GITHUB_REPOSITORY',
)

option_github_image_id = click.option(
    '-s',
    '--github-image-id',
    help='Commit SHA of the image. \
    Breeze can automatically pull the commit SHA id specified Default: latest',
)

option_image_tag = click.option(
    '-t', '--image-tag', help='Set tag for the image (additionally to default Airflow convention).'
)

option_platform = click.option(
    '--platform',
    help='Builds image for the platform specified.',
    envvar='PLATFORM',
    type=click.Choice(ALLOWED_PLATFORMS),
)

option_debian_version = click.option(
    '-d',
    '--debian-version',
    help='Debian version used for the image.',
    type=click.Choice(ALLOWED_DEBIAN_VERSIONS),
    envvar='DEBIAN_VERSION',
)
option_upgrade_to_newer_dependencies = click.option(
    "-u",
    '--upgrade-to-newer-dependencies',
    help='If set to anything else than false, upgrades PIP packages to latest versions available.',
    envvar='UPGRADE_TO_NEWER_DEPENDENCIES',
)
option_additional_extras = click.option(
    '--additional-extras',
    help='This installs additional extra package while installing airflow in the image.',
    envvar='ADDITIONAL_AIRFLOW_EXTRAS',
)
option_additional_dev_apt_deps = click.option(
    '--additional-dev-apt-deps',
    help='Additional apt dev dependencies to use when building the images.',
    envvar='ADDITIONAL_DEV_APT_DEPS',
)
option_additional_runtime_apt_deps = click.option(
    '--additional-runtime-apt-deps',
    help='Additional apt runtime dependencies to use when building the images.',
    envvar='ADDITIONAL_RUNTIME_APT_DEPS',
)
option_additional_python_deps = click.option(
    '--additional-python-deps',
    help='Additional python dependencies to use when building the images.',
    envvar='ADDITIONAL_PYTHON_DEPS',
)
option_additional_dev_apt_command = click.option(
    '--additional-dev-apt-command',
    help='Additional command executed before dev apt deps are installed.',
    envvar='ADDITIONAL_DEV_APT_COMMAND',
)
option_additional_runtime_apt_command = click.option(
    '--additional-runtime-apt-command',
    help='Additional command executed before runtime apt deps are installed.',
    envvar='ADDITIONAL_RUNTIME_APT_COMMAND',
)
option_additional_dev_apt_env = click.option(
    '--additional-dev-apt-env',
    help='Additional environment variables set when adding dev dependencies.',
    envvar='ADDITIONAL_DEV_APT_ENV',
)
option_additional_runtime_apt_env = click.option(
    '--additional-runtime-apt-env',
    help='Additional environment variables set when adding runtime dependencies.',
    envvar='ADDITIONAL_RUNTIME_APT_ENV',
)
option_dev_apt_command = click.option(
    '--dev-apt-command',
    help='The basic command executed before dev apt deps are installed.',
    envvar='DEV_APT_COMMAND',
)
option_dev_apt_deps = click.option(
    '--dev-apt-deps',
    help='The basic apt dev dependencies to use when building the images.',
    envvar='DEV_APT_DEPS',
)
option_runtime_apt_command = click.option(
    '--runtime-apt-command',
    help='The basic command executed before runtime apt deps are installed.',
    envvar='RUNTIME_APT_COMMAND',
)
option_runtime_apt_deps = click.option(
    '--runtime-apt-deps',
    help='The basic apt runtime dependencies to use when building the images.',
    envvar='RUNTIME_APT_DEPS',
)

option_skip_rebuild_check = click.option(
    '-r',
    '--skip-rebuild-check',
    help="Skips checking if rebuild is needed",
    is_flag=True,
    envvar='SKIP_REBUILD_CHECK',
)

option_prepare_buildx_cache = click.option(
    '--prepare-buildx-cache',
    help='Prepares build cache rather than build images locally.',
    is_flag=True,
    envvar='PREPARE_BUILDX_CACHE',
)

option_install_providers_from_sources = click.option(
    '--install-providers-from-sources',
    help="Install providers from sources when installing.",
    is_flag=True,
    envvar='INSTALL_PROVIDERS_FROM_SOURCES',
)

option_load_example_dags = click.option(
    '-e',
    '--load-example-dags',
    help="Enable configuration to load example DAGs when starting Airflow.",
    is_flag=True,
    envvar='LOAD_EXAMPLES',
)

option_load_default_connection = click.option(
    '-c',
    '--load-default-connections',
    help="Enable configuration to load default connections when starting Airflow.",
    is_flag=True,
    envvar='LOAD_DEFAULT_CONNECTIONS',
)


@main.command()
def version():
    """Prints version of breeze.py."""
    console.print(ASCIIART, style=ASCIIART_STYLE)
    console.print(f"\n[green]{NAME} version: {VERSION}[/]\n")


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Make sure that whatever you add here as an option is also
# Added in the "main" command above. The min command above
# Is used for a shorthand of shell and except the extra
# Args it should have the same parameters.
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
@main.command()
@option_verbose
@option_dry_run
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_mount_sources
@option_integration
@option_db_reset
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def shell(
    verbose: bool,
    dry_run: bool,
    python: str,
    backend: str,
    integration: Tuple[str],
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    forward_credentials: bool,
    mount_sources: str,
    use_airflow_version: str,
    force_build: bool,
    db_reset: bool,
    extra_args: Tuple,
):
    """Enters breeze.py environment. this is the default command use when no other is selected."""

    if verbose:
        console.print("\n[green]Welcome to breeze.py[/]\n")
        console.print(f"\n[green]Root of Airflow Sources = {AIRFLOW_SOURCES_ROOT}[/]\n")
    enter_shell(
        verbose=verbose,
        dry_run=dry_run,
        python=python,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        forward_credentials=str(forward_credentials),
        mount_sources=mount_sources,
        use_airflow_version=use_airflow_version,
        force_build=force_build,
        db_reset=db_reset,
        extra_args=extra_args,
    )


@option_verbose
@main.command(name='start-airflow')
@option_dry_run
@option_python
@option_backend
@option_postgres_version
@option_load_example_dags
@option_load_default_connection
@option_mysql_version
@option_mssql_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_mount_sources
@option_integration
@option_db_reset
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def start_airflow(
    verbose: bool,
    dry_run: bool,
    python: str,
    backend: str,
    integration: Tuple[str],
    postgres_version: str,
    load_example_dags: bool,
    load_default_connections: bool,
    mysql_version: str,
    mssql_version: str,
    forward_credentials: bool,
    mount_sources: str,
    use_airflow_version: str,
    force_build: bool,
    db_reset: bool,
    extra_args: Tuple,
):
    """Enters breeze.py environment and starts all Airflow components in the tmux session."""
    enter_shell(
        verbose=verbose,
        dry_run=dry_run,
        python=python,
        backend=backend,
        integration=integration,
        postgres_version=postgres_version,
        load_default_connections=load_default_connections,
        load_example_dags=load_example_dags,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        forward_credentials=str(forward_credentials),
        mount_sources=mount_sources,
        use_airflow_version=use_airflow_version,
        force_build=force_build,
        db_reset=db_reset,
        start_airflow=True,
        extra_args=extra_args,
    )


@main.command(name='build-image')
@option_verbose
@option_dry_run
@option_python
@option_upgrade_to_newer_dependencies
@option_platform
@option_debian_version
@option_github_repository
@option_docker_cache
@option_image_tag
@option_prepare_buildx_cache
@option_install_providers_from_sources
@option_additional_extras
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_runtime_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_dev_apt_command
@option_dev_apt_deps
@option_runtime_apt_command
@option_runtime_apt_deps
def build_ci_image(
    verbose: bool,
    dry_run: bool,
    additional_extras: Optional[str],
    python: str,
    image_tag: Optional[str],
    additional_dev_apt_deps: Optional[str],
    additional_runtime_apt_deps: Optional[str],
    additional_python_deps: Optional[str],
    additional_dev_apt_command: Optional[str],
    additional_runtime_apt_command: Optional[str],
    additional_dev_apt_env: Optional[str],
    additional_runtime_apt_env: Optional[str],
    dev_apt_command: Optional[str],
    dev_apt_deps: Optional[str],
    install_providers_from_sources: bool,
    runtime_apt_command: Optional[str],
    runtime_apt_deps: Optional[str],
    github_repository: Optional[str],
    docker_cache: Optional[str],
    platform: Optional[str],
    debian_version: Optional[str],
    prepare_buildx_cache: bool,
    upgrade_to_newer_dependencies: str = "false",
):
    """Builds docker CI image."""

    if verbose:
        console.print(
            f"\n[bright_blue]Building image of airflow from {AIRFLOW_SOURCES_ROOT} "
            f"python version: {python}[/]\n"
        )
    build_image(
        verbose=verbose,
        dry_run=dry_run,
        additional_extras=additional_extras,
        python=python,
        image_tag=image_tag,
        additional_dev_apt_deps=additional_dev_apt_deps,
        additional_runtime_apt_deps=additional_runtime_apt_deps,
        additional_python_deps=additional_python_deps,
        additional_runtime_apt_command=additional_runtime_apt_command,
        additional_dev_apt_command=additional_dev_apt_command,
        additional_dev_apt_env=additional_dev_apt_env,
        additional_runtime_apt_env=additional_runtime_apt_env,
        install_providers_from_sources=install_providers_from_sources,
        dev_apt_command=dev_apt_command,
        dev_apt_deps=dev_apt_deps,
        runtime_apt_command=runtime_apt_command,
        runtime_apt_deps=runtime_apt_deps,
        github_repository=github_repository,
        docker_cache=docker_cache,
        platform=platform,
        debian_version=debian_version,
        prepare_buildx_cache=prepare_buildx_cache,
        upgrade_to_newer_dependencies=upgrade_to_newer_dependencies,
    )


@option_verbose
@option_dry_run
@main.command(name='build-prod-image')
@option_python
@option_upgrade_to_newer_dependencies
@option_platform
@option_debian_version
@option_github_repository
@option_docker_cache
@option_image_tag
@option_prepare_buildx_cache
@click.option(
    '--installation-method',
    help="Whether to install airflow from sources ('.') or PyPI ('apache-airflow')",
    type=click.Choice(ALLOWED_INSTALLATION_METHODS),
)
@option_install_providers_from_sources
@click.option(
    '--install-from-docker-context-files',
    help='Install wheels from local docker-context-files when building image',
    is_flag=True,
)
@click.option(
    '--cleanup-docker-context-files',
    help='Cleans up docker context files before running build.',
    is_flag=True,
)
@click.option('--extras', help="Extras to install by default")
@click.option('--disable-mysql-client-installation', help="Do not install MySQL client", is_flag=True)
@click.option('--disable-mssql-client-installation', help="Do not install MsSQl client", is_flag=True)
@click.option('--disable-postgres-client-installation', help="Do not install Postgres client", is_flag=True)
@click.option(
    '--disable-airflow-repo-cache', help="Disable cache from Airflow repository during building", is_flag=True
)
@click.option('--disable-pypi', help="Disable pypi during building", is_flag=True)
@click.option(
    '--install-airflow-reference',
    help="Install airflow using specified reference (tag/branch) from GitHub",
)
@click.option('-a', '--install-airflow-version', help="Install specified version of airflow")
@option_additional_extras
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_dev_apt_command
@option_dev_apt_deps
@option_runtime_apt_command
@option_runtime_apt_deps
def build_prod_image(
    verbose: bool,
    dry_run: bool,
    cleanup_docker_context_files: bool,
    disable_mysql_client_installation: bool,
    disable_mssql_client_installation: bool,
    disable_postgres_client_installation: bool,
    disable_airflow_repo_cache: bool,
    disable_pypi: bool,
    install_airflow_reference: Optional[str],
    install_airflow_version: Optional[str],
    docker_cache: str,
    additional_extras: Optional[str],
    python: str,
    image_tag: Optional[str],
    additional_dev_apt_deps: Optional[str],
    additional_runtime_apt_deps: Optional[str],
    additional_python_deps: Optional[str],
    additional_dev_apt_command: Optional[str],
    additional_runtime_apt_command: Optional[str],
    additional_dev_apt_env: Optional[str],
    additional_runtime_apt_env: Optional[str],
    dev_apt_command: Optional[str],
    dev_apt_deps: Optional[str],
    runtime_apt_command: Optional[str],
    runtime_apt_deps: Optional[str],
    github_repository: Optional[str],
    platform: Optional[str],
    debian_version: Optional[str],
    prepare_buildx_cache: bool,
    install_providers_from_sources: bool,
    extras: Optional[str],
    installation_method: Optional[str],
    install_from_docker_context_files: bool,
    upgrade_to_newer_dependencies: str = "false",
):
    """Builds docker Production image."""
    if verbose:
        console.print("\n[bright_blue]Building image[/]\n")
    if prepare_buildx_cache:
        docker_cache = "pulled"
        cleanup_docker_context_files = True
    build_production_image(
        verbose,
        dry_run,
        cleanup_docker_context_files=cleanup_docker_context_files,
        disable_mysql_client_installation=disable_mysql_client_installation,
        disable_mssql_client_installation=disable_mssql_client_installation,
        disable_postgres_client_installation=disable_postgres_client_installation,
        disable_airflow_repo_cache=disable_airflow_repo_cache,
        disable_pypi=disable_pypi,
        install_airflow_reference=install_airflow_reference,
        install_airflow_version=install_airflow_version,
        docker_cache=docker_cache,
        additional_extras=additional_extras,
        python=python,
        additional_dev_apt_deps=additional_dev_apt_deps,
        additional_runtime_apt_deps=additional_runtime_apt_deps,
        additional_python_deps=additional_python_deps,
        additional_runtime_apt_command=additional_runtime_apt_command,
        additional_dev_apt_command=additional_dev_apt_command,
        additional_dev_apt_env=additional_dev_apt_env,
        additional_runtime_apt_env=additional_runtime_apt_env,
        dev_apt_command=dev_apt_command,
        dev_apt_deps=dev_apt_deps,
        runtime_apt_command=runtime_apt_command,
        runtime_apt_deps=runtime_apt_deps,
        github_repository=github_repository,
        platform=platform,
        debian_version=debian_version,
        upgrade_to_newer_dependencies=upgrade_to_newer_dependencies,
        prepare_buildx_cache=prepare_buildx_cache,
        install_providers_from_sources=install_providers_from_sources,
        extras=extras,
        installation_method=installation_method,
        install_docker_context_files=install_from_docker_context_files,
        image_tag=image_tag,
    )


BREEZE_COMMENT = "Added by Updated Airflow Breeze autocomplete setup"
START_LINE = f"# START: {BREEZE_COMMENT}\n"
END_LINE = f"# END: {BREEZE_COMMENT}\n"


def remove_autogenerated_code(script_path: str):
    lines = Path(script_path).read_text().splitlines(keepends=True)
    new_lines = []
    pass_through = True
    for line in lines:
        if line == START_LINE:
            pass_through = False
            continue
        if line.startswith(END_LINE):
            pass_through = True
            continue
        if pass_through:
            new_lines.append(line)
    Path(script_path).write_text("".join(new_lines))


def backup(script_path_file: Path):
    shutil.copy(str(script_path_file), str(script_path_file) + ".bak")


def write_to_shell(command_to_execute: str, dry_run: bool, script_path: str, force_setup: bool) -> bool:
    skip_check = False
    script_path_file = Path(script_path)
    if not script_path_file.exists():
        skip_check = True
    if not skip_check:
        if BREEZE_COMMENT in script_path_file.read_text():
            if not force_setup:
                console.print(
                    "\n[bright_yellow]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force-setup[/]\n"
                )
                return False
            else:
                backup(script_path_file)
                remove_autogenerated_code(script_path)
    text = ''
    if script_path_file.exists():
        console.print(f"\nModifying the {script_path} file!\n")
        console.print(f"\nCopy of the original file is held in {script_path}.bak !\n")
        if not dry_run:
            backup(script_path_file)
            text = script_path_file.read_text()
    else:
        console.print(f"\nCreating the {script_path} file!\n")
    if not dry_run:
        script_path_file.write_text(
            text
            + ("\n" if not text.endswith("\n") else "")
            + START_LINE
            + command_to_execute
            + "\n"
            + END_LINE
        )
    else:
        console.print(f"[bright_blue]The autocomplete script would be added to {script_path}[/]")
    console.print(
        f"\n[bright_yellow]IMPORTANT!!!! Please exit and re-enter your shell or run:[/]"
        f"\n\n   `source {script_path}`\n"
    )
    return True


@option_verbose
@option_dry_run
@click.option(
    '-f',
    '--force-setup',
    is_flag=True,
    help='Force autocomplete setup even' 'if already setup before (overrides the setup).',
)
@main.command(name='setup-autocomplete')
def setup_autocomplete(verbose: bool, dry_run: bool, force_setup: bool):
    """
    Enables autocompletion of Breeze2 commands.
    """

    # Determine if the shell is bash/zsh/powershell. It helps to build the autocomplete path
    detected_shell = os.environ.get('SHELL')
    detected_shell = None if detected_shell is None else detected_shell.split(os.sep)[-1]
    if detected_shell not in ['bash', 'zsh', 'fish']:
        console.print(f"\n[red] The shell {detected_shell} is not supported for autocomplete![/]\n")
        sys.exit(1)
    console.print(f"Installing {detected_shell} completion for local user")
    autocomplete_path = (
        AIRFLOW_SOURCES_ROOT / "dev" / "breeze" / "autocomplete" / f"{NAME}-complete-{detected_shell}.sh"
    )
    console.print(f"[bright_blue]Activation command script is available here: {autocomplete_path}[/]\n")
    console.print(
        f"[bright_yellow]We need to add above script to your {detected_shell} profile and "
        "install 'click' package in your default python installation destination.[/]\n"
    )
    if click.confirm("Should we proceed ?"):
        run_command(['pip', 'install', '--upgrade', 'click'], verbose=True, dry_run=dry_run, check=False)
        if detected_shell == 'bash':
            script_path = str(Path('~').expanduser() / '.bash_completion')
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force_setup)
        elif detected_shell == 'zsh':
            script_path = str(Path('~').expanduser() / '.zshrc')
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force_setup)
        elif detected_shell == 'fish':
            # Include steps for fish shell
            script_path = str(Path('~').expanduser() / f'.config/fish/completions/{NAME}.fish')
            if os.path.exists(script_path) and not force_setup:
                console.print(
                    "\n[bright_yellow]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force-setup[/]\n"
                )
            else:
                with open(autocomplete_path) as source_file, open(script_path, 'w') as destination_file:
                    for line in source_file:
                        destination_file.write(line)
        else:
            # Include steps for powershell
            subprocess.check_call(['powershell', 'Set-ExecutionPolicy Unrestricted -Scope CurrentUser'])
            script_path = (
                subprocess.check_output(['powershell', '-NoProfile', 'echo $profile']).decode("utf-8").strip()
            )
            command_to_execute = f". {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force_setup)
    else:
        console.print(
            "\nPlease follow the https://click.palletsprojects.com/en/8.1.x/shell-completion/ "
            "to setup autocompletion for breeze manually if you want to use it.\n"
        )


@main.command(name='config')
@option_python
@option_backend
@click.option('-C/-c', '--cheatsheet/--no-cheatsheet', help="Enable/disable cheatsheet", default=None)
@click.option('-A/-a', '--asciiart/--no-asciiart', help="Enable/disable ASCIIart", default=None)
def change_config(python, backend, cheatsheet, asciiart):
    """
    Toggles on/off cheatsheet, asciiart. Sets default Python and backend.
    """
    if asciiart:
        console.print('[bright_blue] ASCIIART enabled')
        delete_cache('suppress_asciiart')
    elif asciiart is not None:
        touch_cache_file('suppress_asciiart')
    else:
        pass
    if cheatsheet:
        console.print('[bright_blue] Cheatsheet enabled')
        delete_cache('suppress_cheatsheet')
    elif cheatsheet is not None:
        touch_cache_file('suppress_cheatsheet')
    else:
        pass
    if python is not None:
        write_to_cache_file('PYTHON_MAJOR_MINOR_VERSION', python)
        console.print(f'[bright_blue]Python cached_value {python}')
    if backend is not None:
        write_to_cache_file('BACKEND', backend)
        console.print(f'[bright_blue]Backend cached_value {backend}')


@dataclass
class DocParams:
    package_filter: Tuple[str]
    docs_only: bool
    spellcheck_only: bool

    @property
    def args_doc_builder(self) -> List[str]:
        doc_args = []
        if self.docs_only:
            doc_args.append("--docs-only")
        if self.spellcheck_only:
            doc_args.append("--spellcheck-only")
        if self.package_filter and len(self.package_filter) > 0:
            for single_filter in self.package_filter:
                doc_args.extend(["--package-filter", single_filter])
        return doc_args


@main.command(name='build-docs')
@option_verbose
@option_dry_run
@click.option('-d', '--docs-only', help="Only build documentation", is_flag=True)
@click.option('-s', '--spellcheck-only', help="Only run spell checking", is_flag=True)
@click.option(
    '-p',
    '--package-filter',
    help="List of packages to consider",
    type=click.Choice(get_available_packages()),
    multiple=True,
)
def build_docs(
    verbose: bool, dry_run: bool, docs_only: bool, spellcheck_only: bool, package_filter: Tuple[str]
):
    """
    Builds documentation in the container.

    * figures out CI image name
    * checks if there are enough resources
    * converts parameters into a DocParams class
    """
    params = BuildCiParams()
    ci_image_name = params.airflow_image_name
    check_docker_resources(verbose, ci_image_name)
    doc_builder = DocParams(
        package_filter=package_filter,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
    )
    extra_docker_flags = get_extra_docker_flags(MOUNT_SELECTED)
    cmd = []
    cmd.extend(["docker", "run"])
    cmd.extend(extra_docker_flags)
    cmd.extend(["-t", "-e", "GITHUB_ACTIONS="])
    cmd.extend(["--entrypoint", "/usr/local/bin/dumb-init", "--pull", "never"])
    cmd.extend([ci_image_name, "--", "/opt/airflow/scripts/in_container/run_docs_build.sh"])
    cmd.extend(doc_builder.args_doc_builder)
    run_command(cmd, verbose=verbose, dry_run=dry_run, text=True)


@main.command(
    name="static-check",
    help="Run static checks.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    '-t',
    '--type',
    help="Type(s) of the static checks to run",
    type=click.Choice(PRE_COMMIT_LIST),
    multiple=True,
)
@click.option('-a', '--all-files', help="Run checks on all files", is_flag=True)
@click.option('-f', '--files', help="List of files to run the checks on", multiple=True)
@click.option('-s', '--show-diff-on-failure', help="Show diff for files modified by the checks", is_flag=True)
@click.option('-c', '--last-commit', help="Run check for all files in all commits", is_flag=True)
@option_verbose
@option_dry_run
@click.argument('precommit_args', nargs=-1, type=click.UNPROCESSED)
def static_check(
    verbose: bool,
    dry_run: bool,
    all_files: bool,
    show_diff_on_failure: bool,
    last_commit: bool,
    type: Tuple[str],
    files: bool,
    precommit_args: Tuple,
):
    if check_pre_commit_installed(verbose=verbose):
        command_to_execute = ['pre-commit', 'run']
        for single_check in type:
            command_to_execute.append(single_check)
        if all_files:
            command_to_execute.append("--all-files")
        if show_diff_on_failure:
            command_to_execute.append("--show-diff-on-failure")
        if last_commit:
            command_to_execute.extend(["--from-ref", "HEAD^", "--to-ref", "HEAD"])
        if files:
            command_to_execute.append("--files")
        if verbose:
            command_to_execute.append("--verbose")
        if precommit_args:
            command_to_execute.extend(precommit_args)
        run_command(
            command_to_execute,
            verbose=verbose,
            dry_run=dry_run,
            check=False,
            no_output_dump_on_exception=True,
            text=True,
        )


@main.command(name="stop", help="Stops running breeze environment.")
@option_verbose
@option_dry_run
@click.option(
    "-p",
    "--preserve-volumes",
    help="By default the stop command removes volumes with data. " "Specifying the flag will preserve them.",
    is_flag=True,
)
def stop(verbose: bool, dry_run: bool, preserve_volumes: bool):
    command_to_execute = ['docker-compose', 'down', "--remove-orphans"]
    if not preserve_volumes:
        command_to_execute.append("--volumes")
    shell_params = ShellParams({})
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    run_command(command_to_execute, verbose=verbose, dry_run=dry_run, env=env_variables)


@main.command(name="cleanup", help="Removes the cache of parameters, images and cleans up docker cache.")
@option_verbose
@option_dry_run
def cleanup(verbose: bool, dry_run: bool):
    console.print("\n[bright_yellow]Removing cache of parameters, images, and cleans up docker cache[/]")
    if click.confirm("Are you sure?"):
        docker_images_command_to_execute = [
            'docker',
            'images',
            '--filter',
            'label=org.apache.airflow.image',
            '--format',
            '{{.Repository}}:{{.Tag}}',
        ]
        process = run_command(
            docker_images_command_to_execute, verbose=verbose, text=True, capture_output=True
        )
        images = process.stdout.splitlines() if process and process.stdout else []
        if images:
            console.print("[light_blue]Removing images:[/]")
            for image in images:
                console.print(f"[light_blue] * {image}[/]")
            console.print()
            docker_rmi_command_to_execute = [
                'docker',
                'rmi',
                '--force',
            ]
            docker_rmi_command_to_execute.extend(images)
            run_command(docker_rmi_command_to_execute, verbose=verbose, dry_run=dry_run, check=False)
        else:
            console.print("[light_blue]No images to remote[/]\n")
        system_prune_command_to_execute = ['docker', 'system', 'prune']
        console.print("Pruning docker images")
        run_command(system_prune_command_to_execute, verbose=verbose, dry_run=dry_run, check=False)
        console.print(f"Removing build cache dir ${BUILD_CACHE_DIR}")
        if not dry_run:
            shutil.rmtree(BUILD_CACHE_DIR, ignore_errors=True)


if __name__ == '__main__':
    create_directories()
    main()
