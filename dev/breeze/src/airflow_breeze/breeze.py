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
import atexit
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import IO, List, Optional, Tuple

import rich

from airflow_breeze import NAME, VERSION
from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.confirm import Answer, set_forced_answer, user_confirm
from airflow_breeze.utils.constraints import run_generate_constraints, run_generate_constraints_in_parallel
from airflow_breeze.utils.pulll_image import run_pull_image, run_pull_in_parallel
from airflow_breeze.utils.reinstall import ask_to_reinstall_breeze, reinstall_breeze, warn_non_editable
from airflow_breeze.utils.run_tests import run_docker_compose_tests, verify_an_image

try:
    # We handle ImportError so that click autocomplete works
    import rich_click as click

    try:
        click.formatting.FORCED_WIDTH = os.get_terminal_size().columns - 2
    except OSError:
        pass
    click.rich_click.SHOW_METAVARS_COLUMN = False
    click.rich_click.SHOW_ARGUMENTS = False
    click.rich_click.APPEND_METAVARS_HELP = True
    click.rich_click.STYLE_ERRORS_SUGGESTION = "bright_blue italic"
    click.rich_click.ERRORS_SUGGESTION = "\nTry running the '--help' flag for more information.\n"
    click.rich_click.ERRORS_EPILOGUE = (
        "\nTo find out more, visit [bright_blue]https://github.com/apache/airflow/blob/main/BREEZE.rst[/]\n"
    )
    click.rich_click.OPTION_GROUPS = {
        "breeze": [
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
        "breeze shell": [
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
        "breeze start-airflow": [
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
        "breeze build-image": [
            {
                "name": "Basic usage",
                "options": [
                    "--python",
                    "--upgrade-to-newer-dependencies",
                    "--debian-version",
                    "--image-tag",
                    "--docker-cache",
                    "--force-build",
                ],
            },
            {
                "name": "Building multiple images",
                "options": [
                    "--build_multiple_images",
                    "--python-versions",
                ],
            },
            {
                "name": "Advanced options (for power users)",
                "options": [
                    "--install-providers-from-sources",
                    "--additional-python-deps",
                    "--runtime-apt-deps",
                    "--runtime-apt-command",
                    "--additional-extras",
                    "--additional-runtime-apt-deps",
                    "--additional-runtime-apt-env",
                    "--additional-runtime-apt-command",
                    "--additional-dev-apt-deps",
                    "--additional-dev-apt-env",
                    "--additional-dev-apt-command",
                    "--dev-apt-deps",
                    "--dev-apt-command",
                ],
            },
            {
                "name": "Preparing cache and push (for maintainers and CI)",
                "options": [
                    "--platform",
                    "--prepare-buildx-cache",
                    "--push-image",
                    "--empty-image",
                    "--github-token",
                    "--github-username",
                    "--login-to-github-registry",
                ],
            },
        ],
        "breeze pull-image": [
            {
                "name": "Pull image flags",
                "options": [
                    "--image-tag",
                    "--python",
                    "--verify-image",
                    "--wait-for-image",
                    "--tag-as-latest",
                ],
            },
            {
                "name": "Parallel running",
                "options": [
                    "--run-in-parallel",
                    "--parallelism",
                    "--python-versions",
                ],
            },
        ],
        "breeze verify-image": [
            {
                "name": "Verify image flags",
                "options": [
                    "--image-name",
                    "--python",
                    "--image-tag",
                ],
            }
        ],
        "breeze build-prod-image": [
            {
                "name": "Basic usage",
                "options": [
                    "--python",
                    "--install-airflow-version",
                    "--upgrade-to-newer-dependencies",
                    "--debian-version",
                    "--image-tag",
                    "--docker-cache",
                ],
            },
            {
                "name": "Building multiple images",
                "options": [
                    "--build_multiple_images",
                    "--python-versions",
                ],
            },
            {
                "name": "Options for customizing images",
                "options": [
                    "--install-providers-from-sources",
                    "--additional-python-deps",
                    "--additional-extras",
                    "--additional-runtime-apt-deps",
                    "--additional-runtime-apt-env",
                    "--additional-runtime-apt-command",
                    "--additional-dev-apt-deps",
                    "--additional-dev-apt-env",
                    "--additional-dev-apt-command",
                    "--extras",
                    "--runtime-apt-deps",
                    "--runtime-apt-command",
                    "--dev-apt-deps",
                    "--dev-apt-command",
                ],
            },
            {
                "name": "Customization options (for specific customization needs)",
                "options": [
                    "--cleanup-docker-context-files",
                    "--install-from-docker-context-files",
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
                "name": "Preparing cache and push (for maintainers and CI)",
                "options": [
                    "--github-token",
                    "--github-username",
                    "--login-to-github-registry",
                    "--push-image",
                    "--prepare-buildx-cache",
                    "--platform",
                    "--empty-image",
                ],
            },
        ],
        "breeze pull-prod-image": [
            {
                "name": "Pull image flags",
                "options": [
                    "--image-tag",
                    "--python",
                    "--verify-image",
                    "--wait-for-image",
                    "--tag-as-latest",
                ],
            },
            {
                "name": "Parallel running",
                "options": [
                    "--run-in-parallel",
                    "--parallelism",
                    "--python-versions",
                ],
            },
        ],
        "breeze verify-prod-image": [
            {
                "name": "Verify image flags",
                "options": [
                    "--image-name",
                    "--python",
                    "--image-tag",
                ],
            }
        ],
        "breeze docker-compose-tests": [
            {
                "name": "Docker-compose tests flag",
                "options": [
                    "--image-name",
                    "--python",
                    "--image-tag",
                ],
            }
        ],
        "breeze static-checks": [
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
        "breeze build-docs": [
            {
                "name": "Doc flags",
                "options": [
                    "--docs-only",
                    "--spellcheck-only",
                    "--package-filter",
                ],
            },
        ],
        "breeze stop": [
            {
                "name": "Stop flags",
                "options": [
                    "--preserve-volumes",
                ],
            },
        ],
        "breeze cleanup": [
            {
                "name": "Cleanup flags",
                "options": [
                    "--all",
                ],
            },
        ],
        "breeze setup-autocomplete": [
            {
                "name": "Setup autocomplete flags",
                "options": [
                    "--force",
                ],
            },
        ],
        "breeze config": [
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
        "breeze prepare-airflow-package": [
            {"name": "Package flags", "options": ["--package-format", "--version-suffix-for-pypi"]}
        ],
        "breeze prepare-provider-packages": [
            {
                "name": "Package flags",
                "options": [
                    "--package-format",
                    "--version-suffix-for-pypi",
                    "--package-list-file",
                ],
            }
        ],
        "breeze prepare-provider-documentation": [
            {"name": "Provider documentation preparation flags", "options": ["--skip-package-verification"]}
        ],
        "breeze generate-constraints": [
            {
                "name": "Generate constraints flags",
                "options": [
                    "--image-tag",
                    "--python",
                    "--generate-constraints-mode",
                ],
            },
            {
                "name": "Parallel running",
                "options": [
                    "--run-in-parallel",
                    "--parallelism",
                    "--python-versions",
                ],
            },
        ],
        "breeze self-upgrade": [
            {
                "name": "Self-upgrade flags",
                "options": [
                    "--use-current-airflow-sources",
                    "--force",
                ],
            }
        ],
    }
    click.rich_click.COMMAND_GROUPS = {
        "breeze": [
            {
                "name": "Developer tools",
                "commands": [
                    "shell",
                    "start-airflow",
                    "stop",
                    "build-docs",
                    "static-checks",
                ],
            },
            {
                "name": "Testing",
                "commands": [
                    "docker-compose-tests",
                ],
            },
            {
                "name": "Configuration & maintenance",
                "commands": ["cleanup", "self-upgrade", "setup-autocomplete", "config", "version"],
            },
            {
                "name": "CI Image tools",
                "commands": [
                    "build-image",
                    "pull-image",
                    "verify-image",
                ],
            },
            {
                "name": "Production Image tools",
                "commands": [
                    "build-prod-image",
                    "pull-prod-image",
                    "verify-prod-image",
                ],
            },
            {
                "name": "Release management",
                "commands": [
                    "prepare-provider-documentation",
                    "prepare-provider-packages",
                    "prepare-airflow-package",
                    "generate-constraints",
                ],
            },
        ]
    }


except ImportError:
    import click  # type: ignore[no-redef]

from click import Context, IntRange

from airflow_breeze.build_image.ci.build_ci_image import build_ci_image, get_ci_image_build_params
from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.build_image.prod.build_prod_image import (
    build_production_image,
    get_prod_image_build_params,
)
from airflow_breeze.global_constants import (
    ALLOWED_BACKENDS,
    ALLOWED_BUILD_CACHE,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_EXECUTORS,
    ALLOWED_GENERATE_CONSTRAINTS_MODES,
    ALLOWED_INSTALLATION_METHODS,
    ALLOWED_INTEGRATIONS,
    ALLOWED_MOUNT_OPTIONS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_PACKAGE_FORMATS,
    ALLOWED_PLATFORMS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_EXTRAS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
    MOUNT_SELECTED,
    get_available_packages,
)
from airflow_breeze.pre_commit_ids import PRE_COMMIT_LIST
from airflow_breeze.shell.enter_shell import enter_shell
from airflow_breeze.utils.cache import (
    check_if_cache_exists,
    delete_cache,
    read_from_cache_file,
    synchronize_parameters_with_cache,
    touch_cache_file,
    write_to_cache_file,
)
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
    find_airflow_sources_root_to_operate_on,
    get_installation_airflow_sources,
    get_installation_sources_config_metadata_hash,
    get_package_setup_metadata_hash,
    get_used_airflow_sources,
    get_used_sources_setup_metadata_hash,
    in_autocomplete,
)
from airflow_breeze.utils.run_utils import check_pre_commit_installed, filter_out_none, run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE

find_airflow_sources_root_to_operate_on()

output_file_for_recording = os.environ.get('RECORD_BREEZE_OUTPUT_FILE')


class BetterChoice(click.Choice):
    """
    Nicer formatted choice class for click. We have a lot of parameters sometimes, and formatting
    them without spaces causes ugly artifacts as the words are broken. This one adds spaces so
    that when the long list of choices does not wrap on words.
    """

    def get_metavar(self, param) -> str:
        choices_str = " | ".join(self.choices)
        # Use curly braces to indicate a required argument.
        if param.required and param.param_type_name == "argument":
            return f"{{{choices_str}}}"

        if param.param_type_name == "argument" and param.nargs == -1:
            # avoid double [[ for multiple args
            return f"{choices_str}"

        # Use square braces to indicate an option or optional argument.
        return f"[{choices_str}]"


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

option_answer = click.option(
    "-a",
    "--answer",
    type=BetterChoice(['y', 'n', 'q', 'yes', 'no', 'quit']),
    help="Force answer to questions.",
    envvar='ANSWER',
)

option_python = click.option(
    '-p',
    '--python',
    type=BetterChoice(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    help='Python major/minor version used in Airflow image for PROD/CI images.',
    envvar='PYTHON_MAJOR_MINOR_VERSION',
)

option_backend = click.option(
    '-b',
    '--backend',
    help="Database backend to use.",
    type=BetterChoice(ALLOWED_BACKENDS),
)

option_integration = click.option(
    '--integration',
    help="Integration(s) to enable when running (can be more than one).",
    type=BetterChoice(ALLOWED_INTEGRATIONS),
    multiple=True,
)

option_postgres_version = click.option(
    '-P', '--postgres-version', help="Version of Postgres.", type=BetterChoice(ALLOWED_POSTGRES_VERSIONS)
)

option_mysql_version = click.option(
    '-M', '--mysql-version', help="Version of MySQL.", type=BetterChoice(ALLOWED_MYSQL_VERSIONS)
)

option_mssql_version = click.option(
    '-S', '--mssql-version', help="Version of MsSQL.", type=BetterChoice(ALLOWED_MSSQL_VERSIONS)
)

option_executor = click.option(
    '--executor',
    help='Executor to use for a kubernetes cluster. Default is KubernetesExecutor.',
    type=BetterChoice(ALLOWED_EXECUTORS),
)

option_forward_credentials = click.option(
    '-f', '--forward-credentials', help="Forward local credentials to container when running.", is_flag=True
)

option_use_airflow_version = click.option(
    '-V',
    '--use-airflow-version',
    help="Use (reinstall at entry) Airflow version from PyPI.",
    envvar='USE_AIRFLOW_VERSION',
)

option_mount_sources = click.option(
    '--mount-sources',
    type=BetterChoice(ALLOWED_MOUNT_OPTIONS),
    default=ALLOWED_MOUNT_OPTIONS[0],
    show_default=True,
    help="Choose scope of local sources should be mounted (default = selected).",
)

option_force_build = click.option(
    '-f', '--force-build', help="Force image build no matter if it is " "determined as needed.", is_flag=True
)

option_db_reset = click.option(
    '-d',
    '--db-reset',
    help="Reset DB when entering the container.",
    is_flag=True,
    envvar='DB_RESET',
)

option_github_repository = click.option(
    '-g',
    '--github-repository',
    help='GitHub repository used to pull, push run images.',
    default="apache/airflow",
    show_default=True,
    envvar='GITHUB_REPOSITORY',
)


@click.group(invoke_without_command=True, context_settings={'help_option_names': ['-h', '--help']})
@option_verbose
@option_dry_run
@option_python
@option_github_repository
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
@option_answer
@click.pass_context
def main(ctx: Context, **kwargs):
    create_directories()
    if not ctx.invoked_subcommand:
        ctx.forward(shell, extra_args={})


option_docker_cache = click.option(
    '-c',
    '--docker-cache',
    help='Cache option for image used during the build.',
    default=ALLOWED_BUILD_CACHE[0],
    show_default=True,
    type=BetterChoice(ALLOWED_BUILD_CACHE),
)

option_login_to_github_registry = click.option(
    '--login-to-github-registry',
    help='Logs in to GitHub registry.',
    envvar='LOGIN_TO_GITHUB_REGISTRY',
)


option_github_token = click.option(
    '--github-token',
    help='The token used to authenticate to GitHub.',
    envvar='GITHUB_TOKEN',
)

option_github_username = click.option(
    '--github-username',
    help='The user name used to authenticate to GitHub.',
    envvar='GITHUB_USERNAME',
)

option_github_image_id = click.option(
    '-s',
    '--github-image-id',
    help='Commit SHA of the image. \
    Breeze can automatically pull the commit SHA id specified Default: latest',
)

option_image_tag = click.option(
    '-t',
    '--image-tag',
    help='Tag added to the default naming conventions of Airflow CI/PROD images.',
    envvar='IMAGE_TAG',
)

option_image_name = click.option(
    '-n', '--image-name', help='Name of the image to verify (overrides --python and --image-tag).'
)

option_platform = click.option(
    '--platform',
    help='Platform for Airflow image.',
    envvar='PLATFORM',
    type=BetterChoice(ALLOWED_PLATFORMS),
)

option_debian_version = click.option(
    '-d',
    '--debian-version',
    help='Debian version used for the image.',
    type=BetterChoice(ALLOWED_DEBIAN_VERSIONS),
    default=ALLOWED_DEBIAN_VERSIONS[0],
    show_default=True,
    envvar='DEBIAN_VERSION',
)
option_upgrade_to_newer_dependencies = click.option(
    "-u",
    '--upgrade-to-newer-dependencies',
    default="false",
    show_default=True,
    help='When other than "false", upgrade all PIP packages to latest.',
    envvar='UPGRADE_TO_NEWER_DEPENDENCIES',
)
option_additional_extras = click.option(
    '--additional-extras',
    help='Additional extra package while installing Airflow in the image.',
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
    help='Command executed before dev apt deps are installed.',
    envvar='DEV_APT_COMMAND',
)
option_dev_apt_deps = click.option(
    '--dev-apt-deps',
    help='Apt dev dependencies to use when building the images.',
    envvar='DEV_APT_DEPS',
)
option_runtime_apt_command = click.option(
    '--runtime-apt-command',
    help='Command executed before runtime apt deps are installed.',
    envvar='RUNTIME_APT_COMMAND',
)
option_runtime_apt_deps = click.option(
    '--runtime-apt-deps',
    help='Apt runtime dependencies to use when building the images.',
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

option_push_image = click.option(
    '--push-image',
    help='Push image after building it.',
    is_flag=True,
    envvar='PUSH_IMAGE',
)

option_empty_image = click.option(
    '--empty-image',
    help='Prepare empty image tagged with the same name as the Airflow image.',
    is_flag=True,
    envvar='EMPTY_IMAGE',
)

option_wait_for_image = click.option(
    '--wait-for-image',
    help='Wait until image is available.',
    is_flag=True,
    envvar='WAIT_FOR_IMAGE',
)

option_tag_as_latest = click.option(
    '--tag-as-latest',
    help='Tags the image as latest after pulling.',
    is_flag=True,
    envvar='TAG_AS_LATEST',
)

option_verify_image = click.option(
    '--verify-image',
    help='Verify image.',
    is_flag=True,
    envvar='VERIFY_IMAGE',
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

option_version_suffix_for_pypi = click.option(
    '--version-suffix-for-pypi',
    help='Version suffix used for PyPI packages (alpha, beta, rc1, etc.).',
    default="",
    envvar='VERSION_SUFFIX_FOR_PYPI',
)

option_package_format = click.option(
    '--package-format',
    type=BetterChoice(ALLOWED_PACKAGE_FORMATS),
    help='Format of packages.',
    default=ALLOWED_PACKAGE_FORMATS[0],
    show_default=True,
    envvar='PACKAGE_FORMAT',
)

option_python_versions = click.option(
    '--python-versions',
    help="Space separated list of python versions used for build with multiple versions.",
    default=" ".join(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS),
    show_default=True,
    envvar="PYTHON_VERSIONS",
)

option_run_in_parallel = click.option(
    '--run-in-parallel',
    help="Run the operation in parallel on all or selected subset of Python versions.",
    is_flag=True,
    envvar='RUN_IN_PARALLEL',
)

option_parallelism = click.option(
    '--parallelism',
    help="Maximum number of processes to use while running the operation in parallel.",
    type=IntRange(1, mp.cpu_count() * 2 if not output_file_for_recording else 8),
    default=mp.cpu_count() if not output_file_for_recording else 4,
    envvar='PARALLELISM',
    show_default=True,
)

option_build_multiple_images = click.option(
    '--build_multiple_images',
    help="Run the operation sequentially on all or selected subset of Python versions.",
    is_flag=True,
    envvar='BUILD_MULTIPLE_IMAGES',
)

option_with_ci_group = click.option(
    '-g',
    '--with-ci-group',
    is_flag=True,
    help="Uses CI group for the command to fold long logs in logical groups.",
    envvar='WITH_CI_GROUP',
)

argument_packages = click.argument(
    "packages",
    nargs=-1,
    required=False,
    type=BetterChoice(get_available_packages(short_version=True)),
)


@option_verbose
@main.command()
def version(verbose: bool):
    """Print information about version of apache-airflow-breeze."""
    console.print(ASCIIART, style=ASCIIART_STYLE)
    console.print(f"\n[bright_blue]Breeze version: {VERSION}[/]")
    console.print(f"[bright_blue]Breeze installed from: {get_installation_airflow_sources()}[/]")
    console.print(f"[bright_blue]Used Airflow sources : {get_used_airflow_sources()}[/]\n")
    if verbose:
        console.print(
            f"[bright_blue]Installation sources config hash : "
            f"{get_installation_sources_config_metadata_hash()}[/]"
        )
        console.print(
            f"[bright_blue]Used sources config hash         : " f"{get_used_sources_setup_metadata_hash()}[/]"
        )
        console.print(
            f"[bright_blue]Package config hash              : " f"{(get_package_setup_metadata_hash())}[/]\n"
        )


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
@option_github_repository
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_mount_sources
@option_integration
@option_db_reset
@option_answer
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def shell(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
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
    answer: Optional[str],
    extra_args: Tuple,
):
    """Enter breeze.py environment. this is the default command use when no other is selected."""
    set_forced_answer(answer)
    if verbose or dry_run:
        console.print("\n[green]Welcome to breeze.py[/]\n")
        console.print(f"\n[green]Root of Airflow Sources = {AIRFLOW_SOURCES_ROOT}[/]\n")
    enter_shell(
        verbose=verbose,
        dry_run=dry_run,
        python=python,
        github_repository=github_repository,
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
        answer=answer,
    )


@option_verbose
@main.command(name='start-airflow')
@option_dry_run
@option_python
@option_github_repository
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
@option_answer
@click.argument('extra-args', nargs=-1, type=click.UNPROCESSED)
def start_airflow(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
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
    answer: Optional[str],
    extra_args: Tuple,
):
    """Enter breeze.py environment and starts all Airflow components in the tmux session."""
    set_forced_answer(answer)
    enter_shell(
        verbose=verbose,
        dry_run=dry_run,
        python=python,
        github_repository=github_repository,
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
        answer=answer,
    )


@main.command(name='build-image')
@option_github_repository
@option_verbose
@option_dry_run
@option_with_ci_group
@option_answer
@option_python
@option_build_multiple_images
@option_python_versions
@option_upgrade_to_newer_dependencies
@option_platform
@option_debian_version
@option_github_token
@option_github_username
@option_login_to_github_registry
@option_docker_cache
@option_image_tag
@option_prepare_buildx_cache
@option_push_image
@option_empty_image
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
@option_force_build
def build_image(
    verbose: bool,
    dry_run: bool,
    with_ci_group: bool,
    build_multiple_images: bool,
    python_versions: str,
    answer: str,
    **kwargs,
):
    """Build CI image. Include building multiple images for all python versions (sequentially)."""

    def run_build(ci_image_params: BuildCiParams) -> None:
        return_code, info = build_ci_image(
            verbose=verbose, dry_run=dry_run, with_ci_group=with_ci_group, ci_image_params=ci_image_params
        )
        if return_code != 0:
            console.print(f"[red]Error when building image! {info}")
            sys.exit(return_code)

    set_forced_answer(answer)
    parameters_passed = filter_out_none(**kwargs)
    if build_multiple_images:
        with ci_group(f"Building images sequentially {python_versions}", enabled=with_ci_group):
            python_version_list = get_python_version_list(python_versions)
            for python in python_version_list:
                params = get_ci_image_build_params(parameters_passed)
                params.python = python
                params.answer = answer
                run_build(ci_image_params=params)
    else:
        params = get_ci_image_build_params(parameters_passed)
        synchronize_parameters_with_cache(params, parameters_passed)
        run_build(ci_image_params=params)


def get_python_version_list(python_versions: str) -> List[str]:
    """
    Retrieve and validate space-separated list of Python versions and return them in the form of list.
    :param python_versions: space separated list of Python versions
    :return: List of python versions
    """
    python_version_list = python_versions.split(" ")
    errors = False
    for python in python_version_list:
        if python not in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS:
            console.print(f"[red]The Python version {python} passed in {python_versions} is wrong.[/]")
            errors = True
    if errors:
        console.print(
            f"\nSome of the Python versions passed are not in the "
            f"list: {ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS}. Quitting.\n"
        )
        sys.exit(1)
    return python_version_list


@main.command(name='pull-image')
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_verify_image
@option_wait_for_image
@option_tag_as_latest
@option_image_tag
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def pull_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    run_in_parallel: bool,
    python_versions: str,
    parallelism: int,
    image_tag: Optional[str],
    wait_for_image: bool,
    tag_as_latest: bool,
    verify_image: bool,
    extra_pytest_args: Tuple,
):
    """Pull and optionally verify CI images - possibly in parallel for all Python versions."""
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        ci_image_params_list = [
            BuildCiParams(image_tag=image_tag, python=python, github_repository=github_repository)
            for python in python_version_list
        ]
        run_pull_in_parallel(
            dry_run=dry_run,
            parallelism=parallelism,
            image_params_list=ci_image_params_list,
            python_version_list=python_version_list,
            verbose=verbose,
            verify_image=verify_image,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            extra_pytest_args=extra_pytest_args if extra_pytest_args is not None else (),
        )
    else:
        image_params = BuildCiParams(image_tag=image_tag, python=python, github_repository=github_repository)
        synchronize_parameters_with_cache(image_params, {"python": python})
        return_code, info = run_pull_image(
            image_params=image_params,
            dry_run=dry_run,
            verbose=verbose,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            poll_time=10.0,
        )
        if return_code != 0:
            console.print(f"[red]There was an error when pulling CI image: {info}[/]")
            sys.exit(return_code)


@main.command(
    name='verify-image',
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_image_tag
@option_image_name
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def verify_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: str,
    extra_pytest_args: Tuple,
):
    """Verify CI image."""
    if image_name is None:
        build_params = get_ci_image_build_params(
            {"python": python, "image_tag": image_tag, "github_repository": github_repository}
        )
        image_name = build_params.airflow_image_name_with_tag
    console.print(f"[bright_blue]Verifying CI image: {image_name}[/]")
    return_code, info = verify_an_image(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        image_type='CI',
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)


@option_verbose
@option_dry_run
@option_answer
@option_with_ci_group
@main.command(name='build-prod-image')
@option_python
@option_build_multiple_images
@option_python_versions
@option_upgrade_to_newer_dependencies
@option_platform
@option_debian_version
@option_github_repository
@option_github_token
@option_github_username
@option_login_to_github_registry
@option_docker_cache
@option_image_tag
@option_prepare_buildx_cache
@option_push_image
@option_empty_image
@click.option(
    '--installation-method',
    help="Install Airflow from: sources or PyPI.",
    type=BetterChoice(ALLOWED_INSTALLATION_METHODS),
)
@option_install_providers_from_sources
@click.option(
    '--install-from-docker-context-files',
    help='Install wheels from local docker-context-files when building image.',
    is_flag=True,
)
@click.option(
    '--cleanup-docker-context-files',
    help='Clean up docker context files before running build.',
    is_flag=True,
)
@click.option(
    '--extras', default=",".join(DEFAULT_EXTRAS), show_default=True, help="Extras to install by default."
)
@click.option('--disable-mysql-client-installation', help="Do not install MySQL client.", is_flag=True)
@click.option('--disable-mssql-client-installation', help="Do not install MsSQl client.", is_flag=True)
@click.option('--disable-postgres-client-installation', help="Do not install Postgres client.", is_flag=True)
@click.option(
    '--disable-airflow-repo-cache',
    help="Disable cache from Airflow repository during building.",
    is_flag=True,
)
@click.option('--disable-pypi', help="Disable PyPI during building.", is_flag=True)
@click.option(
    '--install-airflow-reference',
    help="Install Airflow using GitHub tag or branch.",
)
@click.option('-V', '--install-airflow-version', help="Install version of Airflow from PyPI.")
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
    with_ci_group: bool,
    build_multiple_images: bool,
    python_versions: str,
    answer: Optional[str],
    **kwargs,
):
    """
    Build Production image. Include building multiple images for all or selected Python versions sequentially.
    """

    def run_build(prod_image_params: BuildProdParams) -> None:
        return_code, info = build_production_image(
            verbose=verbose, dry_run=dry_run, with_ci_group=with_ci_group, prod_image_params=prod_image_params
        )
        if return_code != 0:
            console.print(f"[red]Error when building image! {info}")
            sys.exit(return_code)

    set_forced_answer(answer)
    parameters_passed = filter_out_none(**kwargs)
    if build_multiple_images:
        with ci_group(f"Building images sequentially {python_versions}", enabled=with_ci_group):
            python_version_list = get_python_version_list(python_versions)
            for python in python_version_list:
                params = get_prod_image_build_params(parameters_passed)
                params.python = python
                params.answer = answer
                run_build(prod_image_params=params)
    else:
        params = get_prod_image_build_params(parameters_passed)
        synchronize_parameters_with_cache(params, parameters_passed)
        run_build(prod_image_params=params)


@main.command(name='pull-prod-image')
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_image_tag
@option_wait_for_image
@option_tag_as_latest
@option_verify_image
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def pull_prod_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    image_tag: Optional[str],
    wait_for_image: bool,
    tag_as_latest: bool,
    verify_image: bool,
    extra_pytest_args: Tuple,
):
    """Pull and optionally verify Production images - possibly in parallel for all Python versions."""
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        prod_image_params_list = [
            BuildProdParams(image_tag=image_tag, python=python, github_repository=github_repository)
            for python in python_version_list
        ]
        run_pull_in_parallel(
            dry_run=dry_run,
            parallelism=parallelism,
            image_params_list=prod_image_params_list,
            python_version_list=python_version_list,
            verbose=verbose,
            verify_image=verify_image,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            extra_pytest_args=extra_pytest_args if extra_pytest_args is not None else (),
        )
    else:
        image_params = BuildProdParams(
            image_tag=image_tag, python=python, github_repository=github_repository
        )
        synchronize_parameters_with_cache(image_params, {"python": python})
        return_code, info = run_pull_image(
            image_params=image_params,
            dry_run=dry_run,
            verbose=verbose,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            poll_time=10.0,
        )
        if return_code != 0:
            console.print(f"[red]There was an error when pulling PROD image: {info}[/]")
            sys.exit(return_code)


@main.command(
    name='verify-prod-image',
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_image_tag
@option_image_name
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def verify_prod_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: str,
    extra_pytest_args: Tuple,
):
    """Verify Production image."""
    if image_name is None:
        build_params = get_prod_image_build_params(
            {"python": python, "image_tag": image_tag, "github_repository": github_repository}
        )
        image_name = build_params.airflow_image_name_with_tag
    console.print(f"[bright_blue]Verifying PROD image: {image_name}[/]")
    return_code, info = verify_an_image(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        image_type='PROD',
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)


@main.command(
    name='docker-compose-tests',
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_image_tag
@option_image_name
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def docker_compose_tests(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: str,
    extra_pytest_args: Tuple,
):
    """Run docker-compose tests."""
    if image_name is None:
        build_params = get_prod_image_build_params(
            {"python": python, "image_tag": image_tag, "github_repository": github_repository}
        )
        image_name = build_params.airflow_image_name_with_tag
    console.print(f"[bright_blue]Running docker-compose with PROD image: {image_name}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)


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
                    "You can force autocomplete installation by adding --force[/]\n"
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
        f"\n\n   source {script_path}\n"
    )
    return True


@option_verbose
@option_dry_run
@click.option(
    '-f',
    '--force',
    is_flag=True,
    help='Force autocomplete setup even if already setup before (overrides the setup).',
)
@option_answer
@main.command(name='setup-autocomplete')
def setup_autocomplete(verbose: bool, dry_run: bool, force: bool, answer: Optional[str]):
    """
    Enables autocompletion of breeze commands.
    """
    set_forced_answer(answer)
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
    console.print(f"[bright_yellow]We need to add above script to your {detected_shell} profile.[/]\n")
    given_answer = user_confirm("Should we proceed ?", default_answer=Answer.NO, timeout=3)
    if given_answer == Answer.YES:
        if detected_shell == 'bash':
            script_path = str(Path('~').expanduser() / '.bash_completion')
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force)
        elif detected_shell == 'zsh':
            script_path = str(Path('~').expanduser() / '.zshrc')
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force)
        elif detected_shell == 'fish':
            # Include steps for fish shell
            script_path = str(Path('~').expanduser() / f'.config/fish/completions/{NAME}.fish')
            if os.path.exists(script_path) and not force:
                console.print(
                    "\n[bright_yellow]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force/]\n"
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
            write_to_shell(command_to_execute, dry_run, script_path, force)
    elif given_answer == Answer.NO:
        console.print(
            "\nPlease follow the https://click.palletsprojects.com/en/8.1.x/shell-completion/ "
            "to setup autocompletion for breeze manually if you want to use it.\n"
        )
    else:
        sys.exit(0)


@main.command(name='config')
@option_python
@option_backend
@click.option('-C/-c', '--cheatsheet/--no-cheatsheet', help="Enable/disable cheatsheet.", default=None)
@click.option('-A/-a', '--asciiart/--no-asciiart', help="Enable/disable ASCIIart.", default=None)
def change_config(python, backend, cheatsheet, asciiart):
    """
    Show/update configuration (Python, Backend, Cheatsheet, ASCIIART).
    """
    asciiart_file = "suppress_asciiart"
    cheatsheet_file = "suppress_cheatsheet"
    python_file = 'PYTHON_MAJOR_MINOR_VERSION'
    backend_file = 'BACKEND'
    if asciiart is not None:
        if asciiart:
            delete_cache(asciiart_file)
            console.print('[bright_blue]Enable ASCIIART![/]')
        else:
            touch_cache_file(asciiart_file)
            console.print('[bright_blue]Disable ASCIIART![/]')
    if cheatsheet is not None:
        if cheatsheet:
            delete_cache(cheatsheet_file)
            console.print('[bright_blue]Enable Cheatsheet[/]')
        elif cheatsheet is not None:
            touch_cache_file(cheatsheet_file)
            console.print('[bright_blue]Disable Cheatsheet[/]')
    if python is not None:
        write_to_cache_file(python_file, python)
        console.print(f'[bright_blue]Python default value set to: {python}[/]')
    if backend is not None:
        write_to_cache_file(backend_file, backend)
        console.print(f'[bright_blue]Backend default value set to: {backend}[/]')

    def get_status(file: str):
        return "disabled" if check_if_cache_exists(file) else "enabled"

    console.print()
    console.print("[bright_blue]Current configuration:[/]")
    console.print()
    console.print(f"[bright_blue]* Python: {read_from_cache_file(python_file)}[/]")
    console.print(f"[bright_blue]* Backend: {read_from_cache_file(backend_file)}[/]")
    console.print(f"[bright_blue]* ASCIIART: {get_status(asciiart_file)}[/]")
    console.print(f"[bright_blue]* Cheatsheet: {get_status(cheatsheet_file)}[/]")
    console.print()


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
@option_github_repository
@click.option('-d', '--docs-only', help="Only build documentation.", is_flag=True)
@click.option('-s', '--spellcheck-only', help="Only run spell checking.", is_flag=True)
@click.option(
    '-p',
    '--package-filter',
    help="List of packages to consider.",
    type=BetterChoice(get_available_packages()),
    multiple=True,
)
def build_docs(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    docs_only: bool,
    spellcheck_only: bool,
    package_filter: Tuple[str],
):
    """Build documentation in the container."""
    params = BuildCiParams(github_repository=github_repository, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    ci_image_name = params.airflow_image_name
    check_docker_resources(verbose, ci_image_name)
    doc_builder = DocParams(
        package_filter=package_filter,
        docs_only=docs_only,
        spellcheck_only=spellcheck_only,
    )
    extra_docker_flags = get_extra_docker_flags(MOUNT_SELECTED)
    env = construct_env_variables_docker_compose_command(params)
    cmd = [
        "docker",
        "run",
        "-t",
        *extra_docker_flags,
        "-e",
        "GITHUB_ACTIONS=",
        "-e",
        "SKIP_ENVIRONMENT_INITIALIZATION=true",
        "--pull",
        "never",
        ci_image_name,
        "/opt/airflow/scripts/in_container/run_docs_build.sh",
        *doc_builder.args_doc_builder,
    ]
    process = run_command(cmd, verbose=verbose, dry_run=dry_run, text=True, env=env, check=False)
    sys.exit(process.returncode)


@main.command(
    name="static-checks",
    help="Run static checks.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option(
    '-t',
    '--type',
    help="Type(s) of the static checks to run (multiple can be added).",
    type=BetterChoice(PRE_COMMIT_LIST),
    multiple=True,
)
@click.option('-a', '--all-files', help="Run checks on all files.", is_flag=True)
@click.option('-f', '--files', help="List of files to run the checks on.", multiple=True)
@click.option(
    '-s', '--show-diff-on-failure', help="Show diff for files modified by the checks.", is_flag=True
)
@click.option('-c', '--last-commit', help="Run checks for all files in last commit.", is_flag=True)
@option_verbose
@option_dry_run
@option_github_repository
@click.argument('precommit_args', nargs=-1, type=click.UNPROCESSED)
def static_checks(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
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
        if verbose or dry_run:
            command_to_execute.append("--verbose")
        if precommit_args:
            command_to_execute.extend(precommit_args)
        env = os.environ.copy()
        env['GITHUB_REPOSITORY'] = github_repository
        static_checks_result = run_command(
            command_to_execute,
            verbose=verbose,
            dry_run=dry_run,
            check=False,
            no_output_dump_on_exception=True,
            text=True,
            env=env,
        )
        if static_checks_result.returncode != 0:
            console.print("[red]There were errors during pre-commit check. They should be fixed[/n]")


@main.command(name="stop", help="Stop running breeze environment.")
@option_verbose
@option_dry_run
@click.option(
    "-p",
    "--preserve-volumes",
    help="Skip removing volumes when stopping Breeze.",
    is_flag=True,
)
def stop(verbose: bool, dry_run: bool, preserve_volumes: bool):
    command_to_execute = ['docker-compose', 'down', "--remove-orphans"]
    if not preserve_volumes:
        command_to_execute.append("--volumes")
    shell_params = ShellParams(verbose=verbose)
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    run_command(command_to_execute, verbose=verbose, dry_run=dry_run, env=env_variables)


@click.option(
    '-f',
    '--force',
    is_flag=True,
    help='Force upgrade without asking question to the user.',
)
@click.option(
    '-a',
    '--use-current-airflow-sources',
    is_flag=True,
    help='Use current workdir Airflow sources for upgrade'
    + (f" rather than from {get_installation_airflow_sources()}." if not output_file_for_recording else "."),
)
@main.command(
    name='self-upgrade',
    help="Self upgrade Breeze. By default it re-installs Breeze "
    f"from {get_installation_airflow_sources()}."
    if not output_file_for_recording
    else "Self upgrade Breeze.",
)
def self_upgrade(force: bool, use_current_airflow_sources: bool):
    if use_current_airflow_sources:
        airflow_sources: Optional[Path] = get_used_airflow_sources()
    else:
        airflow_sources = get_installation_airflow_sources()
    if airflow_sources is not None:
        breeze_sources = airflow_sources / "dev" / "breeze"
        if force:
            reinstall_breeze(breeze_sources)
        else:
            ask_to_reinstall_breeze(breeze_sources)
    else:
        warn_non_editable()
        sys.exit(1)


@main.command(
    name='prepare-airflow-package',
    help="Prepare sdist/whl package of Airflow.",
)
@option_verbose
@option_dry_run
@option_github_repository
@option_with_ci_group
@option_package_format
@option_version_suffix_for_pypi
def prepare_airflow_packages(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    with_ci_group: bool,
    package_format: str,
    version_suffix_for_pypi: str,
):
    shell_params = ShellParams(
        verbose=verbose, github_repository=github_repository, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION
    )
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    env_variables['INSTALL_PROVIDERS_FROM_SOURCES'] = "false"
    extra_docker_flags = get_extra_docker_flags(MOUNT_ALL)
    with ci_group("Prepare Airflow package", enabled=with_ci_group):
        run_command(
            [
                "docker",
                "run",
                "-t",
                *extra_docker_flags,
                "-e",
                "SKIP_ENVIRONMENT_INITIALIZATION=true",
                "-e",
                f"PACKAGE_FORMAT={package_format}",
                "-e",
                f"VERSION_SUFFIX_FOR_PYPI={version_suffix_for_pypi}",
                "--pull",
                "never",
                shell_params.airflow_image_name_with_tag,
                "/opt/airflow/scripts/in_container/run_prepare_airflow_packages.sh",
            ],
            verbose=verbose,
            dry_run=dry_run,
            env=env_variables,
        )


@main.command(
    name='prepare-provider-documentation',
    help="Prepare CHANGELOG, README and COMMITS information for providers.",
)
@option_verbose
@option_dry_run
@option_github_repository
@option_with_ci_group
@option_answer
@click.option(
    '--skip-package-verification',
    help="Skip Provider package verification.",
    is_flag=True,
)
@argument_packages
def prepare_provider_documentation(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    with_ci_group: bool,
    answer: Optional[str],
    skip_package_verification: bool,
    packages: List[str],
):
    set_forced_answer(answer)
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        skip_package_verification=skip_package_verification,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        answer=answer,
    )
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    extra_docker_flags = get_extra_docker_flags(shell_params.mount_sources)
    if answer and answer.lower() in ['y', 'yes']:
        term_flag = "-t"
    else:
        term_flag = "-it"
    cmd_to_run = [
        "docker",
        "run",
        term_flag,
        *extra_docker_flags,
        "-e",
        "SKIP_ENVIRONMENT_INITIALIZATION=true",
        "--pull",
        "never",
        shell_params.airflow_image_name_with_tag,
        "/opt/airflow/scripts/in_container/run_prepare_provider_documentation.sh",
    ]
    if packages:
        cmd_to_run.extend(packages)
    with ci_group("Prepare provider documentation", enabled=with_ci_group):
        run_command(cmd_to_run, verbose=verbose, dry_run=dry_run, env=env_variables)


@main.command(
    name='prepare-provider-packages',
    help="Prepare sdist/whl packages of Airflow Providers.",
)
@option_verbose
@option_dry_run
@option_github_repository
@option_with_ci_group
@option_package_format
@option_version_suffix_for_pypi
@click.option(
    '--package-list-file',
    type=click.File('rt'),
    help='Read list of packages from text file (one package per line)',
)
@argument_packages
def prepare_provider_packages(
    verbose: bool,
    dry_run: bool,
    github_repository: str,
    with_ci_group: bool,
    package_format: str,
    version_suffix_for_pypi: str,
    package_list_file: IO,
    packages: Tuple[str, ...],
):
    packages_list = list(packages)
    if package_list_file:
        packages_list.extend([package.strip() for package in package_list_file.readlines()])
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    )
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    extra_docker_flags = get_extra_docker_flags(shell_params.mount_sources)
    cmd_to_run = [
        "docker",
        "run",
        "-t",
        *extra_docker_flags,
        "-e",
        "SKIP_ENVIRONMENT_INITIALIZATION=true",
        "-e",
        f"PACKAGE_FORMAT={package_format}",
        "-e",
        f"VERSION_SUFFIX_FOR_PYPI={version_suffix_for_pypi}",
        "--pull",
        "never",
        shell_params.airflow_image_name_with_tag,
        "/opt/airflow/scripts/in_container/run_prepare_provider_packages.sh",
    ]
    cmd_to_run.extend(packages_list)
    with ci_group("Prepare provider packages", enabled=with_ci_group):
        run_command(cmd_to_run, verbose=verbose, dry_run=dry_run, env=env_variables)


@main.command(
    name='generate-constraints',
    help="Generates pinned constraint files with all extras from setup.py in parallel.",
)
@click.option(
    '--generate-constraints-mode',
    type=BetterChoice(ALLOWED_GENERATE_CONSTRAINTS_MODES),
    default=ALLOWED_GENERATE_CONSTRAINTS_MODES[0],
    show_default=True,
    help='Mode of generating constraints',
)
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_image_tag
@option_answer
@option_with_ci_group
def generate_constraints(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    image_tag: str,
    answer: Optional[str],
    generate_constraints_mode: str,
    with_ci_group: bool,
):
    set_forced_answer(answer)
    if run_in_parallel:
        given_answer = user_confirm(
            f"Did you build all CI images {python_versions} with --upgrade-to-newer-dependencies "
            f"true flag set?",
            timeout=None,
        )
    else:
        given_answer = user_confirm(
            f"Did you build CI image {python} with --upgrade-to-newer-dependencies true flag set?",
            timeout=None,
        )
    if given_answer != Answer.YES:
        if run_in_parallel:
            console.print("\n[yellow]Use this command to build the images:[/]\n")
            console.print(
                f"     breeze build-image --run-in-parallel --python-versions '{python_versions}' "
                f"--upgrade-to-newer-dependencies true\n"
            )
        else:
            shell_params = ShellParams(
                image_tag=image_tag, python=python, github_repository=github_repository, answer=answer
            )
            synchronize_parameters_with_cache(shell_params, {"python": python})
            console.print("\n[yellow]Use this command to build the image:[/]\n")
            console.print(
                f"     breeze build-image --python '{shell_params.python}' "
                f"--upgrade-to-newer-dependencies true\n"
            )
        sys.exit(1)
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        shell_params_list = [
            ShellParams(
                image_tag=image_tag, python=python, github_repository=github_repository, answer=answer
            )
            for python in python_version_list
        ]
        with ci_group(f"Generating constraints with {generate_constraints_mode}", enabled=with_ci_group):
            run_generate_constraints_in_parallel(
                shell_params_list=shell_params_list,
                parallelism=parallelism,
                dry_run=dry_run,
                verbose=verbose,
                python_version_list=python_version_list,
                generate_constraints_mode=generate_constraints_mode,
            )
    else:
        with ci_group(f"Generating constraints with {generate_constraints_mode}", enabled=with_ci_group):
            shell_params = ShellParams(
                image_tag=image_tag, python=python, github_repository=github_repository, answer=answer
            )
            synchronize_parameters_with_cache(shell_params, {"python": python})
            return_code, info = run_generate_constraints(
                shell_params=shell_params,
                dry_run=dry_run,
                verbose=verbose,
                generate_constraints_mode=generate_constraints_mode,
            )
        if return_code != 0:
            console.print(f"[red]There was an error when generating constraints: {info}[/]")
            sys.exit(return_code)


@main.command(
    name="cleanup",
    help="Cleans the cache of parameters, docker cache and optionally - currently downloaded images.",
)
@click.option(
    '--all',
    is_flag=True,
    help='Also remove currently downloaded Breeze images.',
)
@option_verbose
@option_answer
@option_dry_run
def cleanup(verbose: bool, dry_run: bool, github_repository: str, all: bool, answer: Optional[str]):
    set_forced_answer(answer)
    if all:
        console.print(
            "\n[bright_yellow]Removing cache of parameters, clean up docker cache "
            "and remove locally downloaded images[/]"
        )
    else:
        console.print("\n[bright_yellow]Removing cache of parameters, and cleans up docker cache[/]")
    if all:
        docker_images_command_to_execute = [
            'docker',
            'images',
            '--filter',
            'label=org.apache.airflow.image',
            '--format',
            '{{.Repository}}:{{.Tag}}',
        ]
        command_result = run_command(
            docker_images_command_to_execute, verbose=verbose, text=True, capture_output=True
        )
        images = command_result.stdout.splitlines() if command_result and command_result.stdout else []
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
            given_answer = user_confirm("Are you sure?", timeout=None)
            if given_answer == Answer.YES:
                run_command(docker_rmi_command_to_execute, verbose=verbose, dry_run=dry_run, check=False)
            elif given_answer == Answer.QUIT:
                sys.exit(0)
        else:
            console.print("[light_blue]No locally downloaded images to remove[/]\n")
    console.print("Pruning docker images")
    given_answer = user_confirm("Are you sure?", timeout=None)
    if given_answer == Answer.YES:
        system_prune_command_to_execute = ['docker', 'system', 'prune']
        run_command(system_prune_command_to_execute, verbose=verbose, dry_run=dry_run, check=False)
    elif given_answer == Answer.QUIT:
        sys.exit(0)
    console.print(f"Removing build cache dir ${BUILD_CACHE_DIR}")
    given_answer = user_confirm("Are you sure?", timeout=None)
    if given_answer == Answer.YES:
        if not dry_run:
            shutil.rmtree(BUILD_CACHE_DIR, ignore_errors=True)
    elif given_answer == Answer.QUIT:
        sys.exit(0)


help_console = None


def enable_recording_of_help_output(path: str, title: Optional[str], width: Optional[str]):
    if not title:
        title = "Breeze screenshot"
    if not width:
        width_int = 120
    else:
        width_int = int(width)

    def save_ouput_as_svg():
        if help_console:
            help_console.save_svg(path=path, title=title)

    class RecordingConsole(rich.console.Console):
        def __init__(self, **kwargs):
            super().__init__(record=True, width=width_int, force_terminal=True, **kwargs)
            global help_console
            help_console = self

    atexit.register(save_ouput_as_svg)
    click.rich_click.MAX_WIDTH = width_int
    click.formatting.FORCED_WIDTH = width_int - 2
    click.rich_click.COLOR_SYSTEM = "standard"
    # monkeypatch rich_click console to record help (rich_click does not allow passing extra args to console)
    click.rich_click.Console = RecordingConsole


if output_file_for_recording and not in_autocomplete():
    enable_recording_of_help_output(
        path=output_file_for_recording,
        title=os.environ.get('RECORD_BREEZE_TITLE'),
        width=os.environ.get('RECORD_BREEZE_WIDTH'),
    )

if __name__ == '__main__':
    main()
