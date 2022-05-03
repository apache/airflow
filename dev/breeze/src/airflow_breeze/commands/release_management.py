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

import sys
from typing import IO, List, Optional, Tuple

import click

from airflow_breeze.commands.common_options import (
    argument_packages,
    option_answer,
    option_branch,
    option_dry_run,
    option_github_repository,
    option_image_tag,
    option_max_age,
    option_package_format,
    option_parallelism,
    option_python,
    option_python_versions,
    option_run_in_parallel,
    option_timezone,
    option_updated_on_or_after,
    option_verbose,
    option_version_suffix_for_pypi,
    option_with_ci_group,
)
from airflow_breeze.commands.custom_param_types import BetterChoice
from airflow_breeze.commands.main import main
from airflow_breeze.global_constants import (
    ALLOWED_GENERATE_CONSTRAINTS_MODES,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
)
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.constraints import run_generate_constraints, run_generate_constraints_in_parallel
from airflow_breeze.utils.docker_command_utils import (
    construct_env_variables_docker_compose_command,
    get_extra_docker_flags,
)
from airflow_breeze.utils.find_newer_dependencies import find_newer_dependencies
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.run_utils import run_command

RELEASE_MANAGEMENT_PARAMETERS = {
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
    "breeze find-newer-dependencies": [
        {
            "name": "Find newer dependencies flags",
            "options": [
                "--python",
                "--timezone",
                "--constraints-branch",
                "--updated-on-or-after",
                "--max-age",
            ],
        }
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
}

RELEASE_MANAGEMENT_COMMANDS = {
    "name": "Release management",
    "commands": [
        "prepare-provider-documentation",
        "prepare-provider-packages",
        "prepare-airflow-package",
        "generate-constraints",
        "find-newer-dependencies",
    ],
}


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
    if run_in_parallel:
        given_answer = user_confirm(
            f"Did you build all CI images {python_versions} with --upgrade-to-newer-dependencies flag set?",
        )
    else:
        given_answer = user_confirm(
            f"Did you build CI image {python} with --upgrade-to-newer-dependencies flag set?",
        )
    if given_answer != Answer.YES:
        if run_in_parallel:
            get_console().print("\n[info]Use this command to build the images:[/]\n")
            get_console().print(
                f"     breeze build-image --run-in-parallel --python-versions '{python_versions}' "
                f"--upgrade-to-newer-dependencies\n"
            )
        else:
            shell_params = ShellParams(
                image_tag=image_tag, python=python, github_repository=github_repository, answer=answer
            )
            get_console().print("\n[info]Use this command to build the image:[/]\n")
            get_console().print(
                f"     breeze build-image --python'{shell_params.python}' "
                f"--upgrade-to-newer-dependencies\n"
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
            return_code, info = run_generate_constraints(
                shell_params=shell_params,
                dry_run=dry_run,
                verbose=verbose,
                generate_constraints_mode=generate_constraints_mode,
            )
        if return_code != 0:
            get_console().print(f"[error]There was an error when generating constraints: {info}[/]")
            sys.exit(return_code)


@main.command(name="find-newer-dependencies", help="Finds which dependencies are being upgraded.")
@option_timezone
@option_branch
@option_python
@option_updated_on_or_after
@option_max_age
def breeze_find_newer_dependencies(
    constraints_branch: str, python: str, timezone: str, updated_on_or_after: str, max_age: int
):
    return find_newer_dependencies(
        constraints_branch=constraints_branch,
        python=python,
        timezone=timezone,
        updated_on_or_after=updated_on_or_after,
        max_age=max_age,
    )
