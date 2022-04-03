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
from pathlib import Path
from typing import Dict

import click
from inputimeout import TimeoutOccurred, inputimeout

from airflow_breeze import global_constants
from airflow_breeze.cache import (
    check_cache_and_write_if_not_cached,
    read_from_cache_file,
    write_to_cache_file,
)
from airflow_breeze.ci.build_image import build_image
from airflow_breeze.console import console
from airflow_breeze.global_constants import (
    FLOWER_HOST_PORT,
    MSSQL_HOST_PORT,
    MSSQL_VERSION,
    MYSQL_HOST_PORT,
    MYSQL_VERSION,
    POSTGRES_HOST_PORT,
    POSTGRES_VERSION,
    REDIS_HOST_PORT,
    SSH_PORT,
    WEBSERVER_HOST_PORT,
)
from airflow_breeze.shell.shell_builder import ShellBuilder
from airflow_breeze.utils.docker_command_utils import (
    check_docker_compose_version,
    check_docker_resources,
    check_docker_version,
)
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR
from airflow_breeze.utils.run_utils import (
    filter_out_none,
    get_latest_sha,
    instruct_build_image,
    instruct_for_setup,
    is_repo_rebased,
    md5sum_check_if_build_is_needed,
    run_command,
)
from airflow_breeze.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE

PARAMS_TO_ENTER_SHELL = {
    "HOST_USER_ID": "host_user_id",
    "HOST_GROUP_ID": "host_group_id",
    "COMPOSE_FILE": "compose_files",
    "PYTHON_MAJOR_MINOR_VERSION": "python_version",
    "BACKEND": "backend",
    "AIRFLOW_VERSION": "airflow_version",
    "INSTALL_AIRFLOW_VERSION": "install_airflow_version",
    "AIRFLOW_SOURCES": "airflow_sources",
    "AIRFLOW_CI_IMAGE": "airflow_ci_image_name",
    "AIRFLOW_CI_IMAGE_WITH_TAG": "airflow_ci_image_name_with_tag",
    "AIRFLOW_PROD_IMAGE": "airflow_prod_image_name",
    "AIRFLOW_IMAGE_KUBERNETES": "airflow_image_kubernetes",
    "SQLITE_URL": "sqlite_url",
    "USE_AIRFLOW_VERSION": "use_airflow_version",
    "SKIP_TWINE_CHECK": "skip_twine_check",
    "USE_PACKAGES_FROM_DIST": "use_packages_from_dist",
    "EXECUTOR": "executor",
    "START_AIRFLOW": "start_airflow",
    "ENABLED_INTEGRATIONS": "enabled_integrations",
    "GITHUB_ACTIONS": "github_actions",
    "ISSUE_ID": "issue_id",
    "NUM_RUNS": "num_runs",
    "VERSION_SUFFIX_FOR_SVN": "version_suffix_for_svn",
    "VERSION_SUFFIX_FOR_PYPI": "version_suffix_for_pypi",
}

PARAMS_FOR_SHELL_CONSTANTS = {
    "SSH_PORT": SSH_PORT,
    "WEBSERVER_HOST_PORT": WEBSERVER_HOST_PORT,
    "FLOWER_HOST_PORT": FLOWER_HOST_PORT,
    "REDIS_HOST_PORT": REDIS_HOST_PORT,
    "MYSQL_HOST_PORT": MYSQL_HOST_PORT,
    "MYSQL_VERSION": MYSQL_VERSION,
    "MSSQL_HOST_PORT": MSSQL_HOST_PORT,
    "MSSQL_VERSION": MSSQL_VERSION,
    "POSTGRES_HOST_PORT": POSTGRES_HOST_PORT,
    "POSTGRES_VERSION": POSTGRES_VERSION,
}

PARAMS_IN_CACHE = {
    'python_version': 'PYTHON_MAJOR_MINOR_VERSION',
    'backend': 'BACKEND',
    'executor': 'EXECUTOR',
    'postgres_version': 'POSTGRES_VERSION',
    'mysql_version': 'MYSQL_VERSION',
    'mssql_version': 'MSSQL_VERSION',
}

DEFAULT_VALUES_FOR_PARAM = {
    'python_version': 'DEFAULT_PYTHON_MAJOR_MINOR_VERSION',
    'backend': 'DEFAULT_BACKEND',
    'executor': 'DEFAULT_EXECUTOR',
    'postgres_version': 'POSTGRES_VERSION',
    'mysql_version': 'MYSQL_VERSION',
    'mssql_version': 'MSSQL_VERSION',
}


def construct_env_variables_docker_compose_command(shell_params: ShellBuilder) -> Dict[str, str]:
    env_variables: Dict[str, str] = {}
    for param_name in PARAMS_TO_ENTER_SHELL:
        param_value = PARAMS_TO_ENTER_SHELL[param_name]
        env_variables[param_name] = str(getattr(shell_params, param_value))
    for constant_param_name in PARAMS_FOR_SHELL_CONSTANTS:
        constant_param_value = PARAMS_FOR_SHELL_CONSTANTS[constant_param_name]
        env_variables[constant_param_name] = str(constant_param_value)
    return env_variables


def build_image_if_needed_steps(verbose: bool, shell_params: ShellBuilder):
    build_needed = md5sum_check_if_build_is_needed(shell_params.md5sum_cache_dir, shell_params.the_image_type)
    if build_needed:
        try:
            user_status = inputimeout(
                prompt='\nDo you want to build image?Press y/n/q in 5 seconds\n',
                timeout=5,
            )
            if user_status == 'y':
                latest_sha = get_latest_sha(shell_params.github_repository, shell_params.airflow_branch)
                if is_repo_rebased(latest_sha):
                    build_image(
                        verbose,
                        python_version=shell_params.python_version,
                        upgrade_to_newer_dependencies="false",
                    )
                else:
                    if click.confirm(
                        "\nThis might take a lot of time, we think you should rebase first. \
                            But if you really, really want - you can do it\n"
                    ):
                        build_image(
                            verbose,
                            python_version=shell_params.python_version,
                            upgrade_to_newer_dependencies="false",
                        )
                    else:
                        console.print(
                            '\nPlease rebase your code before continuing.\
                                Check this link to know more \
                                     https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#id15\n'
                        )
                        console.print('Exiting the process')
                        sys.exit()
            elif user_status == 'n':
                instruct_build_image(shell_params.the_image_type, shell_params.python_version)
            elif user_status == 'q':
                console.print('\nQuitting the process')
                sys.exit()
            else:
                console.print('\nYou have given a wrong choice:', user_status, ' Quitting the process')
                sys.exit()
        except TimeoutOccurred:
            console.print('\nTimeout. Considering your response as No\n')
            instruct_build_image(shell_params.the_image_type, shell_params.python_version)
        except Exception:
            console.print('\nTerminating the process')
            sys.exit()


def build_image_checks(verbose: bool, shell_params: ShellBuilder):
    build_ci_image_check_cache = Path(
        BUILD_CACHE_DIR, shell_params.airflow_branch, f".built_{shell_params.python_version}"
    )
    if build_ci_image_check_cache.exists():
        console.print(f'{shell_params.the_image_type} image already built locally.')
    else:
        console.print(f'{shell_params.the_image_type} image not built locally')

    if not shell_params.force_build:
        build_image_if_needed_steps(verbose, shell_params)
    else:
        build_image(
            verbose,
            python_version=shell_params.python_version,
            upgrade_to_newer_dependencies="false",
        )

    instruct_for_setup()
    check_docker_resources(verbose, str(shell_params.airflow_sources), shell_params.airflow_ci_image_name)
    cmd = ['docker-compose', 'run', '--service-ports', '--rm', 'airflow']
    cmd_added = shell_params.command_passed
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    if cmd_added is not None:
        cmd.extend(['-c', cmd_added])
    if verbose:
        shell_params.print_badge_info()
    output = run_command(cmd, verbose=verbose, env=env_variables, text=True)
    if verbose:
        console.print(f"[blue]{output}[/]")


def get_cached_params(user_params) -> Dict:
    updated_params = dict(user_params)
    for param in PARAMS_IN_CACHE:
        if param in user_params:
            param_name = PARAMS_IN_CACHE[param]
            user_param_value = user_params[param]
            if user_param_value is not None:
                write_to_cache_file(param_name, user_param_value)
            else:
                param_value = getattr(global_constants, DEFAULT_VALUES_FOR_PARAM[param])
                _, user_param_value = check_cache_and_write_if_not_cached(param_name, param_value)
            updated_params[param] = user_param_value
    return updated_params


def build_shell(verbose, **kwargs):
    check_docker_version(verbose)
    check_docker_compose_version(verbose)
    updated_kwargs = get_cached_params(kwargs)
    if read_from_cache_file('suppress_asciiart') is None:
        console.print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file('suppress_cheatsheet') is None:
        console.print(CHEATSHEET, style=CHEATSHEET_STYLE)
    enter_shell_params = ShellBuilder(**filter_out_none(**updated_kwargs))
    build_image_checks(verbose, enter_shell_params)
