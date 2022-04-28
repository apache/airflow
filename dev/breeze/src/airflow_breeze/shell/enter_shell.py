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
"""Command to enter container shell for Breeze."""
import subprocess
import sys
from pathlib import Path
from typing import Dict, Union

from airflow_breeze import global_constants
from airflow_breeze.build_image.ci.build_ci_image import build_ci_image
from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.cache import (
    read_and_validate_value_from_cache,
    read_from_cache_file,
    write_to_cache_file,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    SOURCE_OF_DEFAULT_VALUES_FOR_VARIABLES,
    VARIABLES_IN_CACHE,
    check_docker_compose_version,
    check_docker_is_running,
    check_docker_resources,
    check_docker_version,
    construct_env_variables_docker_compose_command,
)
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR
from airflow_breeze.utils.run_utils import filter_out_none, run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE


def synchronize_cached_params(parameters_passed_by_the_user: Dict[str, str]) -> Dict[str, str]:
    """
    Synchronizes cached params with arguments passed via dictionary.

    It will read from cache parameters that are missing and writes back in case user
    actually provided new values for those parameters. It synchronizes all cacheable parameters.

    :param parameters_passed_by_the_user: user args passed
    :return: updated args
    """
    updated_params = dict(parameters_passed_by_the_user)
    for param in VARIABLES_IN_CACHE:
        if param in parameters_passed_by_the_user:
            param_name = VARIABLES_IN_CACHE[param]
            user_param_value = parameters_passed_by_the_user[param]
            if user_param_value is not None:
                write_to_cache_file(param_name, user_param_value)
            else:
                param_value = getattr(global_constants, SOURCE_OF_DEFAULT_VALUES_FOR_VARIABLES[param])
                _, user_param_value = read_and_validate_value_from_cache(param_name, param_value)
            updated_params[param] = user_param_value
    return updated_params


def enter_shell(**kwargs) -> Union[subprocess.CompletedProcess, subprocess.CalledProcessError]:
    """
    Executes entering shell using the parameters passed as kwargs:

    * checks if docker version is good
    * checks if docker-compose version is good
    * updates kwargs with cached parameters
    * displays ASCIIART and CHEATSHEET unless disabled
    * build ShellParams from the updated kwargs
    * executes the command to drop the user to Breeze shell

    """
    verbose = kwargs['verbose']
    dry_run = kwargs['dry_run']
    if not check_docker_is_running(verbose):
        get_console().print(
            '[error]Docker is not running.[/]\n'
            '[warning]Please make sure Docker is installed and running.[/]'
        )
        sys.exit(1)
    check_docker_version(verbose)
    check_docker_compose_version(verbose)
    updated_kwargs = synchronize_cached_params(kwargs)
    if read_from_cache_file('suppress_asciiart') is None:
        get_console().print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file('suppress_cheatsheet') is None:
        get_console().print(CHEATSHEET, style=CHEATSHEET_STYLE)
    enter_shell_params = ShellParams(**filter_out_none(**updated_kwargs))
    return run_shell_with_build_image_checks(verbose, dry_run, enter_shell_params)


def run_shell_with_build_image_checks(
    verbose: bool, dry_run: bool, shell_params: ShellParams
) -> Union[subprocess.CompletedProcess, subprocess.CalledProcessError]:
    """
    Executes a shell command built from params passed, checking if build is not needed.
    * checks if there are enough resources to run shell
    * checks if image was built at least once (if not - forces the build)
    * if not forces, checks if build is needed and asks the user if so
    * builds the image if needed
    * prints information about the build
    * constructs docker compose command to enter shell
    * executes it

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param shell_params: parameters of the execution
    """
    check_docker_resources(verbose, shell_params.airflow_image_name, dry_run=dry_run)
    build_ci_image_check_cache = Path(
        BUILD_CACHE_DIR, shell_params.airflow_branch, f".built_{shell_params.python}"
    )
    ci_image_params = BuildCiParams(python=shell_params.python, upgrade_to_newer_dependencies="false")
    if build_ci_image_check_cache.exists():
        get_console().print(f'[info]{shell_params.the_image_type} image already built locally.[/]')
    else:
        get_console().print(
            f'[warning]{shell_params.the_image_type} image not built locally. Forcing build.[/]'
        )
        ci_image_params.force_build = True

    build_ci_image(verbose, dry_run=dry_run, with_ci_group=False, ci_image_params=ci_image_params)
    shell_params.print_badge_info()
    cmd = ['docker-compose', 'run', '--service-ports', "-e", "BREEZE", '--rm', 'airflow']
    cmd_added = shell_params.command_passed
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    if cmd_added is not None:
        cmd.extend(['-c', cmd_added])
    return run_command(cmd, verbose=verbose, dry_run=dry_run, env=env_variables, text=True)
