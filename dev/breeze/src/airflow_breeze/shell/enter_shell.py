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
from typing import Optional, Union

from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.cache import read_from_cache_file
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    check_docker_compose_version,
    check_docker_is_running,
    check_docker_resources,
    check_docker_version,
    construct_env_variables_docker_compose_command,
)
from airflow_breeze.utils.rebuild_image_if_needed import rebuild_ci_image_if_needed
from airflow_breeze.utils.run_utils import filter_out_none, run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE


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
    check_docker_is_running(verbose)
    check_docker_version(verbose)
    check_docker_compose_version(verbose)
    if read_from_cache_file('suppress_asciiart') is None:
        get_console().print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file('suppress_cheatsheet') is None:
        get_console().print(CHEATSHEET, style=CHEATSHEET_STYLE)
    enter_shell_params = ShellParams(**filter_out_none(**kwargs))
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
    rebuild_ci_image_if_needed(build_params=shell_params, dry_run=dry_run, verbose=verbose)
    shell_params.print_badge_info()
    cmd = ['docker-compose', 'run', '--service-ports', "-e", "BREEZE", '--rm', 'airflow']
    cmd_added = shell_params.command_passed
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    if cmd_added is not None:
        cmd.extend(['-c', cmd_added])

    command_result = run_command(
        cmd, verbose=verbose, dry_run=dry_run, env=env_variables, text=True, check=False
    )
    if command_result.returncode == 0:
        return command_result
    else:
        get_console().print(f"[red]Error {command_result.returncode} returned[/]")
        if verbose:
            get_console().print(command_result.stderr)
        return command_result


def stop_exec_on_error(returncode: int):
    get_console().print('\n[error]ERROR in finding the airflow docker-compose process id[/]\n')
    sys.exit(returncode)


def find_airflow_container(verbose, dry_run) -> Optional[str]:
    exec_shell_params = ShellParams(verbose=verbose, dry_run=dry_run)
    check_docker_resources(exec_shell_params.airflow_image_name, verbose=verbose, dry_run=dry_run)
    exec_shell_params.print_badge_info()
    env_variables = construct_env_variables_docker_compose_command(exec_shell_params)
    cmd = ['docker-compose', 'ps', '--all', '--filter', 'status=running', 'airflow']
    docker_compose_ps_command = run_command(
        cmd, verbose=verbose, dry_run=dry_run, text=True, capture_output=True, env=env_variables, check=False
    )
    if dry_run:
        return "CONTAINER_ID"
    if docker_compose_ps_command.returncode != 0:
        if verbose:
            get_console().print(docker_compose_ps_command.stdout)
            get_console().print(docker_compose_ps_command.stderr)
        stop_exec_on_error(docker_compose_ps_command.returncode)
        return None

    output = docker_compose_ps_command.stdout
    container_info = output.strip().split('\n')
    if container_info:
        container_running = container_info[-1].split(' ')[0]
        if container_running.startswith('-'):
            # On docker-compose v1 we get '--------' as output here
            stop_exec_on_error(docker_compose_ps_command.returncode)
        return container_running
    else:
        stop_exec_on_error(1)
        return None
