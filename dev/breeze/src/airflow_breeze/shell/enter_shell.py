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
import sys
from pathlib import Path
from typing import Dict

from airflow_breeze import global_constants
from airflow_breeze.build_image.ci.build_ci_image import build_image
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.cache import (
    check_cached_value_is_allowed,
    read_from_cache_file,
    write_to_cache_file,
)
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import console
from airflow_breeze.utils.docker_command_utils import (
    SOURCE_OF_DEFAULT_VALUES_FOR_VARIABLES,
    VARIABLES_IN_CACHE,
    check_docker_compose_version,
    check_docker_is_running,
    check_docker_resources,
    check_docker_version,
    construct_env_variables_docker_compose_command,
)
from airflow_breeze.utils.md5_build_check import md5sum_check_if_build_is_needed
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR
from airflow_breeze.utils.run_utils import filter_out_none, instruct_build_image, is_repo_rebased, run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE, CHEATSHEET, CHEATSHEET_STYLE


def build_image_if_needed_steps(verbose: bool, dry_run: bool, shell_params: ShellParams) -> None:
    """
    Check if image build is needed based on what files have been modified since last build.

    * If build is needed, the user is asked for confirmation
    * If the branch is not rebased it warns the user to rebase (to make sure latest remote cache is useful)
    * Builds Image/Skips/Quits depending on the answer

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param shell_params: parameters for the build
    """
    # We import those locally so that click autocomplete works
    from inputimeout import TimeoutOccurred

    build_needed = md5sum_check_if_build_is_needed(shell_params.md5sum_cache_dir, shell_params.the_image_type)
    if not build_needed:
        return
    try:
        answer = user_confirm(message="Do you want to build image?", timeout=5, default_answer=Answer.NO)
        if answer == answer.YES:
            if is_repo_rebased(shell_params.github_repository, shell_params.airflow_branch):
                build_image(
                    verbose,
                    dry_run=dry_run,
                    python=shell_params.python,
                    upgrade_to_newer_dependencies="false",
                )
            else:
                console.print(
                    "\n[bright_yellow]This might take a lot of time, w"
                    "e think you should rebase first.[/]\n"
                )
                answer = user_confirm(
                    "But if you really, really want - you can do it", timeout=5, default_answer=Answer.NO
                )
                if answer == Answer.YES:
                    build_image(
                        verbose=verbose,
                        dry_run=dry_run,
                        python=shell_params.python,
                        upgrade_to_newer_dependencies="false",
                    )
                else:
                    console.print(
                        "[bright_blue]Please rebase your code before continuing.[/]\n"
                        "Check this link to know more "
                        "https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#id15\n"
                    )
                    console.print('[red]Exiting the process[/]\n')
                    sys.exit(1)
        elif answer == Answer.NO:
            instruct_build_image(shell_params.python)
        else:  # users_status == Answer.QUIT:
            console.print('\n[bright_yellow]Quitting the process[/]\n')
            sys.exit()
    except TimeoutOccurred:
        console.print('\nTimeout. Considering your response as No\n')
        instruct_build_image(shell_params.python)
    except Exception as e:
        console.print(f'\nTerminating the process on {e}')
        sys.exit(1)


def run_shell_with_build_image_checks(verbose: bool, dry_run: bool, shell_params: ShellParams):
    """
    Executes shell command built from params passed, checking if build is not needed.
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
    check_docker_resources(verbose, shell_params.airflow_image_name)
    build_ci_image_check_cache = Path(
        BUILD_CACHE_DIR, shell_params.airflow_branch, f".built_{shell_params.python}"
    )
    if build_ci_image_check_cache.exists():
        console.print(f'[bright_blue]{shell_params.the_image_type} image already built locally.[/]')
    else:
        console.print(
            f'[bright_yellow]{shell_params.the_image_type} image not built locally. ' f'Forcing build.[/]'
        )
        shell_params.force_build = True

    if not shell_params.force_build:
        build_image_if_needed_steps(verbose, dry_run, shell_params)
    else:
        build_image(
            verbose,
            dry_run=dry_run,
            python=shell_params.python,
            upgrade_to_newer_dependencies="false",
        )
    shell_params.print_badge_info()
    cmd = ['docker-compose', 'run', '--service-ports', "-e", "BREEZE", '--rm', 'airflow']
    cmd_added = shell_params.command_passed
    env_variables = construct_env_variables_docker_compose_command(shell_params)
    if cmd_added is not None:
        cmd.extend(['-c', cmd_added])
    run_command(cmd, verbose=verbose, dry_run=dry_run, env=env_variables, text=True)


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
                _, user_param_value = check_cached_value_is_allowed(param_name, param_value)
            updated_params[param] = user_param_value
    return updated_params


def enter_shell(**kwargs):
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
        console.print(
            '[red]Docker is not running.[/]\n'
            '[bright_yellow]Please make sure Docker is installed and running.[/]'
        )
        sys.exit(1)
    check_docker_version(verbose)
    check_docker_compose_version(verbose)
    updated_kwargs = synchronize_cached_params(kwargs)
    if read_from_cache_file('suppress_asciiart') is None:
        console.print(ASCIIART, style=ASCIIART_STYLE)
    if read_from_cache_file('suppress_cheatsheet') is None:
        console.print(CHEATSHEET, style=CHEATSHEET_STYLE)
    enter_shell_params = ShellParams(**filter_out_none(**updated_kwargs))
    run_shell_with_build_image_checks(verbose, dry_run, enter_shell_params)
