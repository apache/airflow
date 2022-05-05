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
import sys
from typing import Tuple

import click

from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.commands.common_options import (
    option_db_reset,
    option_dry_run,
    option_github_repository,
    option_image_name,
    option_image_tag,
    option_integration,
    option_python,
    option_verbose,
)
from airflow_breeze.commands.custom_param_types import BetterChoice
from airflow_breeze.commands.main import main
from airflow_breeze.global_constants import ALLOWED_TEST_TYPES
from airflow_breeze.shell.enter_shell import check_docker_is_running, check_docker_resources
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import construct_env_variables_docker_compose_command
from airflow_breeze.utils.run_tests import run_docker_compose_tests
from airflow_breeze.utils.run_utils import run_command

TESTING_COMMANDS = {
    "name": "Testing",
    "commands": ["docker-compose-tests", "tests"],
}

TESTING_PARAMETERS = {
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
    "breeze tests": [
        {
            "name": "Basic flag for tests command",
            "options": [
                "--integration",
                "--test-type",
                "--db-reset",
            ],
        }
    ],
}


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
        build_params = BuildProdParams(
            python=python, image_tag=image_tag, github_repository=github_repository
        )
        image_name = build_params.airflow_image_name_with_tag
    get_console().print(f"[info]Running docker-compose with PROD image: {image_name}[/]")
    return_code, info = run_docker_compose_tests(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)


@main.command(
    name='tests',
    help="Run the specified unit test targets. Multiple targets may be specified separated by spaces.",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_dry_run
@option_verbose
@option_integration
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
@click.option(
    "-tt",
    "--test-type",
    help="Type of test to run.",
    default="All",
    type=BetterChoice(ALLOWED_TEST_TYPES),
)
@option_db_reset
def tests(
    dry_run: bool,
    verbose: bool,
    integration: Tuple,
    extra_pytest_args: Tuple,
    test_type: str,
    db_reset: bool,
):
    os.environ["RUN_TESTS"] = "true"
    if test_type:
        os.environ["TEST_TYPE"] = test_type
    if integration:
        if "trino" in integration:
            integration = integration + ("kerberos",)
        os.environ["LIST_OF_INTEGRATION_TESTS_TO_RUN"] = ' '.join(list(integration))
    if db_reset:
        os.environ["DB_RESET"] = "true"

    exec_shell_params = ShellParams(verbose=verbose, dry_run=dry_run)
    env_variables = construct_env_variables_docker_compose_command(exec_shell_params)
    check_docker_is_running(verbose)
    check_docker_resources(exec_shell_params.airflow_image_name, verbose=verbose, dry_run=dry_run)

    cmd = ['docker-compose', 'run', '--service-ports', '--rm', 'airflow']
    cmd.extend(list(extra_pytest_args))
    run_command(
        cmd,
        verbose=verbose,
        dry_run=dry_run,
        env=env_variables,
    )
