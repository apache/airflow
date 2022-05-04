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
from typing import Tuple

import click

from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.commands.common_options import (
    option_dry_run,
    option_github_repository,
    option_image_name,
    option_image_tag,
    option_python,
    option_verbose,
)
from airflow_breeze.commands.main import main
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_tests import run_docker_compose_tests

TESTING_COMMANDS = {
    "name": "Testing",
    "commands": [
        "docker-compose-tests",
    ],
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
