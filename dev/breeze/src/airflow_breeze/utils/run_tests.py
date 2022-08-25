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
from subprocess import DEVNULL
from typing import Optional, Tuple

from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_utils import run_command


def verify_an_image(
    image_name: str,
    image_type: str,
    output: Optional[Output],
    dry_run: bool,
    verbose: bool,
    slim_image: bool,
    extra_pytest_args: Tuple,
) -> Tuple[int, str]:
    command_result = run_command(
        ["docker", "inspect", image_name],
        dry_run=dry_run,
        verbose=verbose,
        check=False,
        output=output,
    )
    if command_result.returncode != 0:
        get_console(output=output).print(
            f"[error]Error when inspecting {image_type} image: {command_result.returncode}[/]"
        )
        return command_result.returncode, f"Testing {image_type} python {image_name}"
    pytest_args = ("-n", str(os.cpu_count()), "--color=yes")
    if image_type == 'PROD':
        test_path = AIRFLOW_SOURCES_ROOT / "docker_tests" / "test_prod_image.py"
    else:
        test_path = AIRFLOW_SOURCES_ROOT / "docker_tests" / "test_ci_image.py"
    env = os.environ.copy()
    env['DOCKER_IMAGE'] = image_name
    if slim_image:
        env['TEST_SLIM_IMAGE'] = 'true'
    command_result = run_command(
        [sys.executable, "-m", "pytest", str(test_path), *pytest_args, *extra_pytest_args],
        dry_run=dry_run,
        verbose=verbose,
        env=env,
        output=output,
        check=False,
    )
    return command_result.returncode, f"Testing {image_type} python {image_name}"


def run_docker_compose_tests(
    image_name: str, dry_run: bool, verbose: bool, extra_pytest_args: Tuple
) -> Tuple[int, str]:
    command_result = run_command(
        ["docker", "inspect", image_name], dry_run=dry_run, verbose=verbose, check=False, stdout=DEVNULL
    )
    if command_result.returncode != 0:
        get_console().print(f"[error]Error when inspecting PROD image: {command_result.returncode}[/]")
        return command_result.returncode, f"Testing docker-compose python with {image_name}"
    pytest_args = ("-n", str(os.cpu_count()), "--color=yes")
    test_path = AIRFLOW_SOURCES_ROOT / "docker_tests" / "test_docker_compose_quick_start.py"
    env = os.environ.copy()
    env['DOCKER_IMAGE'] = image_name
    command_result = run_command(
        [sys.executable, "-m", "pytest", str(test_path), *pytest_args, *extra_pytest_args],
        dry_run=dry_run,
        verbose=verbose,
        env=env,
        check=False,
    )
    return command_result.returncode, f"Testing docker-compose python with {image_name}"
