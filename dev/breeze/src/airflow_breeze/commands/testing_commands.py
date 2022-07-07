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
import errno
import os
import re
import shutil
import subprocess
import sys
import tempfile
from threading import Event, Thread
from time import sleep
from typing import Dict, List, Optional, Tuple

import click

from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import ALLOWED_TEST_TYPE_CHOICES
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.common_options import (
    option_backend,
    option_db_reset,
    option_dry_run,
    option_github_repository,
    option_image_name,
    option_image_tag_for_running,
    option_integration,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_verbose,
)
from airflow_breeze.utils.console import get_console, message_type_from_return_code
from airflow_breeze.utils.custom_param_types import NotVerifiedBetterChoice
from airflow_breeze.utils.docker_command_utils import (
    get_env_variables_for_docker_commands,
    perform_environment_checks,
)
from airflow_breeze.utils.run_tests import run_docker_compose_tests
from airflow_breeze.utils.run_utils import RunCommandResult, run_command

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
                "--image-tag",
                "--python",
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
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
            ],
        },
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--limit-progress-output",
                "--image-tag",
                "--mount-sources",
            ],
        },
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
@option_image_tag_for_running
@option_image_name
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def docker_compose_tests(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: Optional[str],
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


class MonitoringThread(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, title: str, file_name: str):
        super().__init__(target=self.peek_percent_at_last_lines_of_file, daemon=True)
        self._stop_event = Event()
        self.title = title
        self.file_name = file_name

    def peek_percent_at_last_lines_of_file(self) -> None:
        max_line_length = 400
        matcher = re.compile(r"^.*\[([^\]]*)\]$")
        while not self.stopped():
            if os.path.exists(self.file_name):
                try:
                    with open(self.file_name, 'rb') as temp_f:
                        temp_f.seek(-(max_line_length * 2), os.SEEK_END)
                        tail = temp_f.read().decode()
                    try:
                        two_last_lines = tail.splitlines()[-2:]
                        previous_no_ansi_line = escape_ansi(two_last_lines[0])
                        m = matcher.match(previous_no_ansi_line)
                        if m:
                            get_console().print(f"[info]{self.title}:[/] {m.group(1).strip()}")
                            print(f"\r{two_last_lines[0]}\r")
                            print(f"\r{two_last_lines[1]}\r")
                    except IndexError:
                        pass
                except OSError as e:
                    if e.errno == errno.EINVAL:
                        pass
                    else:
                        raise
            sleep(5)

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()


def escape_ansi(line):
    ansi_escape = re.compile(r'(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]')
    return ansi_escape.sub('', line)


def run_with_progress(
    cmd: List[str],
    env_variables: Dict[str, str],
    test_type: str,
    python: str,
    backend: str,
    version: str,
    verbose: bool,
    dry_run: bool,
) -> RunCommandResult:
    title = f"Running tests: {test_type}, Python: {python}, Backend: {backend}:{version}"
    try:
        with tempfile.NamedTemporaryFile(mode='w+t', delete=False) as f:
            get_console().print(f"[info]Starting test = {title}[/]")
            thread = MonitoringThread(title=title, file_name=f.name)
            thread.start()
            try:
                result = run_command(
                    cmd,
                    verbose=verbose,
                    dry_run=dry_run,
                    env=env_variables,
                    check=False,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                )
            finally:
                thread.stop()
                thread.join()
        with ci_group(f"Result of {title}", message_type=message_type_from_return_code(result.returncode)):
            with open(f.name) as f:
                shutil.copyfileobj(f, sys.stdout)
    finally:
        os.unlink(f.name)
    return result


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
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_integration
@click.option(
    '--limit-progress-output',
    help="Limit progress to percentage only and just show the summary when tests complete.",
    is_flag=True,
)
@option_image_tag_for_running
@option_mount_sources
@click.option(
    "--test-type",
    help="Type of test to run. Note that with Providers, you can also specify which provider "
    "tests should be run - for example --test-type \"Providers[airbyte,http]\"",
    default="All",
    type=NotVerifiedBetterChoice(ALLOWED_TEST_TYPE_CHOICES),
)
@option_db_reset
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def tests(
    dry_run: bool,
    verbose: bool,
    python: str,
    backend: str,
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    limit_progress_output: bool,
    integration: Tuple,
    extra_pytest_args: Tuple,
    test_type: str,
    db_reset: bool,
    image_tag: Optional[str],
    mount_sources: str,
):
    os.environ["RUN_TESTS"] = "true"
    if test_type:
        os.environ["TEST_TYPE"] = test_type
        if "[" in test_type and not test_type.startswith("Providers"):
            get_console().print("[error]Only 'Providers' test type can specify actual tests with \\[\\][/]")
            sys.exit(1)
    if integration:
        if "trino" in integration:
            integration = integration + ("kerberos",)
        os.environ["LIST_OF_INTEGRATION_TESTS_TO_RUN"] = ' '.join(list(integration))
    if db_reset:
        os.environ["DB_RESET"] = "true"
    exec_shell_params = ShellParams(
        verbose=verbose,
        dry_run=dry_run,
        python=python,
        backend=backend,
        postgres_version=postgres_version,
        mysql_version=mysql_version,
        mssql_version=mssql_version,
        image_tag=image_tag,
        mount_sources=mount_sources,
    )
    env_variables = get_env_variables_for_docker_commands(exec_shell_params)
    perform_environment_checks(verbose=verbose)
    cmd = ['docker-compose', 'run', '--service-ports', '--rm', 'airflow']
    cmd.extend(list(extra_pytest_args))
    version = (
        mssql_version
        if backend == "mssql"
        else mysql_version
        if backend == "mysql"
        else postgres_version
        if backend == "postgres"
        else "none"
    )
    if limit_progress_output:
        result = run_with_progress(
            cmd=cmd,
            env_variables=env_variables,
            test_type=test_type,
            python=python,
            backend=backend,
            version=version,
            verbose=verbose,
            dry_run=dry_run,
        )
    else:
        result = run_command(cmd, verbose=verbose, dry_run=dry_run, env=env_variables, check=False)
    sys.exit(result.returncode)
