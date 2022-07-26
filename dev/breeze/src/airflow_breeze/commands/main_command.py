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
import platform
import subprocess
import sys

from airflow_breeze.configure_rich_click import click
from airflow_breeze.utils.common_options import (
    option_airflow_extras,
    option_answer,
    option_backend,
    option_db_reset,
    option_debian_version,
    option_dry_run,
    option_force_build,
    option_forward_credentials,
    option_github_repository,
    option_installation_package_format,
    option_integration,
    option_mount_sources,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_use_airflow_version,
    option_use_packages_from_dist,
    option_verbose,
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
@option_debian_version
@option_forward_credentials
@option_force_build
@option_use_airflow_version
@option_airflow_extras
@option_use_packages_from_dist
@option_installation_package_format
@option_mount_sources
@option_integration
@option_db_reset
@option_answer
@click.pass_context
def main(ctx: click.Context, **kwargs):
    from airflow_breeze.commands.developer_commands import shell

    check_for_rosetta_environment()
    check_for_python_emulation()

    if not ctx.invoked_subcommand:
        ctx.forward(shell, extra_args={})


def check_for_python_emulation():
    try:
        system_machine = subprocess.check_output(["uname", "-m"], text=True).strip()
        python_machine = platform.uname().machine
        if system_machine != python_machine:
            from airflow_breeze.utils.console import get_console

            get_console().print(
                f'\n\n[error]Your Python architecture is {python_machine} and '
                f'system architecture is {system_machine}[/]'
            )
            get_console().print(
                '[warning]This is very bad and your Python is 10x slower as it is emulated[/]'
            )
            get_console().print(
                '[warning]You likely installed your Python wrongly and you should '
                'remove it and reinstall from scratch[/]\n'
            )
            from inputimeout import inputimeout

            user_status = inputimeout(
                prompt="Are you REALLY sure you want to continue? (press y otherwise we exit in 20s) ",
                timeout=20,
            )
            if not user_status.upper() in ['Y', 'YES']:
                sys.exit(1)
    except subprocess.CalledProcessError:
        pass
    except PermissionError:
        pass


def check_for_rosetta_environment():
    if sys.platform != 'darwin':
        return
    try:
        runs_in_rosetta = subprocess.check_output(
            ["sysctl", "-n", "sysctl.proc_translated"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if runs_in_rosetta == '1':
            from airflow_breeze.utils.console import get_console

            get_console().print(
                '\n\n[error]You are starting breeze in `rosetta 2` emulated environment on Mac[/]'
            )
            get_console().print(
                '[warning]This is very bad and your Python is 10x slower as it is emulated[/]'
            )
            get_console().print(
                '[warning]You likely have wrong architecture-based IDE (PyCharm/VSCode/Intellij) that '
                'you run it on\n'
                'You should download the right architecture for your Mac (Apple Silicon or Intel)[/]\n'
            )
            from inputimeout import inputimeout

            user_status = inputimeout(
                prompt="Are you REALLY sure you want to continue? (press y otherwise we exit in 20s) ",
                timeout=20,
            )
            if not user_status.upper() in ['Y', 'YES']:
                sys.exit(1)
    except subprocess.CalledProcessError:
        pass
    except PermissionError:
        pass
