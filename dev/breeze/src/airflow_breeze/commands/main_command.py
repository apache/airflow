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
import shutil
import subprocess
import sys
from typing import Optional

from click import Context

from airflow_breeze.commands.ci_image_commands import ci_image
from airflow_breeze.commands.production_image_commands import prod_image
from airflow_breeze.commands.testing_commands import testing
from airflow_breeze.configure_rich_click import click
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_answer,
    option_backend,
    option_db_reset,
    option_dry_run,
    option_forward_credentials,
    option_github_repository,
    option_integration,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_verbose,
)
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import BUILD_CACHE_DIR
from airflow_breeze.utils.run_utils import run_command


def print_deprecated(deprecated_command: str, command_to_use: str):
    get_console().print("[yellow]" + ("#" * 80) + "\n")
    get_console().print(f"[yellow]The command '{deprecated_command}' is deprecated!\n")
    get_console().print(f"Use 'breeze {command_to_use}' instead\n")
    get_console().print("[yellow]" + ("#" * 80) + "\n")


class MainGroupWithAliases(BreezeGroup):
    def get_command(self, ctx: Context, cmd_name: str):
        # Aliases for important commands moved to sub-commands
        from airflow_breeze.commands.setup_commands import setup

        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        if cmd_name == 'build-image':
            print_deprecated('build-image', 'ci-image build')
            return ci_image.get_command(ctx, 'build')
        if cmd_name == 'build-prod-image':
            print_deprecated('build-prod-image', 'prod-image build')
            return prod_image.get_command(ctx, 'build')
        if cmd_name == 'tests':
            print_deprecated('tests', 'testing tests')
            return testing.get_command(ctx, 'tests')
        if cmd_name == 'config':
            print_deprecated('config', 'setup config')
            return setup.get_command(ctx, 'config')
        if cmd_name == 'setup-autocomplete':
            print_deprecated('setup-autocomplete', 'setup autocomplete')
            return setup.get_command(ctx, 'autocomplete')
        if cmd_name == 'version':
            # version alias does not need to be deprecated. It's ok to keep it also at top level
            # even if it is not displayed in help
            return setup.get_command(ctx, 'version')
        return None


@click.group(
    cls=MainGroupWithAliases,
    invoke_without_command=True,
    context_settings={'help_option_names': ['-h', '--help']},
)
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@option_integration
@option_forward_credentials
@option_db_reset
@option_verbose
@option_dry_run
@option_github_repository
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


@main.command(
    name="cleanup",
    help="Cleans the cache of parameters, docker cache and optionally built CI/PROD images.",
)
@click.option(
    '--all',
    is_flag=True,
    help='Also remove currently downloaded Breeze images.',
)
@option_verbose
@option_answer
@option_dry_run
@option_github_repository
def cleanup(verbose: bool, dry_run: bool, github_repository: str, all: bool, answer: Optional[str]):
    if all:
        get_console().print(
            "\n[info]Removing cache of parameters, clean up docker cache "
            "and remove locally downloaded images[/]"
        )
    else:
        get_console().print("[info]Removing cache of parameters, and cleans up docker cache[/]")
    if all:
        docker_images_command_to_execute = [
            'docker',
            'images',
            '--filter',
            'label=org.apache.airflow.image',
            '--format',
            '{{.Repository}}:{{.Tag}}',
        ]
        command_result = run_command(
            docker_images_command_to_execute, verbose=verbose, text=True, capture_output=True
        )
        images = command_result.stdout.splitlines() if command_result and command_result.stdout else []
        if images:
            get_console().print("[info]Removing images:[/]")
            for image in images:
                get_console().print(f"[info] * {image}[/]")
            get_console().print()
            docker_rmi_command_to_execute = [
                'docker',
                'rmi',
                '--force',
            ]
            docker_rmi_command_to_execute.extend(images)
            given_answer = user_confirm("Are you sure with the removal?")
            if given_answer == Answer.YES:
                run_command(docker_rmi_command_to_execute, verbose=verbose, dry_run=dry_run, check=False)
            elif given_answer == Answer.QUIT:
                sys.exit(0)
        else:
            get_console().print("[info]No locally downloaded images to remove[/]\n")
    get_console().print("Pruning docker images")
    given_answer = user_confirm("Are you sure with the removal?")
    if given_answer == Answer.YES:
        system_prune_command_to_execute = ['docker', 'system', 'prune']
        run_command(
            system_prune_command_to_execute,
            verbose=verbose,
            dry_run=dry_run,
            check=False,
            enabled_output_group=True,
        )
    elif given_answer == Answer.QUIT:
        sys.exit(0)
    get_console().print(f"Removing build cache dir ${BUILD_CACHE_DIR}")
    given_answer = user_confirm("Are you sure with the removal?")
    if given_answer == Answer.YES:
        if not dry_run:
            shutil.rmtree(BUILD_CACHE_DIR, ignore_errors=True)
    elif given_answer == Answer.QUIT:
        sys.exit(0)
