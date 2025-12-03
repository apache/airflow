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
from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from typing import TYPE_CHECKING, Any

from airflow_breeze.commands.common_options import (
    option_all_integration,
    option_answer,
    option_auth_manager,
    option_backend,
    option_builder,
    option_db_reset,
    option_docker_host,
    option_dry_run,
    option_forward_credentials,
    option_github_repository,
    option_max_time,
    option_mysql_version,
    option_postgres_version,
    option_project_name,
    option_python,
    option_standalone_dag_processor,
    option_use_uv,
    option_uv_http_timeout,
    option_verbose,
)
from airflow_breeze.configure_rich_click import click
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import remove_docker_networks, remove_docker_volumes
from airflow_breeze.utils.path_utils import AIRFLOW_HOME_PATH, BUILD_CACHE_PATH
from airflow_breeze.utils.provider_dependencies import generate_provider_dependencies_if_needed
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run

if TYPE_CHECKING:
    from click import Context


def print_deprecated(deprecated_command: str, command_to_use: str):
    get_console().print("\n[warning]" + ("#" * 80) + "\n")
    get_console().print(f"[warning]The command '{deprecated_command}' is deprecated!\n")
    get_console().print(f"Use 'breeze {command_to_use}' instead\n")
    get_console().print("[warning]" + ("#" * 80) + "\n")


def print_removed(deprecated_command: str, non_breeze_command_to_use: str, installation_notes: str | None):
    get_console().print("\n[warning]" + ("#" * 80) + "\n")
    get_console().print(f"[warning]The command '{deprecated_command}' is removed!\n")
    get_console().print(f"Use '{non_breeze_command_to_use}' instead.\n")
    if installation_notes:
        get_console().print(installation_notes)
    get_console().print("[warning]" + ("#" * 80) + "\n")


class MainGroupWithAliases(BreezeGroup):
    def get_command(self, ctx: Context, cmd_name: str):
        # Aliases for important commands moved to sub-commands or deprecated commands
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv
        if cmd_name == "static-checks":
            # version alias does not need to be deprecated. It's ok to keep it also at top level
            # even if it is not displayed in help
            print_removed(
                "static-checks",
                "prek",
                "\nYou can install prek with:\n"
                "\n[special]uv tool install prek[/]\n\n"
                "Followed by (in airflow repo):\n\n"
                "[special]prek install -f[/]\n",
            )
            sys.exit(1)
        return None


@click.group(
    cls=MainGroupWithAliases,
    invoke_without_command=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@option_answer
@option_auth_manager
@option_backend
@option_builder
@option_db_reset
@option_docker_host
@option_dry_run
@option_forward_credentials
@option_github_repository
@option_all_integration
@option_max_time
@option_mysql_version
@option_postgres_version
@option_python
@option_project_name
@option_standalone_dag_processor
@option_use_uv
@option_uv_http_timeout
@option_verbose
@click.pass_context
def main(ctx: click.Context, **kwargs: dict[str, Any]):
    from airflow_breeze.commands.developer_commands import shell

    check_for_rosetta_environment()
    check_for_python_emulation()
    generate_provider_dependencies_if_needed()

    if not ctx.invoked_subcommand:
        ctx.forward(shell, extra_args={})


def check_for_python_emulation():
    try:
        system_machine = subprocess.check_output(["uname", "-m"], text=True).strip()
        python_machine = platform.uname().machine
        if system_machine != python_machine:
            from airflow_breeze.utils.console import get_console

            get_console().print(
                f"\n\n[error]Your Python architecture is {python_machine} and "
                f"system architecture is {system_machine}[/]"
            )
            get_console().print(
                "[warning]This is very bad and your Python is 10x slower as it is emulated[/]"
            )
            get_console().print(
                "[warning]You likely installed your Python wrongly and you should "
                "remove it and reinstall from scratch[/]\n"
            )
            from inputimeout import TimeoutOccurred, inputimeout

            try:
                user_status = inputimeout(
                    prompt="Are you REALLY sure you want to continue? "
                    "(answer with y otherwise we exit in 20s)\n",
                    timeout=20,
                )
                if user_status.upper() not in ["Y", "YES"]:
                    sys.exit(1)
            except TimeoutOccurred:
                from airflow_breeze.utils.console import get_console

                get_console().print("\nNo answer, exiting...")
                sys.exit(1)
    except FileNotFoundError:
        pass
    except subprocess.CalledProcessError:
        pass
    except PermissionError:
        pass


def check_for_rosetta_environment():
    if sys.platform != "darwin" or platform.processor() == "i386":
        return

    from inputimeout import TimeoutOccurred, inputimeout

    try:
        runs_in_rosetta = subprocess.check_output(
            ["sysctl", "-n", "sysctl.proc_translated"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if runs_in_rosetta == "1":
            from airflow_breeze.utils.console import get_console

            get_console().print(
                "\n\n[error]You are starting breeze in `rosetta 2` emulated environment on Mac[/]\n"
            )
            get_console().print(
                "[warning]This is very bad and your Python is 10x slower as it is emulated[/]\n"
            )
            get_console().print(
                "You have emulated Python interpreter (Intel rather than ARM). You should check:\n\n"
                '  * Your IDE (PyCharm/VSCode/Intellij): the "About" window should show `aarch64` '
                'not `x86_64` in "Runtime version".\n'
                '  * Your python: run  "python -c '
                'import platform; print(platform.uname().machine)"). '
                "It should show `arm64` not `x86_64`.\n"
                '  * Your `brew`: run "brew config" and it should show `arm` in CPU line not `x86`.\n\n'
                "If you have mixed Intel/ARM binaries installed you should likely nuke and "
                "reinstall your development environment (including brew and Python) from scratch!\n\n"
            )

            user_status = inputimeout(
                prompt="Are you REALLY sure you want to continue? (answer with y otherwise we exit in 20s)\n",
                timeout=20,
            )
            if user_status.upper() not in ["Y", "YES"]:
                sys.exit(1)
    except TimeoutOccurred:
        get_console().print("\nNo answer, exiting...")
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
    "--all",
    is_flag=True,
    help="Also remove currently downloaded Breeze images.",
)
@option_verbose
@option_dry_run
@option_answer
def cleanup(all: bool):
    if all:
        get_console().print(
            "\n[info]Removing cache of parameters, clean up docker cache "
            "and remove locally downloaded images[/]"
        )
    else:
        get_console().print("[info]Removing cache of parameters, and cleans up docker cache[/]")
    if all:
        docker_images_command_to_execute = [
            "docker",
            "images",
            "--filter",
            "label=org.apache.airflow.image",
            "--format",
            "{{.Repository}}:{{.Tag}}",
        ]
        command_result = run_command(docker_images_command_to_execute, text=True, capture_output=True)
        images = command_result.stdout.splitlines() if command_result and command_result.stdout else []
        if images:
            get_console().print("[info]Removing images:[/]")
            for image in images:
                get_console().print(f"[info] * {image}[/]")
            get_console().print()
            docker_rmi_command_to_execute = [
                "docker",
                "rmi",
                "--force",
            ]
            docker_rmi_command_to_execute.extend(images)
            given_answer = user_confirm("Are you sure with the removal?")
            if given_answer == Answer.YES:
                run_command(docker_rmi_command_to_execute, check=False)
            elif given_answer == Answer.QUIT:
                sys.exit(0)
        else:
            get_console().print("[info]No locally downloaded images to remove[/]\n")
    get_console().print("Removing networks created by breeze")
    given_answer = user_confirm("Are you sure with the removal of docker networks created by breeze?")
    if given_answer == Answer.YES:
        remove_docker_networks()
    get_console().print("Removing volumes created by breeze")
    given_answer = user_confirm("Are you sure with the removal of docker volumes created by breeze?")
    if given_answer == Answer.YES:
        remove_docker_volumes()
    get_console().print("Pruning docker images")
    given_answer = user_confirm("Are you sure with the removal of docker images?")
    if given_answer == Answer.YES:
        system_prune_command_to_execute = ["docker", "system", "prune", "-f"]
        run_command(
            system_prune_command_to_execute,
            check=False,
        )
    elif given_answer == Answer.QUIT:
        sys.exit(0)
    get_console().print(f"Removing build cache dir {BUILD_CACHE_PATH}")
    given_answer = user_confirm("Are you sure with the removal?")
    if given_answer == Answer.YES:
        if not get_dry_run():
            shutil.rmtree(BUILD_CACHE_PATH, ignore_errors=True)
    get_console().print("Uninstalling airflow and removing configuration")
    given_answer = user_confirm("Are you sure with the uninstall / remove?")
    if given_answer == Answer.YES:
        if not get_dry_run():
            shutil.rmtree(AIRFLOW_HOME_PATH, ignore_errors=True)
            AIRFLOW_HOME_PATH.mkdir(exist_ok=True, parents=True)
            run_command(["uv", "pip", "uninstall", "apache-airflow"], check=False)
    elif given_answer == Answer.QUIT:
        sys.exit(0)

    to_be_excluded_from_deletion = (
        # dirs
        ".idea/",  # Pycharm config
        ".vscode/",  # VSCode config
        ".venv/",
        "files/",
        "logs/",
        # files
        ".bash_history",
        ".bash_aliases",
    )

    get_console().print(
        "Removing build file and git untracked files. This also removes files ignored in .gitignore.\n"
        f"The following files will not be removed: `{to_be_excluded_from_deletion}`."
    )
    given_answer = user_confirm("Are you sure with the removal of build files?")
    if given_answer == Answer.YES:
        system_prune_command_to_execute = ["git", "clean", "-fdx"]
        for excluded_object in to_be_excluded_from_deletion:
            system_prune_command_to_execute.extend(["-e", excluded_object])

        run_command(
            system_prune_command_to_execute,
            check=False,
        )
    elif given_answer == Answer.QUIT:
        sys.exit(0)
