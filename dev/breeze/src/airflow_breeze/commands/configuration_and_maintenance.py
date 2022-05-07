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
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import click

from airflow_breeze import NAME, VERSION
from airflow_breeze.commands.common_options import (
    option_answer,
    option_backend,
    option_dry_run,
    option_github_repository,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_verbose,
)
from airflow_breeze.commands.main import main
from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION, MOUNT_ALL
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.cache import check_if_cache_exists, delete_cache, touch_cache_file
from airflow_breeze.utils.confirm import STANDARD_TIMEOUT, Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    check_docker_resources,
    get_env_variables_for_docker_commands,
    get_extra_docker_flags,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    BUILD_CACHE_DIR,
    get_installation_airflow_sources,
    get_installation_sources_config_metadata_hash,
    get_package_setup_metadata_hash,
    get_used_airflow_sources,
    get_used_sources_setup_metadata_hash,
)
from airflow_breeze.utils.recording import output_file_for_recording
from airflow_breeze.utils.reinstall import ask_to_reinstall_breeze, reinstall_breeze, warn_non_editable
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE

CONFIGURATION_AND_MAINTENANCE_COMMANDS = {
    "name": "Configuration & maintenance",
    "commands": [
        "cleanup",
        "self-upgrade",
        "setup-autocomplete",
        "config",
        "resource-check",
        "free-space",
        "fix-ownership",
        "version",
    ],
}

CONFIGURATION_AND_MAINTENANCE_PARAMETERS = {
    "breeze cleanup": [
        {
            "name": "Cleanup flags",
            "options": [
                "--all",
            ],
        },
    ],
    "breeze self-upgrade": [
        {
            "name": "Self-upgrade flags",
            "options": [
                "--use-current-airflow-sources",
                "--force",
            ],
        }
    ],
    "breeze setup-autocomplete": [
        {
            "name": "Setup autocomplete flags",
            "options": [
                "--force",
            ],
        },
    ],
    "breeze config": [
        {
            "name": "Config flags",
            "options": [
                "--python",
                "--backend",
                "--cheatsheet",
                "--asciiart",
                "--colour",
            ],
        },
    ],
}


@main.command(
    name="cleanup",
    help="Cleans the cache of parameters, docker cache and optionally - currently downloaded images.",
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
            get_console().print("[light_blue]Removing images:[/]")
            for image in images:
                get_console().print(f"[light_blue] * {image}[/]")
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
            get_console().print("[light_blue]No locally downloaded images to remove[/]\n")
    get_console().print("Pruning docker images")
    given_answer = user_confirm("Are you sure with the removal?")
    if given_answer == Answer.YES:
        system_prune_command_to_execute = ['docker', 'system', 'prune']
        run_command(system_prune_command_to_execute, verbose=verbose, dry_run=dry_run, check=False)
    elif given_answer == Answer.QUIT:
        sys.exit(0)
    get_console().print(f"Removing build cache dir ${BUILD_CACHE_DIR}")
    given_answer = user_confirm("Are you sure with the removal?")
    if given_answer == Answer.YES:
        if not dry_run:
            shutil.rmtree(BUILD_CACHE_DIR, ignore_errors=True)
    elif given_answer == Answer.QUIT:
        sys.exit(0)


@click.option(
    '-f',
    '--force',
    is_flag=True,
    help='Force upgrade without asking question to the user.',
)
@click.option(
    '-a',
    '--use-current-airflow-sources',
    is_flag=True,
    help='Use current workdir Airflow sources for upgrade'
    + (f" rather than from {get_installation_airflow_sources()}." if not output_file_for_recording else "."),
)
@main.command(
    name='self-upgrade',
    help="Self upgrade Breeze. By default it re-installs Breeze "
    f"from {get_installation_airflow_sources()}."
    if not output_file_for_recording
    else "Self upgrade Breeze.",
)
def self_upgrade(force: bool, use_current_airflow_sources: bool):
    if use_current_airflow_sources:
        airflow_sources: Optional[Path] = get_used_airflow_sources()
    else:
        airflow_sources = get_installation_airflow_sources()
    if airflow_sources is not None:
        breeze_sources = airflow_sources / "dev" / "breeze"
        if force:
            reinstall_breeze(breeze_sources)
        else:
            ask_to_reinstall_breeze(breeze_sources, timeout=None)
    else:
        warn_non_editable()
        sys.exit(1)


@option_verbose
@option_dry_run
@click.option(
    '-f',
    '--force',
    is_flag=True,
    help='Force autocomplete setup even if already setup before (overrides the setup).',
)
@option_answer
@main.command(name='setup-autocomplete')
def setup_autocomplete(verbose: bool, dry_run: bool, force: bool, answer: Optional[str]):
    """
    Enables autocompletion of breeze commands.
    """
    # Determine if the shell is bash/zsh/powershell. It helps to build the autocomplete path
    detected_shell = os.environ.get('SHELL')
    detected_shell = None if detected_shell is None else detected_shell.split(os.sep)[-1]
    if detected_shell not in ['bash', 'zsh', 'fish']:
        get_console().print(f"\n[error] The shell {detected_shell} is not supported for autocomplete![/]\n")
        sys.exit(1)
    get_console().print(f"Installing {detected_shell} completion for local user")
    autocomplete_path = (
        AIRFLOW_SOURCES_ROOT / "dev" / "breeze" / "autocomplete" / f"{NAME}-complete-{detected_shell}.sh"
    )
    get_console().print(f"[info]Activation command script is available here: {autocomplete_path}[/]\n")
    get_console().print(f"[warning]We need to add above script to your {detected_shell} profile.[/]\n")
    given_answer = user_confirm(
        "Should we proceed with modifying the script?", default_answer=Answer.NO, timeout=STANDARD_TIMEOUT
    )
    if given_answer == Answer.YES:
        if detected_shell == 'bash':
            script_path = str(Path('~').expanduser() / '.bash_completion')
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force)
        elif detected_shell == 'zsh':
            script_path = str(Path('~').expanduser() / '.zshrc')
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force)
        elif detected_shell == 'fish':
            # Include steps for fish shell
            script_path = str(Path('~').expanduser() / f'.config/fish/completions/{NAME}.fish')
            if os.path.exists(script_path) and not force:
                get_console().print(
                    "\n[warning]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force/]\n"
                )
            else:
                with open(autocomplete_path) as source_file, open(script_path, 'w') as destination_file:
                    for line in source_file:
                        destination_file.write(line)
        else:
            # Include steps for powershell
            subprocess.check_call(['powershell', 'Set-ExecutionPolicy Unrestricted -Scope CurrentUser'])
            script_path = (
                subprocess.check_output(['powershell', '-NoProfile', 'echo $profile']).decode("utf-8").strip()
            )
            command_to_execute = f". {autocomplete_path}"
            write_to_shell(command_to_execute, dry_run, script_path, force)
    elif given_answer == Answer.NO:
        get_console().print(
            "\nPlease follow the https://click.palletsprojects.com/en/8.1.x/shell-completion/ "
            "to setup autocompletion for breeze manually if you want to use it.\n"
        )
    else:
        sys.exit(0)


@option_verbose
@main.command()
def version(verbose: bool, python: str):
    """Print information about version of apache-airflow-breeze."""

    get_console().print(ASCIIART, style=ASCIIART_STYLE)
    get_console().print(f"\n[info]Breeze version: {VERSION}[/]")
    get_console().print(f"[info]Breeze installed from: {get_installation_airflow_sources()}[/]")
    get_console().print(f"[info]Used Airflow sources : {get_used_airflow_sources()}[/]\n")
    if verbose:
        get_console().print(
            f"[info]Installation sources config hash : "
            f"{get_installation_sources_config_metadata_hash()}[/]"
        )
        get_console().print(
            f"[info]Used sources config hash         : " f"{get_used_sources_setup_metadata_hash()}[/]"
        )
        get_console().print(
            f"[info]Package config hash              : " f"{(get_package_setup_metadata_hash())}[/]\n"
        )


@main.command(name='config')
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@click.option('-C/-c', '--cheatsheet/--no-cheatsheet', help="Enable/disable cheatsheet.", default=None)
@click.option('-A/-a', '--asciiart/--no-asciiart', help="Enable/disable ASCIIart.", default=None)
@click.option(
    '-B/-b',
    '--colour/--no-colour',
    help="Enable/disable Colour mode (useful for colour blind-friendly communication).",
    default=None,
)
def change_config(
    python: str,
    backend: str,
    postgres_version: str,
    mysql_version: str,
    mssql_version: str,
    cheatsheet: bool,
    asciiart: bool,
    colour: bool,
):
    """
    Show/update configuration (Python, Backend, Cheatsheet, ASCIIART).
    """
    asciiart_file = "suppress_asciiart"
    cheatsheet_file = "suppress_cheatsheet"
    colour_file = "suppress_colour"

    if asciiart is not None:
        if asciiart:
            delete_cache(asciiart_file)
            get_console().print('[info]Enable ASCIIART![/]')
        else:
            touch_cache_file(asciiart_file)
            get_console().print('[info]Disable ASCIIART![/]')
    if cheatsheet is not None:
        if cheatsheet:
            delete_cache(cheatsheet_file)
            get_console().print('[info]Enable Cheatsheet[/]')
        elif cheatsheet is not None:
            touch_cache_file(cheatsheet_file)
            get_console().print('[info]Disable Cheatsheet[/]')
    if colour is not None:
        if colour:
            delete_cache(colour_file)
            get_console().print('[info]Enable Colour[/]')
        elif colour is not None:
            touch_cache_file(colour_file)
            get_console().print('[info]Disable Colour[/]')

    def get_status(file: str):
        return "disabled" if check_if_cache_exists(file) else "enabled"

    get_console().print()
    get_console().print("[info]Current configuration:[/]")
    get_console().print()
    get_console().print(f"[info]* Python: {python}[/]")
    get_console().print(f"[info]* Backend: {backend}[/]")
    get_console().print()
    get_console().print(f"[info]* Postgres version: {postgres_version}[/]")
    get_console().print(f"[info]* MySQL version: {mysql_version}[/]")
    get_console().print(f"[info]* MsSQL version: {mssql_version}[/]")
    get_console().print()
    get_console().print(f"[info]* ASCIIART: {get_status(asciiart_file)}[/]")
    get_console().print(f"[info]* Cheatsheet: {get_status(cheatsheet_file)}[/]")
    get_console().print()
    get_console().print()
    get_console().print(f"[info]* Colour: {get_status(colour_file)}[/]")
    get_console().print()


@main.command(name="free-space", help="Free space for jobs run in CI.")
@option_verbose
@option_dry_run
@option_answer
def free_space(verbose: bool, dry_run: bool, answer: str):
    if user_confirm("Are you sure to run free-space and perform cleanup?") == Answer.YES:
        run_command(["sudo", "swapoff", "-a"], verbose=verbose, dry_run=dry_run)
        run_command(["sudo", "rm", "-f", "/swapfile"], verbose=verbose, dry_run=dry_run)
        run_command(["sudo", "apt-get", "clean"], verbose=verbose, dry_run=dry_run, check=False)
        run_command(
            ["docker", "system", "prune", "--all", "--force", "--volumes"], verbose=verbose, dry_run=dry_run
        )
        run_command(["df", "-h"], verbose=verbose, dry_run=dry_run)
        run_command(["docker", "logout", "ghcr.io"], verbose=verbose, dry_run=dry_run, check=False)


@main.command(name="resource-check", help="Check if available docker resources are enough.")
@option_verbose
@option_dry_run
def resource_check(verbose: bool, dry_run: bool):
    shell_params = ShellParams(verbose=verbose, python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    check_docker_resources(shell_params.airflow_image_name, verbose=verbose, dry_run=dry_run)


@main.command(name="fix-ownership", help="Fix ownership of source files to be same as host user.")
@option_verbose
@option_dry_run
def fix_ownership(verbose: bool, dry_run: bool):
    shell_params = ShellParams(
        verbose=verbose,
        mount_sources=MOUNT_ALL,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        skip_environment_initialization=True,
    )
    extra_docker_flags = get_extra_docker_flags(MOUNT_ALL)
    env = get_env_variables_for_docker_commands(shell_params)
    cmd = [
        "docker",
        "run",
        "-t",
        *extra_docker_flags,
        "--pull",
        "never",
        shell_params.airflow_image_name_with_tag,
        "/opt/airflow/scripts/in_container/run_fix_ownership.sh",
    ]
    run_command(cmd, verbose=verbose, dry_run=dry_run, text=True, env=env, check=False)
    # Always succeed
    sys.exit(0)


def write_to_shell(command_to_execute: str, dry_run: bool, script_path: str, force_setup: bool) -> bool:
    skip_check = False
    script_path_file = Path(script_path)
    if not script_path_file.exists():
        skip_check = True
    if not skip_check:
        if BREEZE_COMMENT in script_path_file.read_text():
            if not force_setup:
                get_console().print(
                    "\n[warning]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force[/]\n"
                )
                return False
            else:
                backup(script_path_file)
                remove_autogenerated_code(script_path)
    text = ''
    if script_path_file.exists():
        get_console().print(f"\nModifying the {script_path} file!\n")
        get_console().print(f"\nCopy of the original file is held in {script_path}.bak !\n")
        if not dry_run:
            backup(script_path_file)
            text = script_path_file.read_text()
    else:
        get_console().print(f"\nCreating the {script_path} file!\n")
    if not dry_run:
        script_path_file.write_text(
            text
            + ("\n" if not text.endswith("\n") else "")
            + START_LINE
            + command_to_execute
            + "\n"
            + END_LINE
        )
    else:
        get_console().print(f"[info]The autocomplete script would be added to {script_path}[/]")
    get_console().print(
        f"\n[warning]Please exit and re-enter your shell or run:[/]" f"\n\n   source {script_path}\n"
    )
    return True


BREEZE_COMMENT = "Added by Updated Airflow Breeze autocomplete setup"
START_LINE = f"# START: {BREEZE_COMMENT}\n"
END_LINE = f"# END: {BREEZE_COMMENT}\n"


def remove_autogenerated_code(script_path: str):
    lines = Path(script_path).read_text().splitlines(keepends=True)
    new_lines = []
    pass_through = True
    for line in lines:
        if line == START_LINE:
            pass_through = False
            continue
        if line.startswith(END_LINE):
            pass_through = True
            continue
        if pass_through:
            new_lines.append(line)
    Path(script_path).write_text("".join(new_lines))


def backup(script_path_file: Path):
    shutil.copy(str(script_path_file), str(script_path_file) + ".bak")
