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

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import click
from click import Context
from rich.console import Console

from airflow_breeze import NAME, VERSION
from airflow_breeze.commands.main_command import main
from airflow_breeze.utils.cache import check_if_cache_exists, delete_cache, touch_cache_file
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.common_options import (
    option_answer,
    option_backend,
    option_dry_run,
    option_mssql_version,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_verbose,
)
from airflow_breeze.utils.confirm import STANDARD_TIMEOUT, Answer, user_confirm
from airflow_breeze.utils.console import get_console, get_stderr_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    get_installation_airflow_sources,
    get_installation_sources_config_metadata_hash,
    get_package_setup_metadata_hash,
    get_used_airflow_sources,
    get_used_sources_setup_metadata_hash,
)
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.reinstall import reinstall_breeze, warn_non_editable
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE


@click.group(cls=BreezeGroup, name='setup', help='Tools that developers can use to configure Breeze')
def setup():
    pass


@click.option(
    '-a',
    '--use-current-airflow-sources',
    is_flag=True,
    help='Use current workdir Airflow sources for upgrade'
    + (f" rather than {get_installation_airflow_sources()}." if not generating_command_images() else "."),
)
@setup.command(
    name='self-upgrade',
    help="Self upgrade Breeze. By default it re-installs Breeze "
    f"from {get_installation_airflow_sources()}."
    if not generating_command_images()
    else "Self upgrade Breeze.",
)
def self_upgrade(use_current_airflow_sources: bool):
    if use_current_airflow_sources:
        airflow_sources: Path | None = get_used_airflow_sources()
    else:
        airflow_sources = get_installation_airflow_sources()
    if airflow_sources is not None:
        breeze_sources = airflow_sources / "dev" / "breeze"
        reinstall_breeze(breeze_sources, re_run=False)
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
@setup.command(name='autocomplete')
def autocomplete(verbose: bool, dry_run: bool, force: bool, answer: str | None):
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
@setup.command()
def version(verbose: bool):
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
            f"[info]Used sources config hash         : {get_used_sources_setup_metadata_hash()}[/]"
        )
        get_console().print(
            f"[info]Package config hash              : {(get_package_setup_metadata_hash())}[/]\n"
        )


@setup.command(name='config')
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_mssql_version
@click.option('-C/-c', '--cheatsheet/--no-cheatsheet', help="Enable/disable cheatsheet.", default=None)
@click.option('-A/-a', '--asciiart/--no-asciiart', help="Enable/disable ASCIIart.", default=None)
@click.option(
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


def dict_hash(dictionary: dict[str, Any]) -> str:
    """MD5 hash of a dictionary. Sorted and dumped via json to account for random sequence)"""
    # noinspection InsecureHash
    dhash = hashlib.md5()
    encoded = json.dumps(dictionary, sort_keys=True, default=vars).encode()
    dhash.update(encoded)
    return dhash.hexdigest()


def get_command_hash_export(verbose: bool) -> str:
    hashes = []
    with Context(main) as ctx:
        the_context_dict = ctx.to_info_dict()
        if verbose:
            get_stderr_console().print(the_context_dict)
        hashes.append(f"main:{dict_hash(the_context_dict['command']['params'])}")
        commands_dict = the_context_dict['command']['commands']
        for command in sorted(commands_dict.keys()):
            current_command_dict = commands_dict[command]
            if 'commands' in current_command_dict:
                subcommands = current_command_dict['commands']
                for subcommand in sorted(subcommands.keys()):
                    hashes.append(f"{command}:{subcommand}:{dict_hash(subcommands[subcommand])}")
                hashes.append(f"{command}:{dict_hash(current_command_dict)}")
            else:
                hashes.append(f"{command}:{dict_hash(current_command_dict)}")
    return "\n".join(hashes) + "\n"


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
        f"\n[warning]Please exit and re-enter your shell or run:[/]\n\n   source {script_path}\n"
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


BREEZE_IMAGES_DIR = AIRFLOW_SOURCES_ROOT / "images" / "breeze"
BREEZE_INSTALL_DIR = AIRFLOW_SOURCES_ROOT / "dev" / "breeze"
BREEZE_SOURCES_DIR = BREEZE_INSTALL_DIR / "src"


def get_commands() -> list[str]:
    results = []
    content = (BREEZE_IMAGES_DIR / "output-commands-hash.txt").read_text()
    for line in content.splitlines():
        strip_line = line.strip()
        if strip_line.strip() == '' or strip_line.startswith("#"):
            continue
        results.append(':'.join(strip_line.split(":")[:-1]))
    return results


SCREENSHOT_WIDTH = "120"

PREAMBLE = """# This file is automatically generated by pre-commit. If you have a conflict with this file
# Please do not solve it but run `breeze setup regenerate-command-images`.
# This command should fix the conflict and regenerate help images that you have conflict with.
"""


def get_hash_dict(hash_file_content: str) -> dict[str, str]:
    results = {}
    for line in hash_file_content.splitlines():
        strip_line = line.strip()
        if strip_line.strip() == '' or strip_line.startswith("#"):
            continue
        command = ':'.join(strip_line.split(":")[:-1])
        the_hash = strip_line.split(":")[-1]
        results[command] = the_hash
    return results


def print_difference(dict1: dict[str, str], dict2: dict[str, str]):
    console = Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    console.print(f"Difference: {set(dict1.items()) ^ set(dict2.items())}")


def print_help_for_all_commands(
    commands: tuple[str, ...], check_only: bool, force: bool, verbose: bool, dry_run: bool
):
    console = Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    if check_only and force:
        console.print("[error]The --check-only flag cannot be used with --force flag.")
        sys.exit(2)
    if check_only and commands:
        console.print("[error]The --check-only flag cannot be used with --coomand flag.")
        sys.exit(2)
    env = os.environ.copy()
    env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_ROOT)
    env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
    env['RECORD_BREEZE_TITLE'] = "Breeze commands"
    env['RECORD_BREEZE_OUTPUT_FILE'] = str(BREEZE_IMAGES_DIR / "output-commands.svg")
    env['TERM'] = "xterm-256color"
    env['PYTHONPATH'] = str(BREEZE_SOURCES_DIR)
    new_hash_dump = PREAMBLE + get_command_hash_export(verbose=verbose)
    regenerate_all_commands = False
    hash_file_path = BREEZE_IMAGES_DIR / "output-commands-hash.txt"
    commands_list = list(commands)
    if force:
        console.print("[info]Force regeneration all breeze command images")
        commands_list.extend(get_hash_dict(new_hash_dump))
        regenerate_all_commands = True
    elif commands_list:
        console.print(f"[info]Regenerating breeze command images for specified commands:{commands_list}")
    else:
        try:
            old_hash_dump = hash_file_path.read_text()
        except FileNotFoundError:
            old_hash_dump = ""
        old_hash_dict = get_hash_dict(old_hash_dump)
        new_hash_dict = get_hash_dict(new_hash_dump)
        if old_hash_dict == new_hash_dict:
            if check_only:
                console.print("[bright_blue]The hash files are the same. Returning with return code 0.")
            else:
                console.print("[bright_blue]Skip generation of SVG images as command hashes are unchanged.")
            return
        if check_only:
            console.print("[yellow]The hash files differ. Returning 1")
            print_difference(old_hash_dict, new_hash_dict)
            sys.exit(1)
        console.print("[yellow]The hash files differ. Regenerating changed commands")
        print_difference(old_hash_dict, new_hash_dict)
        for hash_command in new_hash_dict:
            if hash_command not in old_hash_dict:
                console.print(f"[yellow]New command: {hash_command}")
                commands_list.append(hash_command)
            elif old_hash_dict[hash_command] != new_hash_dict[hash_command]:
                console.print(f"[yellow]Updated command: {hash_command}")
                commands_list.append(hash_command)
            else:
                console.print(f"[bright_blue]Unchanged command: {hash_command}")
        regenerate_all_commands = True
    if regenerate_all_commands:
        env = os.environ.copy()
        env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_ROOT)
        env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
        env['RECORD_BREEZE_TITLE'] = "Breeze commands"
        env['RECORD_BREEZE_OUTPUT_FILE'] = str(BREEZE_IMAGES_DIR / "output-commands.svg")
        env['TERM'] = "xterm-256color"
        run_command(["breeze", "--help"], env=env, verbose=verbose, dry_run=dry_run)
    for command in commands_list:
        if command == 'main':
            continue
        if ":" not in command:
            env = os.environ.copy()
            env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_ROOT)
            env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
            env['RECORD_BREEZE_TITLE'] = f"Command: {command}"
            env['RECORD_BREEZE_OUTPUT_FILE'] = str(BREEZE_IMAGES_DIR / f"output_{command}.svg")
            env['TERM'] = "xterm-256color"
            run_command(["breeze", command, "--help"], env=env, verbose=verbose, dry_run=dry_run)
        else:
            split_command = command.split(":")
            env = os.environ.copy()
            env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_ROOT)
            env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
            env['RECORD_BREEZE_TITLE'] = f"Command: {split_command[0]} {split_command[1]}"
            env['RECORD_BREEZE_OUTPUT_FILE'] = str(
                BREEZE_IMAGES_DIR / f"output_{split_command[0]}_{split_command[1]}.svg"
            )
            env['TERM'] = "xterm-256color"
            run_command(
                ["breeze", split_command[0], split_command[1], "--help"],
                env=env,
                verbose=verbose,
                dry_run=dry_run,
            )
    if regenerate_all_commands:
        hash_file_path.write_text(new_hash_dump)
        get_console().print(f"\n[info]New hash of breeze commands written in {hash_file_path}\n")


@setup.command(name="regenerate-command-images", help="Regenerate breeze command images.")
@click.option("--force", is_flag=True, help="Forces regeneration of all images", envvar='FORCE')
@click.option(
    "--check-only",
    is_flag=True,
    help="Only check if some images need to be regenerated. Return 0 if no need or 1 if needed. "
    "Cannot be used together with --command flag or --force.",
    envvar='CHECK_ONLY',
)
@click.option(
    '--command',
    help="Command(s) to regenerate images for (optional, might be repeated)",
    show_default=True,
    multiple=True,
    type=BetterChoice(get_commands()),
)
@option_verbose
@option_dry_run
def regenerate_command_images(
    command: tuple[str, ...], force: bool, check_only: bool, verbose: bool, dry_run: bool
):
    print_help_for_all_commands(
        commands=command, check_only=check_only, force=force, verbose=verbose, dry_run=dry_run
    )
