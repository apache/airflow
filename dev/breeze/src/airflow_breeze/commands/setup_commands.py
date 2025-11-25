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
import textwrap
from copy import copy
from pathlib import Path
from typing import Any

import click
from click import Context
from rich.console import Console

from airflow_breeze import NAME, VERSION
from airflow_breeze.commands.common_options import (
    option_answer,
    option_backend,
    option_dry_run,
    option_mysql_version,
    option_postgres_version,
    option_python,
    option_verbose,
)
from airflow_breeze.commands.developer_commands import option_auth_manager
from airflow_breeze.commands.main_command import main
from airflow_breeze.utils.cache import check_if_cache_exists, delete_cache, touch_cache_file
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import STANDARD_TIMEOUT, Answer, user_confirm
from airflow_breeze.utils.console import get_console, get_stderr_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import VOLUMES_FOR_SELECTED_MOUNTS
from airflow_breeze.utils.path_utils import (
    AIRFLOW_ROOT_PATH,
    BREEZE_IMAGES_PATH,
    BREEZE_ROOT_PATH,
    BREEZE_SOURCES_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH,
    get_installation_airflow_sources,
    get_installation_sources_config_metadata_hash,
    get_used_airflow_sources,
    get_used_sources_setup_metadata_hash,
)
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.reinstall import reinstall_breeze, warn_non_editable
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose
from airflow_breeze.utils.visuals import ASCIIART, ASCIIART_STYLE


@click.group(cls=BreezeGroup, name="setup", help="Tools that developers can use to configure Breeze")
def setup_group():
    pass


@click.option(
    "-a",
    "--use-current-airflow-sources",
    is_flag=True,
    help="Use current workdir Airflow sources for upgrade"
    + (f" rather than {get_installation_airflow_sources()}." if not generating_command_images() else "."),
)
@setup_group.command(
    name="self-upgrade",
    help=f"Self upgrade Breeze. By default it re-installs Breeze from {get_installation_airflow_sources()}."
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


@setup_group.command(name="autocomplete")
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="Force autocomplete setup even if already setup before (overrides the setup).",
)
@option_verbose
@option_dry_run
@option_answer
def autocomplete(force: bool):
    """
    Enables autocompletion of breeze commands.
    """
    # Determine if the shell is bash/zsh/powershell. It helps to build the autocomplete path
    detected_shell = os.environ.get("SHELL")
    detected_shell = None if detected_shell is None else detected_shell.split(os.sep)[-1]
    if detected_shell not in ["bash", "zsh", "fish"]:
        get_console().print(f"\n[error] The shell {detected_shell} is not supported for autocomplete![/]\n")
        sys.exit(1)
    get_console().print(f"Installing {detected_shell} completion for local user")
    autocomplete_path = BREEZE_ROOT_PATH / "autocomplete" / f"{NAME}-complete-{detected_shell}.sh"
    get_console().print(f"[info]Activation command script is available here: {autocomplete_path}[/]\n")
    get_console().print(f"[warning]We need to add above script to your {detected_shell} profile.[/]\n")
    given_answer = user_confirm(
        "Should we proceed with modifying the script?", default_answer=Answer.NO, timeout=STANDARD_TIMEOUT
    )
    if given_answer == Answer.YES:
        if detected_shell == "bash":
            script_path = str(Path("~").expanduser() / ".bash_completion")
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, force)
        elif detected_shell == "zsh":
            script_path = str(Path("~").expanduser() / ".zshrc")
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, force)
        elif detected_shell == "fish":
            # Include steps for fish shell
            script_path = str(Path("~").expanduser() / f".config/fish/completions/{NAME}.fish")
            if os.path.exists(script_path) and not force:
                get_console().print(
                    "\n[warning]Autocompletion is already setup. Skipping. "
                    "You can force autocomplete installation by adding --force/]\n"
                )
            else:
                with open(autocomplete_path) as source_file, open(script_path, "w") as destination_file:
                    for line in source_file:
                        destination_file.write(line)
        else:
            # Include steps for powershell
            subprocess.check_call(["powershell", "Set-ExecutionPolicy Unrestricted -Scope CurrentUser"])
            script_path = (
                subprocess.check_output(["powershell", "-NoProfile", "echo $profile"]).decode("utf-8").strip()
            )
            command_to_execute = f". {autocomplete_path}"
            write_to_shell(command_to_execute=command_to_execute, script_path=script_path, force_setup=force)
    elif given_answer == Answer.NO:
        get_console().print(
            "\nPlease follow the https://click.palletsprojects.com/en/8.1.x/shell-completion/ "
            "to setup autocompletion for breeze manually if you want to use it.\n"
        )
    else:
        sys.exit(0)


@setup_group.command()
@option_verbose
@option_dry_run
def version():
    """Print information about version of apache-airflow-breeze."""

    get_console().print(ASCIIART, style=ASCIIART_STYLE)
    get_console().print(f"\n[info]Breeze version: {VERSION}[/]")
    get_console().print(f"[info]Breeze installed from: {get_installation_airflow_sources()}[/]")
    get_console().print(f"[info]Used Airflow sources : {get_used_airflow_sources()}[/]\n")
    if get_verbose():
        get_console().print(
            f"[info]Installation sources config hash : {get_installation_sources_config_metadata_hash()}[/]"
        )
        get_console().print(
            f"[info]Used sources config hash         : {get_used_sources_setup_metadata_hash()}[/]"
        )


@setup_group.command(name="config")
@option_python
@option_backend
@option_postgres_version
@option_mysql_version
@option_auth_manager
@click.option("-C/-c", "--cheatsheet/--no-cheatsheet", help="Enable/disable cheatsheet.", default=None)
@click.option("-A/-a", "--asciiart/--no-asciiart", help="Enable/disable ASCIIart.", default=None)
@click.option(
    "--colour/--no-colour",
    help="Enable/disable Colour mode (useful for colour blind-friendly communication).",
    default=None,
)
def change_config(
    python: str,
    backend: str,
    postgres_version: str,
    mysql_version: str,
    auth_manager: str,
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
            get_console().print("[info]Enable ASCIIART[/]")
        else:
            touch_cache_file(asciiart_file)
            get_console().print("[info]Disable ASCIIART[/]")
    if cheatsheet is not None:
        if cheatsheet:
            delete_cache(cheatsheet_file)
            get_console().print("[info]Enable Cheatsheet[/]")
        elif cheatsheet is not None:
            touch_cache_file(cheatsheet_file)
            get_console().print("[info]Disable Cheatsheet[/]")
    if colour is not None:
        if colour:
            delete_cache(colour_file)
            get_console().print("[info]Enable Colour[/]")
        elif colour is not None:
            touch_cache_file(colour_file)
            get_console().print("[info]Disable Colour[/]")

    def get_suppress_status(file: str):
        return "disabled" if check_if_cache_exists(file) else "enabled"

    get_console().print()
    get_console().print("[info]Current configuration:[/]")
    get_console().print()
    get_console().print(f"[info]* Python: {python}[/]")
    get_console().print(f"[info]* Backend: {backend}[/]")
    get_console().print(f"[info]* Postgres version: {postgres_version}[/]")
    get_console().print(f"[info]* MySQL version: {mysql_version}[/]")
    get_console().print(f"[info]* Auth Manager: {auth_manager}[/]")
    get_console().print()
    get_console().print(f"[info]* ASCIIART: {get_suppress_status(asciiart_file)}[/]")
    get_console().print(f"[info]* Cheatsheet: {get_suppress_status(cheatsheet_file)}[/]")
    get_console().print()
    get_console().print(f"[info]* Colour: {get_suppress_status(colour_file)}[/]")
    get_console().print()


def dedent_help(dictionary: dict[str, Any]) -> None:
    """
    Dedent help stored in the dictionary.

    Python 3.13 automatically dedents docstrings retrieved from functions.
    See https://github.com/python/cpython/issues/81283

    However, click uses docstrings in the absence of help strings, and we are using click
    command definition dictionary hash to detect changes in the command definitions, so if the
    help strings are not dedented, the hash will change.

    That's why we must de-dent all the help strings in the command definition dictionary
    before we hash it.
    """
    for key, value in dictionary.items():
        if isinstance(value, dict):
            dedent_help(value)
        elif key == "help" and isinstance(value, str):
            dictionary[key] = textwrap.dedent(value)


def recursively_sort_opts(object: dict[str, Any] | list[Any]) -> None:
    if isinstance(object, dict):
        for key in object:
            if key == "opts" and isinstance(object[key], list):
                object[key] = sorted(object[key])
            elif isinstance(object[key], dict):
                recursively_sort_opts(object[key])
            elif isinstance(object[key], list):
                recursively_sort_opts(object[key])
    elif isinstance(object, list):
        for element in object:
            recursively_sort_opts(element)


def dict_hash(dictionary: dict[str, Any], dedent_help_strings: bool = True, sort_opts: bool = True) -> str:
    """
    MD5 hash of a dictionary of configuration for click.

    Sorted and dumped via json to account for random sequence of keys in the dictionary. Also it
    implements a few corrections to the dict because click does not always keep the same sorting order in
    options or produced differently indented help strings.

    :param dictionary: dictionary to hash
    :param dedent_help_strings: whether to dedent help strings before hashing
    :param sort_opts: whether to sort options before hashing
    """
    if dedent_help_strings:
        dedent_help(dictionary)
    if sort_opts:
        recursively_sort_opts(dictionary)
    # noinspection InsecureHash
    dhash = hashlib.md5()
    try:
        encoded = json.dumps(dictionary, sort_keys=True, default=vars).encode()
    except TypeError:
        get_console().print(dictionary)
        raise
    dhash.update(encoded)
    return dhash.hexdigest()


def is_short_flag(opt):
    return len(opt) == 2 and (not opt.startswith("--"))


def validate_params_for_command(command_params, command):
    options_command_map = {}
    is_duplicate_found = False
    if "params" in command_params:
        for param in command_params["params"]:
            name = param["name"]
            for opt in param["opts"]:
                if is_short_flag(opt):
                    if opt not in options_command_map:
                        options_command_map[opt] = [[command, name]]
                    else:
                        # same flag used in same command
                        get_console().print(
                            f"[error] {opt} short flag has duplicate short hand commands under command(s): "
                            f"{'breeze ' + command} for parameters "
                            f"{options_command_map[opt][0][1]} and {name}\n"
                        )
                        options_command_map[opt][0][1] = name
                        is_duplicate_found = True
    return is_duplicate_found


def get_command_hash_dict() -> dict[str, str]:
    import rich_click

    hashes: dict[str, str] = {}
    with Context(main) as ctx:
        the_context_dict = ctx.to_info_dict()
        if get_verbose():
            get_stderr_console().print(the_context_dict)
        commands_dict = the_context_dict["command"]["commands"]
        options = rich_click.rich_click.OPTION_GROUPS
        for command in sorted(commands_dict.keys()):
            duplicate_found = validate_params_for_command(commands_dict[command], command)
            if duplicate_found:
                sys.exit(1)
            current_command_dict = commands_dict[command]
            subcommands = current_command_dict.get("commands", {})
            if subcommands:
                only_subcommands_with_help = copy(current_command_dict)
                only_subcommands_with_help["commands"] = {
                    subcommand: {
                        "name": subcommands[subcommand]["name"],
                        "help": subcommands[subcommand]["help"],
                    }
                    for subcommand in subcommands
                    if subcommands[subcommand].get("help")
                }
                hashes[f"{command}"] = dict_hash(only_subcommands_with_help) + "\n"
            else:
                hashes[f"{command}"] = dict_hash(current_command_dict) + "\n"
            duplicate_found_subcommand = False
            for subcommand in sorted(subcommands.keys()):
                duplicate_found = validate_params_for_command(
                    commands_dict[command]["commands"][subcommand], command + " " + subcommand
                )
                if duplicate_found:
                    duplicate_found_subcommand = True
                subcommand_click_dict = subcommands[subcommand]
                try:
                    subcommand_rich_click_dict = options[f"breeze {command} {subcommand}"]
                except KeyError:
                    get_console().print(
                        f"[error]The `breeze {command} {subcommand}` is missing in rich-click options[/]"
                    )
                    get_console().print(
                        "[info]Please add it to rich_click.OPTION_GROUPS "
                        "via one of the `*_commands_config.py` "
                        "files in `dev/breeze/src/airflow_breeze/commands`[/]"
                    )
                    sys.exit(1)
                final_dict = {
                    "click_commands": subcommand_click_dict,
                    "rich_click_options": subcommand_rich_click_dict,
                }
                hashes[f"{command}:{subcommand}"] = dict_hash(final_dict) + "\n"
            if duplicate_found_subcommand:
                sys.exit(1)
    return hashes


def write_to_shell(command_to_execute: str, script_path: str, force_setup: bool) -> bool:
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
            backup(script_path_file)
            remove_autogenerated_code(script_path)
    text = ""
    if script_path_file.exists():
        get_console().print(f"\nModifying the {script_path} file!\n")
        get_console().print(f"\nCopy of the original file is held in {script_path}.bak !\n")
        if not get_dry_run():
            backup(script_path_file)
            text = script_path_file.read_text()
    else:
        get_console().print(f"\nCreating the {script_path} file!\n")
    if not get_dry_run():
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
        elif line.startswith(END_LINE):
            pass_through = True
        elif pass_through:
            new_lines.append(line)
    Path(script_path).write_text("".join(new_lines))


def backup(script_path_file: Path):
    shutil.copy(str(script_path_file), str(script_path_file) + ".bak")


def get_old_command_hash() -> dict[str, str]:
    command_hash = {}
    for file in BREEZE_IMAGES_PATH.glob("output_*.txt"):
        command = ":".join(file.name.split("_")[1:])[:-4]
        command_hash[command] = file.read_text()
    return command_hash


SCREENSHOT_WIDTH = "120"


def print_difference(dict1: dict[str, str], dict2: dict[str, str]):
    console = Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    console.print(f"Difference: {set(dict1.items()) ^ set(dict2.items())}")


def regenerate_help_images_for_all_commands(commands: tuple[str, ...], check_only: bool, force: bool) -> int:
    console = Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    if check_only and force:
        console.print("[error]The --check-only flag cannot be used with --force flag.")
        return 2
    if check_only and commands:
        console.print("[error]The --check-only flag cannot be used with --command flag.")
        return 2
    env = os.environ.copy()
    env["AIRFLOW_ROOT_PATH"] = str(AIRFLOW_ROOT_PATH)
    env["RECORD_BREEZE_WIDTH"] = SCREENSHOT_WIDTH
    env["TERM"] = "xterm-256color"
    env["PYTHONPATH"] = str(BREEZE_SOURCES_PATH)
    new_hash_dict = get_command_hash_dict()
    regenerate_all_commands = False
    commands_list = list(commands)
    if force:
        console.print("[info]Force regeneration all breeze command images")
        commands_list.extend(new_hash_dict.keys())
        regenerate_all_commands = True
    elif commands_list:
        console.print(f"[info]Regenerating breeze command images for specified commands:{commands_list}")
    else:
        old_hash_dict = get_old_command_hash()
        if old_hash_dict == new_hash_dict:
            if check_only:
                console.print(
                    "[bright_blue]The hash dumps old/new are the same. Returning with return code 0."
                )
            else:
                console.print("[bright_blue]Skip generation of SVG images as command hashes are unchanged.")
            return 0
        if check_only:
            console.print("[yellow]The hash files differ. Returning 1")
            print_difference(old_hash_dict, new_hash_dict)
            return 1
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
        env["RECORD_BREEZE_TITLE"] = "Breeze commands"
        env["RECORD_BREEZE_OUTPUT_FILE"] = str(BREEZE_IMAGES_PATH / "output-commands.svg")
        env["RECORD_BREEZE_UNIQUE_ID"] = "breeze-help"
        run_command(
            ["breeze", "--help"],
            env=env,
        )
    for command in commands_list:
        if command != "main":
            subcommands = command.split(":")
            env["RECORD_BREEZE_TITLE"] = f"Command: {' '.join(subcommands)}"
            env["RECORD_BREEZE_OUTPUT_FILE"] = str(BREEZE_IMAGES_PATH / f"output_{'_'.join(subcommands)}.svg")
            env["RECORD_BREEZE_UNIQUE_ID"] = f"breeze-{'-'.join(subcommands)}"
            run_command(["breeze", *subcommands, "--help"], env=env)
    if regenerate_all_commands:
        for command, hash_txt in new_hash_dict.items():
            (BREEZE_IMAGES_PATH / f"output_{'_'.join(command.split(':'))}.txt").write_text(hash_txt)
        get_console().print("\n[info]New hash of breeze commands written\n")
    return 1


COMMON_PARAM_NAMES = ["--help", "--verbose", "--dry-run", "--answer"]
COMMAND_PATH_PREFIX = "dev/breeze/src/airflow_breeze/commands/"

DEVELOPER_COMMANDS = [
    "start-airflow",
    "build-docs",
    "down",
    "exec",
    "shell",
    "run",
    "compile-ui-assets",
    "cleanup",
    "generate-migration-file",
    "doctor",
]


def command_path(command: str) -> str:
    if command in DEVELOPER_COMMANDS:
        return COMMAND_PATH_PREFIX + "developer_commands.py"
    return COMMAND_PATH_PREFIX + command.replace("-", "_") + "_commands.py"


def command_path_config(command: str) -> str:
    if command in DEVELOPER_COMMANDS:
        return COMMAND_PATH_PREFIX + "developer_commands_config.py"
    return COMMAND_PATH_PREFIX + command.replace("-", "_") + "_commands_config.py"


def find_options_in_options_list(option: str, option_list: list[list[str]]) -> int | None:
    for i, options in enumerate(option_list):
        if option in options:
            return i
    return None


def errors_detected_in_params(command: str, subcommand: str | None, command_dict: dict[str, Any]) -> bool:
    import rich_click

    get_console().print(
        f"[info]Checking if params are in groups for specified command :{command}"
        + (f" {subcommand}." if subcommand else ".")
    )
    errors_detected = False
    options = rich_click.rich_click.OPTION_GROUPS
    rich_click_key = "breeze " + command + (f" {subcommand}" if subcommand else "")
    if rich_click_key not in options:
        get_console().print(
            f"[error]The command `{rich_click_key}` not found in dictionaries "
            f"defined in rich click configuration."
        )
        get_console().print(f"[warning]Please add it to the `{command_path_config(command)}`.")
        return True
    rich_click_param_groups = options[rich_click_key]
    defined_param_names = [
        param["opts"] for param in command_dict["params"] if param["param_type_name"] == "option"
    ]
    for group in rich_click_param_groups:
        if "options" in group:
            for param in group["options"]:
                index = find_options_in_options_list(param, defined_param_names)
                if index is not None:
                    del defined_param_names[index]
                else:
                    get_console().print(
                        f"[error]Parameter `{param}` is not defined as option in {command_path(command)} in "
                        f"`{rich_click_key}`, but is present in the "
                        f"`{rich_click_key}` group in `{command_path_config(command)}`."
                    )
                    get_console().print(
                        "[warning]Please remove it from there or add parameter in "
                        "the command. NOTE! This error might be printed when the option is"
                        "added twice in the command definition!."
                    )
                    errors_detected = True
    for param in COMMON_PARAM_NAMES:
        index = find_options_in_options_list(param, defined_param_names)
        if index is not None:
            del defined_param_names[index]
    if defined_param_names:
        for param in defined_param_names:
            get_console().print(
                f"[error]Parameter `{param}` is defined in `{command_path(command)}` in "
                f"`{rich_click_key}`, but does not belong to any group options "
                f"in `{rich_click_key}` group in `{command_path_config(command)}` and is not common."
            )
            get_console().print("[warning]Please add it to relevant group or create new group there.")
        errors_detected = True
    return errors_detected


def check_that_all_params_are_in_groups(commands: tuple[str, ...]) -> int:
    Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    env = os.environ.copy()
    env["AIRFLOW_ROOT_PATH"] = str(AIRFLOW_ROOT_PATH)
    env["RECORD_BREEZE_WIDTH"] = SCREENSHOT_WIDTH
    env["TERM"] = "xterm-256color"
    env["PYTHONPATH"] = str(BREEZE_SOURCES_PATH)
    with Context(main) as ctx:
        the_context_dict = ctx.to_info_dict()
    commands_dict = the_context_dict["command"]["commands"]
    if commands:
        commands_list = list(commands)
    else:
        commands_list = commands_dict.keys()
    errors_detected = False
    for command in commands_list:
        current_command_dict = commands_dict[command]
        if "commands" in current_command_dict:
            subcommands = current_command_dict["commands"]
            for subcommand in sorted(subcommands.keys()):
                if errors_detected_in_params(command, subcommand, subcommands[subcommand]):
                    errors_detected = True
        else:
            if errors_detected_in_params(command, None, current_command_dict):
                errors_detected = True
    return 1 if errors_detected else 0


@setup_group.command(name="regenerate-command-images", help="Regenerate breeze command images.")
@click.option("--force", is_flag=True, help="Forces regeneration of all images", envvar="FORCE")
@click.option(
    "--check-only",
    is_flag=True,
    help="Only check if some images need to be regenerated. Return 0 if no need or 1 if needed. "
    "Cannot be used together with --command flag or --force.",
    envvar="CHECK_ONLY",
)
@click.option(
    "--command",
    help="Command(s) to regenerate images for (optional, might be repeated)",
    show_default=True,
    multiple=True,
    type=BetterChoice(sorted(list(get_old_command_hash().keys()))),
)
@option_verbose
@option_dry_run
def regenerate_command_images(command: tuple[str, ...], force: bool, check_only: bool):
    return_code = regenerate_help_images_for_all_commands(
        commands=command, check_only=check_only, force=force
    )
    sys.exit(return_code)


@setup_group.command(name="check-all-params-in-groups", help="Check that all parameters are put in groups.")
@click.option(
    "--command",
    help="Command(s) to regenerate images for (optional, might be repeated)",
    show_default=True,
    multiple=True,
    type=BetterChoice(sorted(list(get_old_command_hash().keys()))),
)
@option_verbose
@option_dry_run
def check_all_params_in_groups(command: tuple[str, ...]):
    return_code = check_that_all_params_are_in_groups(commands=command)
    sys.exit(return_code)


def _insert_documentation(file_path: Path, content: list[str], header: str, footer: str):
    text = file_path.read_text().splitlines(keepends=True)
    replacing = False
    result: list[str] = []
    for line in text:
        if line.strip().startswith(header.strip()):
            replacing = True
            result.append(line)
            result.extend(content)
        if line.strip().startswith(footer.strip()):
            replacing = False
        if not replacing:
            result.append(line)
    src = "".join(result)
    file_path.write_text(src)


@setup_group.command(
    name="synchronize-local-mounts",
    help="Synchronize local mounts between python files and docker compose yamls.",
)
@option_verbose
@option_dry_run
def synchronize_local_mounts():
    get_console().print("[info]Synchronizing local mounts between python files and docker compose yamls.[/]")
    mounts_header = (
        "        # START automatically generated volumes from "
        "VOLUMES_FOR_SELECTED_MOUNTS in docker_command_utils.py"
    )
    mounts_footer = (
        "        # END automatically generated volumes from "
        "VOLUMES_FOR_SELECTED_MOUNTS in docker_command_utils.py"
    )
    prefix = "      "
    volumes = []
    for src, dest in VOLUMES_FOR_SELECTED_MOUNTS:
        volumes.extend(
            [
                prefix + "- type: bind\n",
                prefix + f"  source: ../../../{src}\n",
                prefix + f"  target: {dest}\n",
            ]
        )
    _insert_documentation(SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH, volumes, mounts_header, mounts_footer)
    get_console().print("[success]Synchronized local mounts.[/]")
