#!/usr/bin/env python3
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
import subprocess
import sys
from pathlib import Path
from typing import Optional

import click
import click_completion
from click import ClickException
from click_completion import get_auto_shell

from airflow_breeze.console import console
from airflow_breeze.visuals import ASCIIART, ASCIIART_STYLE

AIRFLOW_SOURCES_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent

NAME = "Breeze2"
VERSION = "0.0.1"

__AIRFLOW_SOURCES_ROOT = Path.cwd()

__AIRFLOW_CFG_FILE = "setup.cfg"


def get_airflow_sources_root():
    return __AIRFLOW_SOURCES_ROOT


def search_upwards_for_airflow_sources_root(start_from: Path) -> Optional[Path]:
    root = Path(start_from.root)
    d = start_from
    while d != root:
        attempt = d / __AIRFLOW_CFG_FILE
        if attempt.exists() and "name = apache-airflow\n" in attempt.read_text():
            return attempt.parent
        d = d.parent
    return None


def find_airflow_sources_root():
    # Try to find airflow sources in current working dir
    airflow_sources_root = search_upwards_for_airflow_sources_root(Path.cwd())
    if not airflow_sources_root:
        # Or if it fails, find it in parents of the directory where the ./breeze.py is.
        airflow_sources_root = search_upwards_for_airflow_sources_root(Path(__file__).resolve().parent)
    global __AIRFLOW_SOURCES_ROOT
    if airflow_sources_root:
        __AIRFLOW_SOURCES_ROOT = airflow_sources_root
    else:
        console.print(f"\n[yellow]Could not find Airflow sources location. Assuming {__AIRFLOW_SOURCES_ROOT}")
    os.chdir(__AIRFLOW_SOURCES_ROOT)


click_completion.init()


@click.group()
def main():
    find_airflow_sources_root()


option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)


@main.command()
def version():
    """Prints version of breeze.py."""
    console.print(ASCIIART, style=ASCIIART_STYLE)
    console.print(f"\n[green]{NAME} version: {VERSION}[/]\n")


@option_verbose
@main.command()
def shell(verbose: bool):
    """Enters breeze.py environment. this is the default command use when no other is selected."""
    if verbose:
        console.print("\n[green]Welcome to breeze.py[/]\n")
        console.print(f"\n[green]Root of Airflow Sources = {__AIRFLOW_SOURCES_ROOT}[/]\n")
    console.print(ASCIIART, style=ASCIIART_STYLE)
    raise ClickException("\nPlease implement entering breeze.py\n")


@option_verbose
@main.command(name='build-ci-image')
@click.option(
    '--additional-extras',
    help='This installs additional extra package while installing airflow in the image.',
)
@click.option('-p', '--python', help='Choose your python version')
@click.option(
    '--additional-dev-apt-deps', help='Additional apt dev dependencies to use when building the images.'
)
@click.option(
    '--additional-runtime-apt-deps',
    help='Additional apt runtime dependencies to use when building the images.',
)
@click.option(
    '--additional-python-deps', help='Additional python dependencies to use when building the images.'
)
@click.option(
    '--additional_dev_apt_command', help='Additional command executed before dev apt deps are installed.'
)
@click.option(
    '--additional_runtime_apt_command',
    help='Additional command executed before runtime apt deps are installed.',
)
@click.option(
    '--additional_dev_apt_env', help='Additional environment variables set when adding dev dependencies.'
)
@click.option(
    '--additional_runtime_apt_env',
    help='Additional environment variables set when adding runtime dependencies.',
)
@click.option('--dev-apt-command', help='The basic command executed before dev apt deps are installed.')
@click.option(
    '--dev-apt-deps',
    help='The basic apt dev dependencies to use when building the images.',
)
@click.option(
    '--runtime-apt-command', help='The basic command executed before runtime apt deps are installed.'
)
@click.option(
    '--runtime-apt-deps',
    help='The basic apt runtime dependencies to use when building the images.',
)
@click.option('--github-repository', help='Choose repository to push/pull image.')
@click.option('--build-cache', help='Cache option')
@click.option('--upgrade-to-newer-dependencies', is_flag=True)
def build_ci_image(
    verbose: bool,
    additional_extras: Optional[str],
    python: Optional[float],
    additional_dev_apt_deps: Optional[str],
    additional_runtime_apt_deps: Optional[str],
    additional_python_deps: Optional[str],
    additional_dev_apt_command: Optional[str],
    additional_runtime_apt_command: Optional[str],
    additional_dev_apt_env: Optional[str],
    additional_runtime_apt_env: Optional[str],
    dev_apt_command: Optional[str],
    dev_apt_deps: Optional[str],
    runtime_apt_command: Optional[str],
    runtime_apt_deps: Optional[str],
    github_repository: Optional[str],
    build_cache: Optional[str],
    upgrade_to_newer_dependencies: bool,
):
    """Builds docker CI image without entering the container."""
    from airflow_breeze.ci.build_image import build_image

    if verbose:
        console.print(f"\n[blue]Building image of airflow from {__AIRFLOW_SOURCES_ROOT}[/]\n")
    build_image(
        verbose,
        additional_extras=additional_extras,
        python_version=python,
        additional_dev_apt_deps=additional_dev_apt_deps,
        additional_runtime_apt_deps=additional_runtime_apt_deps,
        additional_python_deps=additional_python_deps,
        additional_runtime_apt_command=additional_runtime_apt_command,
        additional_dev_apt_command=additional_dev_apt_command,
        additional_dev_apt_env=additional_dev_apt_env,
        additional_runtime_apt_env=additional_runtime_apt_env,
        dev_apt_command=dev_apt_command,
        dev_apt_deps=dev_apt_deps,
        runtime_apt_command=runtime_apt_command,
        runtime_apt_deps=runtime_apt_deps,
        github_repository=github_repository,
        docker_cache=build_cache,
        upgrade_to_newer_dependencies=upgrade_to_newer_dependencies,
    )


@option_verbose
@main.command(name='build-prod-image')
def build_prod_image(verbose: bool):
    """Builds docker Production image without entering the container."""
    if verbose:
        console.print("\n[blue]Building image[/]\n")
    raise ClickException("\nPlease implement building the Production image\n")


@option_verbose
@main.command(name='start-airflow')
def start_airflow(verbose: bool):
    """Enters breeze.py environment and set up the tmux session"""
    if verbose:
        console.print("\n[green]Welcome to breeze.py[/]\n")
    console.print(ASCIIART, style=ASCIIART_STYLE)
    raise ClickException("\nPlease implement entering breeze.py\n")


def write_to_shell(command_to_execute: str, script_path: str, breeze_comment: str):
    skip_check = False
    script_path_file = Path(script_path)
    if not script_path_file.exists():
        skip_check = True
    if not skip_check:
        with open(script_path) as script_file:
            if breeze_comment in script_file.read():
                click.echo("Autocompletion is already setup. Skipping")
                click.echo(f"Please exit and re-enter your shell or run: \'source {script_path}\'")
                sys.exit()
    click.echo(f"This will modify the {script_path} file")
    with open(script_path, 'a') as script_file:
        script_file.write(f"\n# START: {breeze_comment}\n")
        script_file.write(f"{command_to_execute}\n")
        script_file.write(f"# END: {breeze_comment}\n")
        click.echo(f"Please exit and re-enter your shell or run: \'source {script_path}\'")


@main.command(name='setup-autocomplete')
def setup_autocomplete():
    """
    Enables autocompletion of Breeze2 commands.
    Functionality: By default the generated shell scripts will be available in ./dev/breeze/autocomplete/ path
    Depending on the shell type in the machine we have to link it to the corresponding file
    """
    global NAME
    breeze_comment = "Added by Updated Airflow Breeze autocomplete setup"
    # Determine if the shell is bash/zsh/powershell. It helps to build the autocomplete path
    shell = get_auto_shell()
    click.echo(f"Installing {shell} completion for local user")
    extra_env = {'_CLICK_COMPLETION_COMMAND_CASE_INSENSITIVE_COMPLETE': 'ON'}
    autocomplete_path = Path(AIRFLOW_SOURCES_DIR) / ".build/autocomplete" / f"{NAME}-complete.{shell}"
    shell, path = click_completion.core.install(
        shell=shell, prog_name=NAME, path=autocomplete_path, append=False, extra_env=extra_env
    )
    click.echo(f"Activation command scripts are created in this autocompletion path: {autocomplete_path}")
    if click.confirm(f"Do you want to add the above autocompletion scripts to your {shell} profile?"):
        if shell == 'bash':
            script_path = Path('~').expanduser() / '/.bash_completion'
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, breeze_comment)
        elif shell == 'zsh':
            script_path = Path('~').expanduser() / '/.zshrc'
            command_to_execute = f"source {autocomplete_path}"
            write_to_shell(command_to_execute, script_path, breeze_comment)
        elif shell == 'fish':
            # Include steps for fish shell
            script_path = Path('~').expanduser() / f'/.config/fish/completions/{NAME}.fish'
            with open(path) as source_file, open(script_path, 'w') as destination_file:
                for line in source_file:
                    destination_file.write(line)
        else:
            # Include steps for powershell
            subprocess.check_call(['powershell', 'Set-ExecutionPolicy Unrestricted -Scope CurrentUser'])
            script_path = subprocess.check_output(['powershell', '-NoProfile', 'echo $profile']).strip()
            command_to_execute = f". {autocomplete_path}"
            write_to_shell(command_to_execute, script_path.decode("utf-8"), breeze_comment)
    else:
        click.echo(f"Link for manually adding the autocompletion script to {shell} profile")


if __name__ == '__main__':
    from airflow_breeze.cache import BUILD_CACHE_DIR

    BUILD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    main()
