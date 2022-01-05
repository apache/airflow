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
from pathlib import Path
from typing import Optional

import click
from click import ClickException

from airflow_breeze.console import console
from airflow_breeze.visuals import ASCIIART, ASCIIART_STYLE

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


if __name__ == '__main__':
    from airflow_breeze.cache import BUILD_CACHE_DIR

    BUILD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    main()
