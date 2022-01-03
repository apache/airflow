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
from rich.console import Console

from airflow_breeze.visuals import ASCIIART, ASCIIART_STYLE

NAME = "Breeze2"
VERSION = "0.0.1"

__AIRFLOW_SOURCES_ROOT = Path.cwd()

__AIRFLOW_CFG_FILE = "setup.cfg"

console = Console(force_terminal=True, color_system="standard", width=180)


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
@main.command()
def build_ci_image(verbose: bool):
    """Builds breeze.ci image for breeze.py."""
    if verbose:
        console.print(f"\n[blue]Building image of airflow from {__AIRFLOW_SOURCES_ROOT}[/]\n")
    raise ClickException("\nPlease implement building the CI image\n")


if __name__ == '__main__':
    main()
