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

import click
from click import ClickException
from rich.console import Console

from airflow_breeze.visuals import ASCIIART, ASCIIART_STYLE

NAME = "Breeze2"
VERSION = "0.0.1"


@click.group()
def main():
    pass


console = Console(force_terminal=True, color_system="standard", width=180)


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
    console.print(ASCIIART, style=ASCIIART_STYLE)
    raise ClickException("\nPlease implement entering breeze.py\n")


@option_verbose
@main.command()
def build_ci_image(verbose: bool):
    """Builds breeze.ci image for breeze.py."""
    if verbose:
        console.print("\n[blue]Building image[/]\n")
    raise ClickException("\nPlease implement building the CI image\n")


if __name__ == '__main__':
    main()
