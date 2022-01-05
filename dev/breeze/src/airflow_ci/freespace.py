#!/usr/bin/env python3
#
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

"""freespace.py for clean environment before start CI"""

import shlex
import subprocess
from typing import List

import click
from rich.console import Console

console = Console(force_terminal=True, color_system="standard", width=180)

option_verbose = click.option(
    "--verbose",
    envvar='VERBOSE',
    is_flag=True,
    help="Print verbose information about free space steps",
)


@click.group()
def main():
    pass


@option_verbose
def run_command(cmd: List[str], verbose, *, check: bool = True, **kwargs):
    if verbose:
        console.print(f"\n[green]$ {' '.join(shlex.quote(c) for c in cmd)}[/]\n")
    try:
        subprocess.run(cmd, check=check, **kwargs)
    except subprocess.CalledProcessError as ex:
        print("========================= OUTPUT start ============================")
        print(ex.stderr)
        print(ex.stdout)
        print("========================= OUTPUT end ============================")


@main.command()
@option_verbose
def free_space(verbose):
    run_command(["sudo", "swapoff", "-a"], verbose)
    run_command(["sudo", "rm", "-f", "/swapfile"], verbose)
    run_command(["sudo", "apt", "clean", "||", "true"], verbose)
    run_command(["docker", "system", "prune", "--all", "--force", "volumes"], verbose)
    run_command(["df", "-h"], verbose)
    run_command(["docker", "logout", "ghcr.io"], verbose)


if __name__ == '__main__':
    main()
