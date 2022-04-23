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

"""Cleans up the environment before starting CI."""


import rich_click as click
from rich.console import Console

from airflow_breeze.utils.run_utils import run_command

console = Console(force_terminal=True, color_system="standard", width=180)

option_verbose = click.option(
    "--verbose",
    envvar='VERBOSE',
    is_flag=True,
    help="Print verbose information about free space steps",
)

option_dry_run = click.option(
    "--dry-run",
    is_flag=True,
    help="Just prints commands without executing them",
)


@click.command()
@option_verbose
@option_dry_run
def main(verbose, dry_run):
    run_command(["sudo", "swapoff", "-a"], verbose=verbose, dry_run=dry_run)
    run_command(["sudo", "rm", "-f", "/swapfile"], verbose=verbose, dry_run=dry_run)
    run_command(["sudo", "apt-get", "clean"], verbose=verbose, dry_run=dry_run, check=False)
    run_command(
        ["docker", "system", "prune", "--all", "--force", "--volumes"], verbose=verbose, dry_run=dry_run
    )
    run_command(["df", "-h"], verbose=verbose, dry_run=dry_run)
    run_command(["docker", "logout", "ghcr.io"], verbose=verbose, dry_run=dry_run, check=False)


if __name__ == '__main__':
    main()
