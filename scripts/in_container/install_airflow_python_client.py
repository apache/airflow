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

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import AIRFLOW_DIST_PATH, click, console, run_command

ALLOWED_DISTRIBUTION_FORMAT = ["wheel", "sdist", "both"]


def find_airflow_python_client(extension: str):
    packages = [f.as_posix() for f in AIRFLOW_DIST_PATH.glob(f"apache_airflow_client-[0-9]*.{extension}")]
    if len(packages) > 1:
        console.print(f"\n[red]Found multiple airflow client packages: {packages}\n")
        sys.exit(1)
    elif len(packages) == 0:
        console.print("\n[red]No airflow client package found\n")
        sys.exit(1)
    if packages:
        console.print(f"\n[bright_blue]Found airflow client package: {packages[0]}\n")
    else:
        console.print("\n[yellow]No airflow client package found.\n")
    return packages[0]


@click.command()
@click.option(
    "--distribution-format",
    default=ALLOWED_DISTRIBUTION_FORMAT[0],
    envvar="DISTRIBUTION_FORMAT",
    show_default=True,
    type=click.Choice(ALLOWED_DISTRIBUTION_FORMAT),
    help="Package format to use",
)
@click.option(
    "--use-distributions-from-dist",
    is_flag=True,
    default=True,
    show_default=True,
    envvar="USE_DISTRIBUTIONS_FROM_DIST",
    help="Should install distributions from dist folder if set.",
)
@click.option(
    "--github-actions",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="GITHUB_ACTIONS",
    help="Running in GitHub Actions",
)
def install_airflow_python_client(
    distribution_format: str, use_distributions_from_dist: bool, github_actions: bool
):
    if use_distributions_from_dist and distribution_format not in ["wheel", "sdist"]:
        console.print(
            f"[red]DISTRIBUTION_FORMAT must be one of 'wheel' or 'sdist' and not {distribution_format}"
        )
        sys.exit(1)

    extension = "whl" if distribution_format == "wheel" else "tar.gz"

    install_airflow_python_client_cmd = [
        "uv",
        "pip",
        "install",
        find_airflow_python_client(extension),
    ]
    console.print("\n[bright_blue]Installing airflow python client\n")
    run_command(install_airflow_python_client_cmd, github_actions=github_actions, check=True)


if __name__ == "__main__":
    install_airflow_python_client()
