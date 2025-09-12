#! /usr/bin/env python
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

# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "packaging>=25",
#   "requests>=2.28.1",
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import re
import sys
from pathlib import Path

import requests
from packaging.version import Version, parse
from rich.console import Console

console = Console(color_system="standard", stderr=True, width=400)


def check_airflow_version(airflow_version: Version) -> tuple[str, bool]:
    """
    Check if the given version is a valid Airflow version and latest.

    airflow_version: The Airflow version to check.
    returns: tuple containing the version and a boolean indicating if it's latest.
    """
    latest = False
    try:
        response = requests.get(
            "https://pypi.org/pypi/apache-airflow/json", headers={"User-Agent": "Python requests"}
        )
        response.raise_for_status()
        data = response.json()
        latest_version = Version(data["info"]["version"])
        all_versions = sorted(
            (parse(v) for v in data["releases"].keys()),
            reverse=True,
        )
        if airflow_version not in all_versions:
            console.print(f"[red]Version {airflow_version} is not a valid Airflow release version.")
            console.print("[yellow]Available versions (latest 30 shown):")
            console.print([str(v) for v in all_versions[:30]])
            sys.exit(1)
        if airflow_version == latest_version:
            latest = True
        # find requires-python = ">=VERSION" in pyproject.toml file of airflow
        pyproject_toml_conntent = (Path(__file__).parents[2] / "pyproject.toml").read_text()
        matched_version = re.search('requires-python = ">=([0-9]+.[0-9]+)', pyproject_toml_conntent)
        if matched_version:
            min_version = matched_version.group(1)
        else:
            console.print("[red]Error: requires-python version not found in pyproject.toml")
            sys.exit(1)
        constraints_url = (
            f"https://raw.githubusercontent.com/apache/airflow/"
            f"constraints-{airflow_version}/constraints-{min_version}.txt"
        )
        console.print(f"[bright_blue]Checking constraints file: {constraints_url}")
        response = requests.head(constraints_url)
        if response.status_code == 404:
            console.print(
                f"[red]Error: Constraints file not found for version {airflow_version}. "
                f"Please set appropriate tag."
            )
            sys.exit(1)
        response.raise_for_status()
        console.print(f"[green]Constraints file found for version {airflow_version}, Python {min_version}")
        return str(airflow_version), latest
    except Exception as e:
        console.print(f"[red]Error fetching latest version: {e}")
        sys.exit(1)


def normalize_version(version: str) -> Version:
    try:
        return Version(version)
    except Exception as e:
        console.print(f"[red]Error normalizing version: {e}")
        sys.exit(1)


def print_both_outputs(output: str):
    print(output)
    console.print(output)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        console.print("[yellow]Usage: python normalize_airflow_version.py <version>")
        sys.exit(1)
    version = sys.argv[1]
    parsed_version = normalize_version(version)
    actual_version, is_latest = check_airflow_version(parsed_version)
    print_both_outputs(f"airflowVersion={actual_version}")
    skip_latest = "false" if is_latest else "true"
    print_both_outputs(f"skipLatest={skip_latest}")
