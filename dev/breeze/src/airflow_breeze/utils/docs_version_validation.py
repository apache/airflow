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

import json
import os
import sys
from pathlib import Path

from rich.console import Console

from airflow_breeze.global_constants import PACKAGES_METADATA_EXCLUDE_NAMES

console = Console(color_system="standard")

AIRFLOW_SITE_DIRECTORY = os.environ.get("AIRFLOW_SITE_DIRECTORY")

error_versions: list[str] = []

if AIRFLOW_SITE_DIRECTORY and "docs-archive" not in AIRFLOW_SITE_DIRECTORY:
    AIRFLOW_SITE_DIRECTORY = os.path.join(Path(AIRFLOW_SITE_DIRECTORY), "docs-archive")


def validate_docs_version() -> None:
    """
    Validate the versions of documentation packages in the specified directory.

    This script checks the versions of documentation packages in the published directory
    when we publish and add back-references to the documentation. the directory is expected to be structured like:
    docs-archive/
                apache-airflow/
                    1.10.0/
                    stable/
                    stable.txt
                apache-airflow-providers-standard/
                    2.0.0/
                    stable/
                    stable.txt

    If anything found apart from the expected structure, it will cause error to redirects urls or publishing the documentation to s3
    """
    doc_packages = os.listdir(AIRFLOW_SITE_DIRECTORY)

    if not doc_packages:
        console.print("[red]No documentation packages found in the specified directory.[/red]")
        return

    package_version_map = {}

    for package in doc_packages:
        if package in PACKAGES_METADATA_EXCLUDE_NAMES:
            console.print(f"[yellow]Skipping excluded package: {package}[/yellow]")
            continue

        package_path = os.path.join(str(AIRFLOW_SITE_DIRECTORY), package)
        versions = [v for v in os.listdir(package_path) if v != "stable" and v != "stable.txt"]
        if versions:
            package_version_map[package] = get_all_versions(package, versions)

    if error_versions:
        console.print("[red]Errors found in version validation:[/red]")
        for error in error_versions:
            console.print(f"[red]{error}[/red]")
        console.print(
            "[blue]These errors could be due to invalid redirects present in the doc packages.[/blue]"
        )
        sys.exit(1)

    console.print("[green]All versions validated successfully![/green]")
    console.print(f"[blue] {json.dumps(package_version_map, indent=2)} [/blue]")


def get_all_versions(package_name: str, versions: list[str]) -> list[str]:
    from packaging.version import Version

    good_versions = []
    for version in versions:
        try:
            Version(version)
            good_versions.append(version)
        except ValueError as e:
            error_versions.append(f"{e} found under doc folder {package_name}")
    return sorted(
        good_versions,
        key=lambda d: Version(d),
    )


if __name__ == "__main__":
    console.print("[blue]Validating documentation versions...[/blue]")

    if AIRFLOW_SITE_DIRECTORY is None:
        console.print(
            "[red]AIRFLOW_SITE_DIRECTORY environment variable is not set. "
            "Please set it to the directory containing the Airflow site files.[red]"
        )
        sys.exit(1)

    validate_docs_version()
