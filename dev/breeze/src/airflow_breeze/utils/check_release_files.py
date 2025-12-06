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

import itertools
import re
from pathlib import Path

from airflow_breeze.utils.console import get_console

PROVIDERS_DOCKER = """\
FROM ghcr.io/apache/airflow/main/ci/python3.10
RUN cd airflow-core; uv sync --no-sources

# Install providers
{}
"""

AIRFLOW_DOCKER = """\
FROM python:3.10

# Upgrade
RUN pip install "apache-airflow=={}"

"""

TASK_SDK_DOCKER = """\
FROM python:3.10

# Upgrade
RUN pip install "apache-airflow-task-sdk=={}"

"""

AIRFLOW_CTL_DOCKER = """\
FROM python:3.10

# Install airflow-ctl
RUN pip install "apache-airflow-ctl=={}"

"""

PYTHON_CLIENT_DOCKER = """\
FROM python:3.10

# Install python-client
RUN pip install "apache-airflow-python-client=={}"

"""


def get_packages(packages_file: Path) -> list[tuple[str, str]]:
    """Read packages from packages.txt file."""
    try:
        content = packages_file.read_text()
    except FileNotFoundError:
        raise SystemExit(f"List of packages to check is empty. Please add packages to `{packages_file}`")

    if not content:
        raise SystemExit(f"List of packages to check is empty. Please add packages to `{packages_file}`")

    # e.g. https://pypi.org/project/apache-airflow-providers-airbyte/3.1.0rc1/
    packages = []
    for line in content.splitlines():
        if line:
            _, name, version = line.rstrip("/").rsplit("/", 2)
            packages.append((name, version))

    return packages


def create_docker(txt: str, output_file: Path):
    """Generate Dockerfile for testing installation."""
    output_file.write_text(txt)

    console = get_console()
    console.print("\n[bold]To check installation run:[/bold]")
    console.print(
        f"""\
        docker build -f {output_file} --tag local/airflow .
        docker run --rm --entrypoint "airflow" local/airflow info
        docker image rm local/airflow
        """
    )


def check_providers(files: list[str], release_date: str, packages: list[tuple[str, str]]) -> list[str]:
    """Check if all expected provider files are present."""
    console = get_console()
    console.print("Checking providers from packages.txt:\n")
    missing_list = []
    expected_files = expand_name_variations(
        [
            f"apache_airflow_providers-{release_date}-source.tar.gz",
        ]
    )
    missing_list.extend(check_all_files(expected_files=expected_files, actual_files=files))
    for name, version_raw in packages:
        console.print(f"Checking {name} {version_raw}")
        version = strip_rc_suffix(version_raw)
        expected_files = expand_name_variations(
            [
                f"{name.replace('-', '_')}-{version}.tar.gz",
                f"{name.replace('-', '_')}-{version}-py3-none-any.whl",
            ]
        )

        missing_list.extend(check_all_files(expected_files=expected_files, actual_files=files))

    return missing_list


def strip_rc_suffix(version: str) -> str:
    """Remove rc suffix from version string."""
    return re.sub(r"rc\d+$", "", version)


def print_status(file: str, is_found: bool):
    """Print status of a file check."""
    console = get_console()
    color, status = ("green", "OK") if is_found else ("red", "MISSING")
    console.print(f"    - {file}: [{color}]{status}[/{color}]")


def check_all_files(actual_files: list[str], expected_files: list[str]) -> list[str]:
    """Check if all expected files are in actual files list."""
    missing_list = []
    for file in expected_files:
        is_found = file in actual_files
        if not is_found:
            missing_list.append(file)
        print_status(file=file, is_found=is_found)
    return missing_list


def check_airflow_release(files: list[str], version: str) -> list[str]:
    """Check if all expected Airflow release files are present."""
    console = get_console()
    console.print(f"Checking airflow release for version {version}:\n")
    version = strip_rc_suffix(version)

    expected_files = expand_name_variations(
        [
            f"apache_airflow-{version}.tar.gz",
            f"apache_airflow-{version}-source.tar.gz",
            f"apache_airflow-{version}-py3-none-any.whl",
            f"apache_airflow_core-{version}.tar.gz",
            f"apache_airflow_core-{version}-py3-none-any.whl",
        ]
    )
    return check_all_files(expected_files=expected_files, actual_files=files)


def check_task_sdk_release(files: list[str], version: str) -> list[str]:
    """Check if all expected task-sdk release files are present."""
    console = get_console()
    console.print(f"Checking task-sdk release for version {version}:\n")
    version = strip_rc_suffix(version)

    expected_files = expand_name_variations(
        [
            f"apache_airflow_task_sdk-{version}.tar.gz",
            f"apache_airflow_task_sdk-{version}-py3-none-any.whl",
        ]
    )
    return check_all_files(expected_files=expected_files, actual_files=files)


def expand_name_variations(files: list[str]) -> list[str]:
    """Expand file names to include signature and checksum variations."""
    return sorted(base + suffix for base, suffix in itertools.product(files, ["", ".asc", ".sha512"]))


def check_airflow_ctl_release(files: list[str], version: str) -> list[str]:
    """Check if all expected airflow-ctl release files are present."""
    console = get_console()
    console.print(f"Checking airflow-ctl release for version {version}:\n")
    version = strip_rc_suffix(version)

    expected_files = expand_name_variations(
        [
            f"apache_airflow_ctl-{version}-source.tar.gz",
            f"apache_airflow_ctl-{version}.tar.gz",
            f"apache_airflow_ctl-{version}-py3-none-any.whl",
        ]
    )
    return check_all_files(expected_files=expected_files, actual_files=files)


def check_python_client_release(files: list[str], version: str) -> list[str]:
    """Check if all expected python-client release files are present."""
    console = get_console()
    console.print(f"Checking python-client release for version {version}:\n")
    version = strip_rc_suffix(version)

    expected_files = expand_name_variations(
        [
            f"apache_airflow_python_client-{version}-source.tar.gz",
            f"apache_airflow_client-{version}.tar.gz",
            f"apache_airflow_client-{version}-py3-none-any.whl",
        ]
    )
    return check_all_files(expected_files=expected_files, actual_files=files)


def warn_of_missing_files(files: list[str], directory: str):
    """Print warning message for missing files."""
    console = get_console()
    console.print(
        f"[red]Check failed. Here are the files we expected but did not find in {directory}:[/red]\n"
    )

    for file in files:
        console.print(f"    - [red]{file}[/red]")
