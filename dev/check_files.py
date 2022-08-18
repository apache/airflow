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
import re
from itertools import product
from typing import List, Tuple

import rich_click as click
from rich import print

PROVIDERS_DOCKER = """\
FROM apache/airflow:latest

# Install providers
{}
"""

AIRFLOW_DOCKER = """\
FROM python:3.7

# Upgrade
RUN pip install "apache-airflow=={}"

"""

DOCKER_UPGRADE = """\
FROM apache/airflow:1.10.15

# Install upgrade-check
RUN pip install "apache-airflow-upgrade-check=={}"

"""

AIRFLOW = "AIRFLOW"
PROVIDERS = "PROVIDERS"
UPGRADE_CHECK = "UPGRADE_CHECK"


def get_packages() -> List[Tuple[str, str]]:
    try:
        with open("packages.txt") as file:
            content = file.read()
    except FileNotFoundError:
        content = ''
    if not content:
        raise SystemExit("List of packages to check is empty. Please add packages to `packages.txt`")

    # e.g. https://pypi.org/project/apache-airflow-providers-airbyte/3.1.0rc1/

    packages = []
    for line in content.split("\n"):
        if not line:
            continue
        name, version = line.rstrip("/").split("/")[-2:]
        packages.append((name, version))

    return packages


def create_docker(txt: str):
    # Generate docker
    with open("Dockerfile.pmc", "w+") as f:
        f.write(txt)

    print("\n[bold]To check installation run:[/bold]")
    print(
        """\
        docker build -f Dockerfile.pmc --tag local/airflow .
        docker run --rm local/airflow airflow info
        """
    )


def check_providers(files: List[str]):
    print("Checking providers from packages.txt:\n")
    missing_list = []
    for name, version in get_packages():
        print(f"Checking {name} {version}")
        version = strip_rc_suffix(version)
        expected_files = expand_name_variations(
            [
                f"{name}-{version}.tar.gz",
                f"{name.replace('-', '_')}-{version}-py3-none-any.whl",
            ]
        )

        missing_list.extend(check_all_files(expected_files=expected_files, actual_files=files))

    return missing_list


def strip_rc_suffix(version):
    return re.sub(r'rc\d+$', '', version)


def print_status(file, is_found: bool):
    color, status = ('green', 'OK') if is_found else ('red', 'MISSING')
    print(f"    - {file}: [{color}]{status}[/{color}]")


def check_all_files(actual_files, expected_files):
    missing_list = []
    for file in expected_files:
        is_found = file in actual_files
        if not is_found:
            missing_list.append(file)
        print_status(file=file, is_found=is_found)
    return missing_list


def check_release(files: List[str], version: str):
    print(f"Checking airflow release for version {version}:\n")
    version = strip_rc_suffix(version)

    expected_files = expand_name_variations(
        [
            f"apache-airflow-{version}.tar.gz",
            f"apache-airflow-{version}-source.tar.gz",
            f"apache_airflow-{version}-py3-none-any.whl",
        ]
    )
    return check_all_files(expected_files=expected_files, actual_files=files)


def expand_name_variations(files):
    return list(sorted(base + suffix for base, suffix in product(files, ['', '.asc', '.sha512'])))


def check_upgrade_check(files: List[str], version: str):
    print(f"Checking upgrade_check for version {version}:\n")
    version = strip_rc_suffix(version)

    expected_files = expand_name_variations(
        [
            f"apache-airflow-upgrade-check-{version}-bin.tar.gz",
            f"apache-airflow-upgrade-check-{version}-source.tar.gz",
            f"apache_airflow_upgrade_check-{version}-py2.py3-none-any.whl",
        ]
    )
    return check_all_files(expected_files=expected_files, actual_files=files)


def warn_of_missing_files(files):
    print("[red]Check failed. Here are the files we expected but did not find:[/red]\n")

    for file in files:
        print(f"    - [red]{file}[/red]")


path_option = click.option(
    "--path",
    "-p",
    prompt="Path to files",
    type=str,
    help="Path to directory where are sources",
)
version_option = click.option(
    "--version",
    "-v",
    prompt="Version",
    type=str,
    help="Version of package to verify. For example 1.10.15.rc1, 2021.3.17rc1",
)


@click.group()
def cli():
    """
    Use this tool to verify that all expected packages are present in Apache Airflow svn.
    In case of providers, it will generate Dockerfile.pmc that you can use
    to verify that all packages are installable.

    In case of providers, you should update `packages.txt` file with list of packages
    that you expect to find (copy-paste the list from VOTE thread).

    Example usages:
    python check_files.py airflow -p ~/code/airflow_svn -v 1.10.15rc1
    python check_files.py upgrade_check -p ~/code/airflow_svn -v 1.3.0rc2
    python check_files.py providers -p ~/code/airflow_svn
    """


@click.command()
@path_option
@click.pass_context
def providers(ctx, path: str):
    files = os.listdir(os.path.join(path, "providers"))
    pips = [f"{name}=={version}" for name, version in get_packages()]
    missing_files = check_providers(files)
    create_docker(PROVIDERS_DOCKER.format("\n".join(f"RUN pip install '{p}'" for p in pips)))
    if missing_files:
        warn_of_missing_files(missing_files)


@click.command()
@path_option
@version_option
@click.pass_context
def airflow(ctx, path: str, version: str):
    files = os.listdir(os.path.join(path, version))
    missing_files = check_release(files, version)
    create_docker(AIRFLOW_DOCKER.format(version))
    if missing_files:
        warn_of_missing_files(missing_files)
    return


@click.command()
@path_option
@version_option
@click.pass_context
def upgrade_check(ctx, path: str, version: str):
    files = os.listdir(os.path.join(path, "upgrade-check", version))
    missing_files = check_upgrade_check(files, version)

    create_docker(DOCKER_UPGRADE.format(version))
    if missing_files:
        warn_of_missing_files(missing_files)
    return


cli.add_command(providers)
cli.add_command(airflow)
cli.add_command(upgrade_check)

if __name__ == "__main__":
    cli()


def test_check_release_pass():
    """Passes if all present"""
    files = [
        'apache_airflow-2.2.1-py3-none-any.whl',
        'apache_airflow-2.2.1-py3-none-any.whl.asc',
        'apache_airflow-2.2.1-py3-none-any.whl.sha512',
        'apache-airflow-2.2.1-source.tar.gz',
        'apache-airflow-2.2.1-source.tar.gz.asc',
        'apache-airflow-2.2.1-source.tar.gz.sha512',
        'apache-airflow-2.2.1.tar.gz',
        'apache-airflow-2.2.1.tar.gz.asc',
        'apache-airflow-2.2.1.tar.gz.sha512',
    ]
    assert check_release(files, version='2.2.1rc2') == []


def test_check_release_fail():
    """Fails if missing one"""
    files = [
        'apache_airflow-2.2.1-py3-none-any.whl',
        'apache_airflow-2.2.1-py3-none-any.whl.asc',
        'apache_airflow-2.2.1-py3-none-any.whl.sha512',
        'apache-airflow-2.2.1-source.tar.gz',
        'apache-airflow-2.2.1-source.tar.gz.asc',
        'apache-airflow-2.2.1-source.tar.gz.sha512',
        'apache-airflow-2.2.1.tar.gz.asc',
        'apache-airflow-2.2.1.tar.gz.sha512',
    ]

    missing_files = check_release(files, version='2.2.1rc2')
    assert missing_files == ['apache-airflow-2.2.1.tar.gz']
