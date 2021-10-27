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
from typing import List

import click as click
from rich import print

PROVIDERS_DOCKER = """\
FROM apache/airflow:latest

# Install providers
{}
"""

AIRFLOW_DOCKER = """\
FROM apache/airflow:{}

# Upgrade
RUN pip install "apache-airflow=={}"

"""

DOCKER_UPGRADE = """\
FROM apache/airflow:1.10.15

# Install upgrade-check
RUN pip install "apache-airflow-upgrade-check=={}"

"""

DOCKER_CMD = """
docker build --tag local/airflow .
docker local/airflow info
"""

AIRFLOW = "AIRFLOW"
PROVIDERS = "PROVIDERS"
UPGRADE_CHECK = "UPGRADE_CHECK"


def get_packages() -> List[str]:
    try:
        with open("packages.txt") as file:
            content = file.read()
    except FileNotFoundError:
        content = ''
    if not content:
        raise SystemExit("List of packages to check is empty. Please add packages to `packages.txt`")

    packages = [p.replace("* ", "").strip() for p in content.split("\n") if p]
    return packages


def create_docker(txt: str):
    # Generate docker
    with open("Dockerfile.pmc", "w+") as f:
        f.write(txt)

    print("\n[bold]To check installation run:[/bold]")
    print(
        """\
        docker build -f Dockerfile.pmc --tag local/airflow .
        docker run local/airflow info
        """
    )


def check_providers(files: List[str], version: str):
    print(f"Checking providers for version {version}:\n")
    version = strip_rc_suffix(version)
    missing_list = []
    for p in get_packages():
        print(p)
        expected_files = expand_name_variations(
            [
                f"{p}-{version}.tar.gz",
                f"{p.replace('-', '_')}-{version}-py3-none-any.whl",
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


@click.command()
@click.option(
    "--type",
    "-t",
    "check_type",
    prompt="providers, airflow, upgrade_check",
    type=str,
    help="Type of the check to perform. One of: providers, airflow, upgrade_check",
)
@click.option(
    "--version",
    "-v",
    prompt="Version",
    type=str,
    help="Version of package to verify. For example 1.10.15.rc1, 2021.3.17rc1",
)
@click.option(
    "--path",
    "-p",
    prompt="Path to files",
    type=str,
    help="Path to directory where are sources",
)
def main(check_type: str, path: str, version: str):
    """
    Use this tool to verify that all expected packages are present in Apache Airflow svn.
    In case of providers, it will generate Dockerfile.pmc that you can use
    to verify that all packages are installable.

    In case of providers, you should update `packages.txt` file with list of packages
    that you expect to find (copy-paste the list from VOTE thread).

    Example usages:
    python check_files.py -v 1.10.15rc1 -t airflow -p ~/code/airflow_svn
    python check_files.py -v 1.3.0rc2 -t upgrade_check -p ~/code/airflow_svn
    python check_files.py -v 1.0.3rc1 -t providers -p ~/code/airflow_svn
    """

    if check_type.upper() == PROVIDERS:
        files = os.listdir(os.path.join(path, "providers"))
        pips = [f"{p}=={version}" for p in get_packages()]
        missing_files = check_providers(files, version)
        create_docker(PROVIDERS_DOCKER.format("\n".join(f"RUN pip install '{p}'" for p in pips)))
        if missing_files:
            warn_of_missing_files(missing_files)
        return

    if check_type.upper() == AIRFLOW:
        files = os.listdir(os.path.join(path, version))
        missing_files = check_release(files, version)

        base_version = version.split("rc")[0]
        prev_version = base_version[:-1] + str(int(base_version[-1]) - 1)
        create_docker(AIRFLOW_DOCKER.format(prev_version, version))
        if missing_files:
            warn_of_missing_files(missing_files)
        return

    if check_type.upper() == UPGRADE_CHECK:
        files = os.listdir(os.path.join(path, "upgrade-check", version))
        missing_files = check_upgrade_check(files, version)

        create_docker(DOCKER_UPGRADE.format(version))
        if missing_files:
            warn_of_missing_files(missing_files)
        return

    raise SystemExit(f"Unknown check type: {check_type}")


if __name__ == "__main__":
    main()


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
