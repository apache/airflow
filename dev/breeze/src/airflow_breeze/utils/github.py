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

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rich.markup import escape

from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.shared_options import get_dry_run


def get_ga_output(name: str, value: Any) -> str:
    output_name = name.replace("_", "-")
    printed_value = str(value).lower() if isinstance(value, bool) else value
    get_console().print(
        f"[info]{output_name}[/] = [green]{escape(str(printed_value))}[/]"
    )
    return f"{output_name}={printed_value}"


def download_file_from_github(
    tag: str, path: str, output_file: Path, timeout: int = 60
) -> bool:
    """
    Downloads a file from GitHub repository of Apache Airflow

    :param tag: tag to download from
    :param path: path of the file relative to the repository root
    :param output_file: Path where the file should be downloaded
    :param timeout: timeout in seconds for the download request, default is 60 seconds
    :return: whether the file was successfully downloaded (False if the file is missing or error occurred)
    """
    import requests

    url = f"https://raw.githubusercontent.com/apache/airflow/{tag}/{path}"
    get_console().print(f"[info]Downloading {url} to {output_file}")
    if not get_dry_run():
        try:
            response = requests.get(url, timeout=timeout)
            if response.status_code == 404:
                get_console().print(f"[warning]The {url} has not been found. Skipping")
                return False
            if response.status_code != 200:
                get_console().print(
                    f"[error]{url} could not be downloaded. Status code {response.status_code}"
                )
                return False
            output_file.write_bytes(response.content)
        except requests.Timeout:
            get_console().print(
                f"[error]The request to {url} timed out after {timeout} seconds."
            )
            return False
    get_console().print(f"[success]Downloaded {url} to {output_file}")
    return True


ACTIVE_TAG_MATCH = re.compile(r"^(\d+)\.\d+\.\d+$")


def get_active_airflow_versions(confirm: bool = True) -> tuple[list[str], dict[str, str]]:
    """
    Gets list of active Airflow versions from GitHub.

    :param confirm: if True, will ask the user before proceeding with the versions found
    :return: tuple: list of active Airflow versions and dict of Airflow release dates (in iso format)
    """
    from git import GitCommandError, Repo
    from packaging.version import Version

    airflow_release_dates: dict[str, str] = {}
    get_console().print(
        "\n[warning]Make sure you have `apache` remote added pointing to apache/airflow repository\n"
    )
    get_console().print("[info]Fetching all released Airflow 2 versions from GitHub[/]\n")
    repo = Repo(AIRFLOW_SOURCES_ROOT)
    all_active_tags: list[str] = []
    try:
        ref_tags = repo.git.ls_remote("--tags", "apache").splitlines()
    except GitCommandError as ex:
        get_console().print(
            "[error]Could not fetch tags from `apache` remote! Make sure to have it configured.\n"
        )
        get_console().print(f"{ex}\n")
        get_console().print(
            "[info]You can add apache remote with on of those commands (depend which protocol you use):\n"
            " * git remote add apache https://github.com/apache/airflow.git\n"
            " * git remote add apache git@github.com:apache/airflow.git\n"
        )
        sys.exit(1)
    tags = [tag.split("refs/tags/")[1].strip() for tag in ref_tags if "refs/tags/" in tag]
    for tag in tags:
        match = ACTIVE_TAG_MATCH.match(tag)
        if match and match.group(1) == "2":
            all_active_tags.append(tag)
    airflow_versions = sorted(all_active_tags, key=Version)
    for version in airflow_versions:
        date = get_tag_date(version)
        if not date:
            get_console().print("[error]Error fetching tag date for Airflow {version}")
            sys.exit(1)
        airflow_release_dates[version] = date
    get_console().print("[info]All Airflow 2 versions")
    for version in airflow_versions:
        get_console().print(f"  {version}: [info]{airflow_release_dates[version]}[/]")
    if confirm:
        answer = user_confirm(
            "Should we continue with those versions?",
            quit_allowed=False,
            default_answer=Answer.YES,
        )
        if answer == Answer.NO:
            get_console().print("[red]Aborting[/]")
            sys.exit(1)
    return airflow_versions, airflow_release_dates


def download_constraints_file(
    airflow_version: str,
    python_version: str,
    include_provider_dependencies: bool,
    output_file: Path,
) -> bool:
    """
    Downloads constraints file from GitHub repository of Apache Airflow

    :param airflow_version: airflow version
    :param python_version: python version
    :param include_provider_dependencies: whether to include provider dependencies
    :param output_file: the file where to store the constraint file
    :return: true if the file was successfully downloaded
    """
    if include_provider_dependencies:
        constraints_file_path = f"constraints-{python_version}.txt"
    else:
        constraints_file_path = f"constraints-no-providers-{python_version}.txt"
    constraints_tag = f"constraints-{airflow_version}"
    return download_file_from_github(
        tag=constraints_tag,
        path=constraints_file_path,
        output_file=output_file,
    )


def get_tag_date(tag: str) -> str | None:
    """
    Returns UTC timestamp of the tag in the repo in iso time format 8601
    :param tag: tag to get date for
    :return: iso time format 8601 of the tag date
    """
    from git import Repo

    repo = Repo(AIRFLOW_SOURCES_ROOT)
    try:
        tag_object = repo.tags[tag].object
    except IndexError:
        get_console().print(f"[warning]Tag {tag} not found in the repository")
        return None
    timestamp: int = (
        tag_object.committed_date
        if hasattr(tag_object, "committed_date")
        else tag_object.tagged_date
    )
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
