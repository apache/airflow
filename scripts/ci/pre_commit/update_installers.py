#!/usr/bin/env python
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

import os
import re
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH, console

FILES_TO_UPDATE = [
    AIRFLOW_SOURCES_ROOT_PATH / "Dockerfile",
    AIRFLOW_SOURCES_ROOT_PATH / "Dockerfile.ci",
    AIRFLOW_SOURCES_ROOT_PATH / "scripts" / "ci" / "install_breeze.sh",
    AIRFLOW_SOURCES_ROOT_PATH / "scripts" / "docker" / "common.sh",
    AIRFLOW_SOURCES_ROOT_PATH / "pyproject.toml",
    AIRFLOW_SOURCES_ROOT_PATH / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py",
    AIRFLOW_SOURCES_ROOT_PATH
    / "dev"
    / "breeze"
    / "src"
    / "airflow_breeze"
    / "commands"
    / "release_management_commands.py",
]


DOC_FILES_TO_UPDATE: list[Path] = [
    AIRFLOW_SOURCES_ROOT_PATH / "dev/" / "breeze" / "doc" / "ci" / "02_images.md"
]


def get_latest_pypi_version(package_name: str) -> str:
    response = requests.get(f"https://pypi.org/pypi/{package_name}/json")
    response.raise_for_status()  # Ensure we got a successful response
    data = response.json()
    latest_version = data["info"]["version"]  # The version info is under the 'info' key
    return latest_version


AIRFLOW_PIP_PATTERN = re.compile(r"(AIRFLOW_PIP_VERSION=)([0-9.]+)")
AIRFLOW_PIP_QUOTED_PATTERN = re.compile(r"(AIRFLOW_PIP_VERSION = )(\"[0-9.]+\")")
PIP_QUOTED_PATTERN = re.compile(r"(PIP_VERSION = )(\"[0-9.]+\")")
AIRFLOW_PIP_DOC_PATTERN = re.compile(r"(\| *`AIRFLOW_PIP_VERSION` *\| *)(`[0-9.]+`)( *\|)")
AIRFLOW_PIP_UPGRADE_PATTERN = re.compile(r"(python -m pip install --upgrade pip==)([0-9.]+)")

AIRFLOW_UV_PATTERN = re.compile(r"(AIRFLOW_UV_VERSION=)([0-9.]+)")
AIRFLOW_UV_QUOTED_PATTERN = re.compile(r"(AIRFLOW_UV_VERSION = )(\"[0-9.]+\")")
AIRFLOW_UV_DOC_PATTERN = re.compile(r"(\| *`AIRFLOW_UV_VERSION` *\| *)(`[0-9.]+`)( *\|)")
UV_GREATER_PATTERN = re.compile(r'"(uv>=)([0-9]+)"')

UPGRADE_UV: bool = os.environ.get("UPGRADE_UV", "true").lower() == "true"
UPGRADE_PIP: bool = os.environ.get("UPGRADE_PIP", "true").lower() == "true"


def replace_group_2_while_keeping_total_length(pattern: re.Pattern[str], replacement: str, text: str) -> str:
    def replacer(match):
        original_length = len(match.group(2))
        padding = ""
        if len(match.groups()) > 2:
            padding = match.group(3)
            new_length = len(replacement)
            diff = new_length - original_length
            if diff <= 0:
                padding = " " * -diff + padding
            else:
                padding = padding[diff:]
        padded_replacement = match.group(1) + replacement + padding
        return padded_replacement.strip()

    return re.sub(pattern, replacer, text)


if __name__ == "__main__":
    pip_version = get_latest_pypi_version("pip")
    console.print(f"[bright_blue]Latest pip version: {pip_version}")
    uv_version = get_latest_pypi_version("uv")
    console.print(f"[bright_blue]Latest uv version: {uv_version}")

    changed = False
    for file in FILES_TO_UPDATE:
        console.print(f"[bright_blue]Updating {file}")
        file_content = file.read_text()
        new_content = file_content
        if UPGRADE_PIP:
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_PIP_PATTERN, pip_version, new_content
            )
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_PIP_UPGRADE_PATTERN, pip_version, new_content
            )
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_PIP_QUOTED_PATTERN, f'"{pip_version}"', new_content
            )
            new_content = replace_group_2_while_keeping_total_length(
                PIP_QUOTED_PATTERN, f'"{pip_version}"', new_content
            )
        if UPGRADE_UV:
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_UV_PATTERN, uv_version, new_content
            )
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_UV_QUOTED_PATTERN, f'"{uv_version}"', new_content
            )
            new_content = replace_group_2_while_keeping_total_length(
                UV_GREATER_PATTERN, uv_version, new_content
            )
        if new_content != file_content:
            file.write_text(new_content)
            console.print(f"[bright_blue]Updated {file}")
            changed = True
    for file in DOC_FILES_TO_UPDATE:
        console.print(f"[bright_blue]Updating {file}")
        file_content = file.read_text()
        new_content = file_content
        if UPGRADE_PIP:
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_PIP_DOC_PATTERN, f"`{pip_version}`", new_content
            )
        if UPGRADE_UV:
            new_content = replace_group_2_while_keeping_total_length(
                AIRFLOW_UV_DOC_PATTERN, f"`{uv_version}`", new_content
            )
        if new_content != file_content:
            file.write_text(new_content)
            console.print(f"[bright_blue]Updated {file}")
            changed = True
    if changed:
        sys.exit(1)
