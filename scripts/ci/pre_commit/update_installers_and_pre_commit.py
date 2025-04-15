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
from enum import Enum
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import AIRFLOW_CORE_ROOT_PATH, AIRFLOW_ROOT_PATH, console

# List of files to update and whether to keep total length of the original value when replacing.
FILES_TO_UPDATE: list[tuple[Path, bool]] = [
    (AIRFLOW_ROOT_PATH / "Dockerfile", False),
    (AIRFLOW_ROOT_PATH / "Dockerfile.ci", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "install_breeze.sh", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "docker" / "common.sh", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "tools" / "setup_breeze", False),
    (AIRFLOW_ROOT_PATH / "pyproject.toml", False),
    (AIRFLOW_ROOT_PATH / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py", False),
    (
        AIRFLOW_ROOT_PATH
        / "dev"
        / "breeze"
        / "src"
        / "airflow_breeze"
        / "commands"
        / "release_management_commands.py",
        False,
    ),
    (AIRFLOW_ROOT_PATH / ".github" / "actions" / "install-pre-commit" / "action.yml", False),
    (AIRFLOW_ROOT_PATH / "dev/" / "breeze" / "doc" / "ci" / "02_images.md", True),
    (AIRFLOW_ROOT_PATH / ".pre-commit-config.yaml", False),
    (AIRFLOW_CORE_ROOT_PATH / "pyproject.toml", False),
]


def get_latest_pypi_version(package_name: str) -> str:
    response = requests.get(f"https://pypi.org/pypi/{package_name}/json")
    response.raise_for_status()  # Ensure we got a successful response
    data = response.json()
    latest_version = data["info"]["version"]  # The version info is under the 'info' key
    return latest_version


def get_latest_lts_node_version() -> str:
    response = requests.get("https://nodejs.org/dist/index.json")
    response.raise_for_status()  # Ensure we got a successful response
    versions = response.json()
    lts_prefix = "v22"
    lts_versions = [version["version"] for version in versions if version["version"].startswith(lts_prefix)]
    # The json array is sorted from newest to oldest, so the first element is the latest LTS version
    # Skip leading v in version
    return lts_versions[0][1:]


class Quoting(Enum):
    UNQUOTED = 0
    SINGLE_QUOTED = 1
    DOUBLE_QUOTED = 2
    REVERSE_SINGLE_QUOTED = 3


PIP_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_PIP_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(python -m pip install --upgrade pip==)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_PIP_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PIP_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PIP_VERSION=)(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(\| *`AIRFLOW_PIP_VERSION` *\| *)(`[0-9.]+`)( *\|)"), Quoting.REVERSE_SINGLE_QUOTED),
]

UV_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_UV_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(uv>=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_UV_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(UV_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(UV_VERSION=)(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(\| *`AIRFLOW_UV_VERSION` *\| *)(`[0-9.]+`)( *\|)"), Quoting.REVERSE_SINGLE_QUOTED),
    (
        re.compile(
            r"(default: \")([0-9.]+)(\"  # Keep this comment to "
            r"allow automatic replacement of uv version)"
        ),
        Quoting.UNQUOTED,
    ),
]

SETUPTOOLS_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_SETUPTOOLS_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
]

PRE_COMMIT_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_PRE_COMMIT_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_PRE_COMMIT_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(pre-commit>=)([0-9]+)"), Quoting.UNQUOTED),
    (re.compile(r"(PRE_COMMIT_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PRE_COMMIT_VERSION=)(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (
        re.compile(r"(\| *`AIRFLOW_PRE_COMMIT_VERSION` *\| *)(`[0-9.]+`)( *\|)"),
        Quoting.REVERSE_SINGLE_QUOTED,
    ),
    (
        re.compile(
            r"(default: \")([0-9.]+)(\"  # Keep this comment to allow automatic "
            r"replacement of pre-commit version)"
        ),
        Quoting.UNQUOTED,
    ),
]

PRE_COMMIT_UV_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_PRE_COMMIT_UV_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_PRE_COMMIT_UV_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(pre-commit-uv>=)([0-9]+)"), Quoting.UNQUOTED),
    (re.compile(r"(PRE_COMMIT_UV_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PRE_COMMIT_UV_VERSION=)(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (
        re.compile(r"(\| *`AIRFLOW_PRE_COMMIT_UV_VERSION` *\| *)(`[0-9.]+`)( *\|)"),
        Quoting.REVERSE_SINGLE_QUOTED,
    ),
    (
        re.compile(
            r"(default: \")([0-9.]+)(\"  # Keep this comment to allow automatic "
            r"replacement of pre-commit-uv version)"
        ),
        Quoting.UNQUOTED,
    ),
]

NODE_LTS_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"( *node: )([0-9.]+)"), Quoting.UNQUOTED),
]


def get_replacement(value: str, quoting: Quoting) -> str:
    if quoting == Quoting.DOUBLE_QUOTED:
        return f'"{value}"'
    if quoting == Quoting.SINGLE_QUOTED:
        return f"'{value}'"
    if quoting == Quoting.REVERSE_SINGLE_QUOTED:
        return f"`{value}`"
    return value


UPGRADE_UV: bool = os.environ.get("UPGRADE_UV", "true").lower() == "true"
UPGRADE_PIP: bool = os.environ.get("UPGRADE_PIP", "true").lower() == "true"
UPGRADE_SETUPTOOLS: bool = os.environ.get("UPGRADE_SETUPTOOLS", "true").lower() == "true"
UPGRADE_PRE_COMMIT: bool = os.environ.get("UPGRADE_PRE_COMMIT", "true").lower() == "true"
UPGRADE_NODE_LTS: bool = os.environ.get("UPGRADE_NODE_LTS", "true").lower() == "true"


def replace_version(pattern: re.Pattern[str], version: str, text: str, keep_total_length: bool = True) -> str:
    # Assume that the pattern has up to 3 replacement groups:
    # 1. Prefix
    # 2. Original version
    # 3. Suffix
    #
    # (prefix)(version)(suffix)
    # In case "keep_total_length" is set to True, the replacement will be padded with spaces to match
    # the original length
    def replacer(match):
        prefix = match.group(1)
        postfix = match.group(3) if len(match.groups()) > 2 else ""
        if not keep_total_length:
            return prefix + version + postfix
        original_length = len(match.group(2))
        new_length = len(version)
        diff = new_length - original_length
        if diff <= 0:
            postfix = " " * -diff + postfix
        else:
            postfix = postfix[diff:]
        padded_replacement = prefix + version + postfix
        return padded_replacement.strip()

    return re.sub(pattern, replacer, text)


if __name__ == "__main__":
    changed = False
    pip_version = get_latest_pypi_version("pip")
    uv_version = get_latest_pypi_version("uv")
    setuptools_version = get_latest_pypi_version("setuptools")
    pre_commit_version = get_latest_pypi_version("pre-commit")
    pre_commit_uv_version = get_latest_pypi_version("pre-commit-uv")
    node_lts_version = get_latest_lts_node_version()
    for file, keep_length in FILES_TO_UPDATE:
        console.print(f"[bright_blue]Updating {file}")
        file_content = file.read_text()
        new_content = file_content
        if UPGRADE_PIP:
            console.print(f"[bright_blue]Latest pip version: {pip_version}")
            for line_pattern, quoting in PIP_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(pip_version, quoting), new_content, keep_length
                )
        if UPGRADE_SETUPTOOLS:
            console.print(f"[bright_blue]Latest setuptools version: {setuptools_version}")
            for line_pattern, quoting in SETUPTOOLS_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(setuptools_version, quoting), new_content, keep_length
                )
        if UPGRADE_UV:
            console.print(f"[bright_blue]Latest uv version: {uv_version}")
            for line_pattern, quoting in UV_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(uv_version, quoting), new_content, keep_length
                )
        if UPGRADE_PRE_COMMIT:
            console.print(f"[bright_blue]Latest pre-commit version: {pre_commit_version}")
            for line_pattern, quoting in PRE_COMMIT_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(pre_commit_version, quoting), new_content, keep_length
                )
            if UPGRADE_UV:
                console.print(f"[bright_blue]Latest pre-commit-uv version: {pre_commit_uv_version}")
                for line_pattern, quoting in PRE_COMMIT_UV_PATTERNS:
                    new_content = replace_version(
                        line_pattern,
                        get_replacement(pre_commit_uv_version, quoting),
                        new_content,
                        keep_length,
                    )
        if UPGRADE_NODE_LTS:
            console.print(f"[bright_blue]Latest Node LTS version: {node_lts_version}")
            for line_pattern, quoting in NODE_LTS_PATTERNS:
                new_content = replace_version(
                    line_pattern,
                    get_replacement(node_lts_version, quoting),
                    new_content,
                    keep_length,
                )
        if new_content != file_content:
            file.write_text(new_content)
            console.print(f"[bright_blue]Updated {file}")
            changed = True
    if changed:
        sys.exit(1)
