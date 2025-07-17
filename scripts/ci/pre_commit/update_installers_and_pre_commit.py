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
from packaging.version import Version

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
    (AIRFLOW_ROOT_PATH / ".github" / "workflows" / "ci-amd.yml", False),
    (AIRFLOW_CORE_ROOT_PATH / "pyproject.toml", False),
]


def get_latest_pypi_version(package_name: str) -> str:
    response = requests.get(
        f"https://pypi.org/pypi/{package_name}/json", headers={"User-Agent": "Python requests"}
    )
    response.raise_for_status()  # Ensure we got a successful response
    data = response.json()
    latest_version = data["info"]["version"]  # The version info is under the 'info' key
    return latest_version


def get_latest_python_version(python_major_minor: str, github_token: str | None) -> str | None:
    latest_version = None
    # Matches versions of vA.B.C and vA.B where C can only be numeric and v is optional
    version_match = re.compile(rf"^v?{python_major_minor}\.?\d*$")
    headers = {"User-Agent": "Python requests"}
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    for i in range(5):
        response = requests.get(
            f"https://api.github.com/repos/python/cpython/tags?per_page=100&page={i + 1}",
            headers=headers,
        )
        response.raise_for_status()  # Ensure we got a successful response
        data = response.json()
        versions = [str(tag["name"]) for tag in data if version_match.match(tag.get("name", ""))]
        if versions:
            latest_version = sorted(versions, key=Version, reverse=True)[0]
            break
    return latest_version


def get_latest_golang_version() -> str:
    response = requests.get("https://go.dev/dl/?mode=json")
    response.raise_for_status()  # Ensure we got a successful response
    versions = response.json()
    stable_versions = [release["version"].replace("go", "") for release in versions if release["stable"]]
    return sorted(stable_versions, key=Version, reverse=True)[0]


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

PYTHON_PATTERNS: list[tuple[str, Quoting]] = [
    (r"(\"{python_major_minor}\": \")(v[0-9.]+)(\")", Quoting.UNQUOTED),
]

GOLANG_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(GOLANG_MAJOR_MINOR_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(\| *`GOLANG_MAJOR_MINOR_VERSION` *\| *)(`[0-9.]+`)( *\|)"), Quoting.REVERSE_SINGLE_QUOTED),
]

UV_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_UV_VERSION=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(uv>=)([0-9.]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_UV_VERSION = )(\"[0-9.]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"^(\s*UV_VERSION = )(\"[0-9.]+\")", re.MULTILINE), Quoting.DOUBLE_QUOTED),
    (re.compile(r"^(\s*UV_VERSION=)(\"[0-9.]+\")", re.MULTILINE), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(\| *`AIRFLOW_UV_VERSION` *\| *)(`[0-9.]+`)( *\|)"), Quoting.REVERSE_SINGLE_QUOTED),
    (
        re.compile(
            r"(\")([0-9.]+)(\"  # Keep this comment to "
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
            r"(\")([0-9.]+)(\"  # Keep this comment to allow automatic "
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
            r"(\")([0-9.]+)(\"  # Keep this comment to allow automatic "
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
UPGRADE_PYTHON: bool = os.environ.get("UPGRADE_PYTHON", "true").lower() == "true"
UPGRADE_GOLANG: bool = os.environ.get("UPGRADE_GOLANG", "true").lower() == "true"
UPGRADE_SETUPTOOLS: bool = os.environ.get("UPGRADE_SETUPTOOLS", "true").lower() == "true"
UPGRADE_PRE_COMMIT: bool = os.environ.get("UPGRADE_PRE_COMMIT", "true").lower() == "true"
UPGRADE_NODE_LTS: bool = os.environ.get("UPGRADE_NODE_LTS", "true").lower() == "true"
UPGRADE_HATCH: bool = os.environ.get("UPGRADE_HATCH", "true").lower() == "true"
UPGRADE_PYYAML: bool = os.environ.get("UPGRADE_PYYAML", "true").lower() == "true"
UPGRADE_GITPYTHON: bool = os.environ.get("UPGRADE_GITPYTHON", "true").lower() == "true"
UPGRADE_RICH: bool = os.environ.get("UPGRADE_RICH", "true").lower() == "true"

ALL_PYTHON_MAJOR_MINOR_VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]

GITHUB_TOKEN: str | None = os.environ.get("GITHUB_TOKEN")


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
    if not GITHUB_TOKEN:
        console.print(
            "[red]GITHUB_TOKEN environment variable is not set. This will lead to failures on rate limits.[/]\n"
            "Please set it to a valid GitHub token with public_repo scope. You can create one by clicking "
            "the URL:\n\n"
            "https://github.com/settings/tokens/new?scopes=public_repo&description=airflow-update-installers-and-pre-commit\n\n"
            "Once you have the token you can prepend pre-commit command with GITHUB_TOKEN='<your token>' or"
            "set it in your environment with export GITHUB_TOKEN='<your token>'\n\n"
        )
        sys.exit(1)
    changed = False
    golang_version = get_latest_golang_version()
    pip_version = get_latest_pypi_version("pip")
    uv_version = get_latest_pypi_version("uv")
    setuptools_version = get_latest_pypi_version("setuptools")
    pre_commit_version = get_latest_pypi_version("pre-commit")
    pre_commit_uv_version = get_latest_pypi_version("pre-commit-uv")
    hatch_version = get_latest_pypi_version("hatch")
    pyyaml_version = get_latest_pypi_version("PyYAML")
    gitpython_version = get_latest_pypi_version("GitPython")
    rich_version = get_latest_pypi_version("rich")
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
        if UPGRADE_PYTHON:
            for python_version in ALL_PYTHON_MAJOR_MINOR_VERSIONS:
                latest_python_version = get_latest_python_version(python_version, GITHUB_TOKEN)
                if latest_python_version:
                    console.print(
                        f"[bright_blue]Latest python {python_version} version: {latest_python_version}"
                    )
                    for line_format, quoting in PYTHON_PATTERNS:
                        line_pattern = re.compile(line_format.format(python_major_minor=python_version))
                        console.print(line_pattern)
                        new_content = replace_version(
                            line_pattern,
                            get_replacement(latest_python_version, quoting),
                            new_content,
                            keep_length,
                        )
        if UPGRADE_GOLANG:
            console.print(f"[bright_blue]Latest golang version: {golang_version}")
            for line_pattern, quoting in GOLANG_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(golang_version, quoting), new_content, keep_length
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
        if UPGRADE_HATCH:
            console.print(f"[bright_blue]Latest hatch version: {hatch_version}")
            new_content = re.sub(
                r"(HATCH_VERSION = )(\"[0-9.]+\")",
                f'HATCH_VERSION = "{hatch_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(HATCH_VERSION=)(\"[0-9.]+\")",
                f'HATCH_VERSION="{hatch_version}"',
                new_content,
            )
        if UPGRADE_PYYAML:
            console.print(f"[bright_blue]Latest PyYAML version: {pyyaml_version}")
            new_content = re.sub(
                r"(PYYAML_VERSION = )(\"[0-9.]+\")",
                f'PYYAML_VERSION = "{pyyaml_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(PYYAML_VERSION=)(\"[0-9.]+\")",
                f'PYYAML_VERSION="{pyyaml_version}"',
                new_content,
            )
        if UPGRADE_GITPYTHON:
            console.print(f"[bright_blue]Latest GitPython version: {gitpython_version}")
            new_content = re.sub(
                r"(GITPYTHON_VERSION = )(\"[0-9.]+\")",
                f'GITPYTHON_VERSION = "{gitpython_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(GITPYTHON_VERSION=)(\"[0-9.]+\")",
                f'GITPYTHON_VERSION="{gitpython_version}"',
                new_content,
            )
        if UPGRADE_RICH:
            console.print(f"[bright_blue]Latest rich version: {rich_version}")
            new_content = re.sub(
                r"(RICH_VERSION = )(\"[0-9.]+\")",
                f'RICH_VERSION = "{rich_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(RICH_VERSION=)(\"[0-9.]+\")",
                f'RICH_VERSION="{rich_version}"',
                new_content,
            )
        if new_content != file_content:
            file.write_text(new_content)
            console.print(f"[bright_blue]Updated {file}")
            changed = True
    if changed:
        sys.exit(1)
