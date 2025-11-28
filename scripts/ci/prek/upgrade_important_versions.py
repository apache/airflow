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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "packaging>=25",
#   "pyyaml>=6.0.2",
#   "requests>=2.31.0",
#   "rich>=13.6.0",
# ]
# ///
#
# DEBUGGING
# * You can set UPGRADE_ALL_BY_DEFAULT to "false" to only upgrade those versions that
#   are set by UPGRADE_NNNNNNN (NNNNNN > thing to upgrade version)
# * You can set VERBOSE="true" to see requests being made
# * You can set UPGRADE_NNNNNNN_INCLUDE_PRE_RELEASES="true"

from __future__ import annotations

import os
import re
import subprocess
import sys
from enum import Enum
from pathlib import Path

import requests
from packaging.version import Version

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_CORE_ROOT_PATH, AIRFLOW_ROOT_PATH, console, retrieve_gh_token

DOCKER_IMAGES_EXAMPLE_DIR_PATH = AIRFLOW_ROOT_PATH / "docker-stack-docs" / "docker-examples"


# List of files to update and whether to keep total length of the original value when replacing.
FILES_TO_UPDATE: list[tuple[Path, bool]] = [
    (AIRFLOW_ROOT_PATH / "Dockerfile", False),
    (AIRFLOW_ROOT_PATH / "Dockerfile.ci", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "prek" / "check_imports_in_providers.py", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "prek" / "ruff_format.py", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "ci" / "install_breeze.sh", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "docker" / "common.sh", False),
    (AIRFLOW_ROOT_PATH / "scripts" / "tools" / "setup_breeze", False),
    (AIRFLOW_ROOT_PATH / "pyproject.toml", False),
    (AIRFLOW_ROOT_PATH / ".github" / "workflows" / "airflow-distributions-tests.yml", False),
    (AIRFLOW_ROOT_PATH / "dev" / "breeze" / "pyproject.toml", False),
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
    (AIRFLOW_ROOT_PATH / ".github" / "workflows" / "release_dockerhub_image.yml", False),
    (AIRFLOW_ROOT_PATH / ".github" / "actions" / "install-prek" / "action.yml", False),
    (AIRFLOW_ROOT_PATH / ".github" / "actions" / "breeze" / "action.yml", False),
    (AIRFLOW_ROOT_PATH / ".github" / "workflows" / "basic-tests.yml", False),
    (AIRFLOW_ROOT_PATH / ".github" / "workflows" / "ci-amd-arm.yml", False),
    (AIRFLOW_ROOT_PATH / "dev" / "breeze" / "doc" / "ci" / "02_images.md", True),
    (AIRFLOW_ROOT_PATH / "docker-stack-docs" / "build-arg-ref.rst", True),
    (AIRFLOW_ROOT_PATH / "devel-common" / "pyproject.toml", True),
    (AIRFLOW_ROOT_PATH / "dev" / "breeze" / "pyproject.toml", False),
    (AIRFLOW_ROOT_PATH / ".pre-commit-config.yaml", False),
    (AIRFLOW_CORE_ROOT_PATH / "pyproject.toml", False),
    (AIRFLOW_CORE_ROOT_PATH / "docs" / "best-practices.rst", False),
]
for file in DOCKER_IMAGES_EXAMPLE_DIR_PATH.rglob("*.sh"):
    FILES_TO_UPDATE.append((file, False))


def get_latest_pypi_version(package_name: str, should_upgrade: bool) -> str:
    if not should_upgrade:
        return ""
    if VERBOSE:
        console.print(f"[bright_blue]Fetching latest version for {package_name} from PyPI")
    response = requests.get(
        f"https://pypi.org/pypi/{package_name}/json", headers={"User-Agent": "Python requests"}
    )
    response.raise_for_status()  # Ensure we got a successful response
    data = response.json()
    if os.environ.get(f"UPGRADE_{package_name.upper()}_INCLUDE_PRE_RELEASES", ""):
        latest_version = str(sorted([Version(version) for version in data["releases"].keys()])[-1])
    else:
        latest_version = data["info"]["version"]  # The version info is under the 'info' key
    if VERBOSE:
        console.print(f"[bright_blue]Latest version for {package_name}: {latest_version}")
    return latest_version


def get_all_python_versions() -> list[Version]:
    if VERBOSE:
        console.print("[bright_blue]Fetching all released Python versions from python.org")
    url = "https://www.python.org/api/v2/downloads/release/?is_published=true"
    headers = {"User-Agent": "Python requests"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    versions = []
    matcher = re.compile(r"^Python ([\d.]+$)")
    for release in data:
        release_name = release["name"]
        match = matcher.match(release_name)
        if match:
            versions.append(Version(match.group(1)))
    return versions


def get_latest_python_version(python_major_minor: str, all_versions: list[Version]) -> str:
    """
    Fetch the latest released Python version for a given major.minor (e.g. '3.12') using python.org API.
    Much faster than paginating through all GitHub tags.
    """
    # Only consider releases matching the major.minor.patch pattern
    matching = [
        version for version in all_versions if python_major_minor == f"{version.major}.{version.minor}"
    ]
    if not matching:
        console.print(f"[bright_red]No released Python versions found for {python_major_minor}")
        sys.exit(1)
    # Sort and return the latest version
    latest_version = sorted(matching)[-1]
    if VERBOSE:
        console.print(f"[bright_blue]Latest version for {python_major_minor}: {latest_version}")
    return str(latest_version)


def get_latest_golang_version() -> str:
    if not UPGRADE_GOLANG:
        return ""
    if VERBOSE:
        console.print("[bright_blue]Fetching latest Go version from go.dev")
    response = requests.get("https://go.dev/dl/?mode=json")
    response.raise_for_status()  # Ensure we got a successful response
    versions = response.json()
    stable_versions = [release["version"].replace("go", "") for release in versions if release["stable"]]
    latest_version = sorted(stable_versions, key=Version, reverse=True)[0]
    if VERBOSE:
        console.print(f"[bright_blue]Latest version for Go: {latest_version}")
    return latest_version


def get_latest_lts_node_version() -> str:
    if not UPGRADE_NODE_LTS:
        return ""
    if VERBOSE:
        console.print("[bright_blue]Fetching latest LTS Node version from nodejs.org")
    response = requests.get("https://nodejs.org/dist/index.json")
    response.raise_for_status()  # Ensure we got a successful response
    versions = response.json()
    lts_prefix = "v22"
    lts_versions = [version["version"] for version in versions if version["version"].startswith(lts_prefix)]
    # The json array is sorted from newest to oldest, so the first element is the latest LTS version
    # Skip leading v in version
    latest_version = lts_versions[0][1:]
    if VERBOSE:
        console.print(f"[bright_blue]Latest version for LTS Node: {latest_version}")
    return latest_version


class Quoting(Enum):
    UNQUOTED = 0
    SINGLE_QUOTED = 1
    DOUBLE_QUOTED = 2
    REVERSE_SINGLE_QUOTED = 3
    REVERSE_DOUBLE_QUOTED = 4


PIP_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_PIP_VERSION=)([0-9.abrc]+)"), Quoting.UNQUOTED),
    (re.compile(r"(python -m pip install --upgrade pip==)([0-9.abrc]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_PIP_VERSION = )(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PIP_VERSION = )(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PIP_VERSION=)(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(\| *`AIRFLOW_PIP_VERSION` *\| *)(`[0-9.abrc]+`)( *\|)"), Quoting.REVERSE_SINGLE_QUOTED),
]

PYTHON_PATTERNS: list[tuple[str, Quoting]] = [
    (r"(\"{python_major_minor}\": \")([0-9.abrc]+)(\")", Quoting.UNQUOTED),
]

GOLANG_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(GOLANG_MAJOR_MINOR_VERSION=)(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (
        re.compile(r"(\| *`GOLANG_MAJOR_MINOR_VERSION` *\| *)(`[0-9.abrc]+`)( *\|)"),
        Quoting.REVERSE_SINGLE_QUOTED,
    ),
]

AIRFLOW_IMAGE_PYTHON_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_PYTHON_VERSION=)(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (
        re.compile(r"(\| ``AIRFLOW_PYTHON_VERSION`` *\| )(``[0-9.abrc]+``)( *\|)"),
        Quoting.REVERSE_DOUBLE_QUOTED,
    ),
]

UV_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_UV_VERSION=)([0-9.abrc]+)"), Quoting.UNQUOTED),
    (re.compile(r"(uv>=)([0-9.abrc]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_UV_VERSION = )(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"^(\s*UV_VERSION = )(\"[0-9.abrc]+\")", re.MULTILINE), Quoting.DOUBLE_QUOTED),
    (re.compile(r"^(\s*UV_VERSION=)(\"[0-9.abrc]+\")", re.MULTILINE), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(\| *`AIRFLOW_UV_VERSION` *\| *)(`[0-9.abrd]+`)( *\|)"), Quoting.REVERSE_SINGLE_QUOTED),
    (
        re.compile(
            r"(\")([0-9.abrc]+)(\"  # Keep this comment to "
            r"allow automatic replacement of uv version)"
        ),
        Quoting.UNQUOTED,
    ),
]

PREK_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(AIRFLOW_PREK_VERSION=)([0-9.abrc]+)"), Quoting.UNQUOTED),
    (re.compile(r"(AIRFLOW_PREK_VERSION = )(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(prek>=)([0-9.abrc]+)"), Quoting.UNQUOTED),
    (re.compile(r"(PREK_VERSION = )(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (re.compile(r"(PREK_VERSION=)(\"[0-9.abrc]+\")"), Quoting.DOUBLE_QUOTED),
    (
        re.compile(r"(\| *`AIRFLOW_PREK_VERSION` *\| *)(`[0-9.abrc]+`)( *\|)"),
        Quoting.REVERSE_SINGLE_QUOTED,
    ),
    (
        re.compile(
            r"(\")([0-9.abrc]+)(\"  # Keep this comment to allow automatic "
            r"replacement of prek version)"
        ),
        Quoting.UNQUOTED,
    ),
]

NODE_LTS_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(^  node: )([0-9.abrc]+)^"), Quoting.UNQUOTED),
]


def get_replacement(value: str, quoting: Quoting) -> str:
    if quoting == Quoting.DOUBLE_QUOTED:
        return f'"{value}"'
    if quoting == Quoting.SINGLE_QUOTED:
        return f"'{value}'"
    if quoting == Quoting.REVERSE_SINGLE_QUOTED:
        return f"`{value}`"
    if quoting == Quoting.REVERSE_DOUBLE_QUOTED:
        return f"``{value}``"
    return value


VERBOSE: bool = os.environ.get("VERBOSE", "false") == "true"
UPGRADE_ALL_BY_DEFAULT: bool = os.environ.get("UPGRADE_ALL_BY_DEFAULT", "true") == "true"
UPGRADE_ALL_BY_DEFAULT_STR: str = str(UPGRADE_ALL_BY_DEFAULT).lower()
if UPGRADE_ALL_BY_DEFAULT:
    if VERBOSE == "true":
        console.print("[bright_blue]Upgrading all important versions")

UPGRADE_GITPYTHON: bool = os.environ.get("UPGRADE_GITPYTHON", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_GOLANG: bool = os.environ.get("UPGRADE_GOLANG", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_HATCH: bool = os.environ.get("UPGRADE_HATCH", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_NODE_LTS: bool = os.environ.get("UPGRADE_NODE_LTS", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_PIP: bool = os.environ.get("UPGRADE_PIP", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_PREK: bool = os.environ.get("UPGRADE_PREK", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_PYTHON: bool = os.environ.get("UPGRADE_PYTHON", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_PYYAML: bool = os.environ.get("UPGRADE_PYYAML", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_RICH: bool = os.environ.get("UPGRADE_RICH", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_RUFF: bool = os.environ.get("UPGRADE_RUFF", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_UV: bool = os.environ.get("UPGRADE_UV", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"
UPGRADE_MYPY: bool = os.environ.get("UPGRADE_MYPY", UPGRADE_ALL_BY_DEFAULT_STR).lower() == "true"

ALL_PYTHON_MAJOR_MINOR_VERSIONS = ["3.10", "3.11", "3.12", "3.13"]
DEFAULT_PROD_IMAGE_PYTHON_VERSION = "3.12"


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
    gh_token = retrieve_gh_token(description="airflow-upgrade-important-versions", scopes="public_repo")
    changed = False
    golang_version = get_latest_golang_version()
    pip_version = get_latest_pypi_version("pip", UPGRADE_PIP)
    uv_version = get_latest_pypi_version("uv", UPGRADE_UV)
    prek_version = get_latest_pypi_version("prek", UPGRADE_PREK)
    hatch_version = get_latest_pypi_version("hatch", UPGRADE_HATCH)
    pyyaml_version = get_latest_pypi_version("PyYAML", UPGRADE_PYYAML)
    gitpython_version = get_latest_pypi_version("GitPython", UPGRADE_GITPYTHON)
    ruff_version = get_latest_pypi_version("ruff", UPGRADE_RUFF)
    rich_version = get_latest_pypi_version("rich", UPGRADE_RICH)
    mypy_version = get_latest_pypi_version("mypy", UPGRADE_MYPY)
    node_lts_version = get_latest_lts_node_version()
    latest_python_versions: dict[str, str] = {}
    latest_image_python_version = ""
    if UPGRADE_PYTHON:
        all_python_versions = get_all_python_versions() if UPGRADE_PYTHON else None
        for python_major_minor_version in ALL_PYTHON_MAJOR_MINOR_VERSIONS:
            latest_python_versions[python_major_minor_version] = get_latest_python_version(
                python_major_minor_version, all_python_versions
            )
            if python_major_minor_version == DEFAULT_PROD_IMAGE_PYTHON_VERSION:
                latest_image_python_version = latest_python_versions[python_major_minor_version]
                console.print(
                    f"[bright_blue]Latest image python {python_major_minor_version} version: {latest_image_python_version}"
                )
    for file, keep_length in FILES_TO_UPDATE:
        console.print(f"[bright_blue]Updating {file}")
        file_content = file.read_text()
        new_content = file_content
        if UPGRADE_PIP:
            for line_pattern, quoting in PIP_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(pip_version, quoting), new_content, keep_length
                )
        if UPGRADE_PYTHON:
            for python_major_minor_version in ALL_PYTHON_MAJOR_MINOR_VERSIONS:
                latest_python_version = latest_python_versions[python_major_minor_version]
                for line_format, quoting in PYTHON_PATTERNS:
                    line_pattern = re.compile(
                        line_format.format(python_major_minor=python_major_minor_version)
                    )
                    new_content = replace_version(
                        line_pattern,
                        get_replacement(latest_python_version, quoting),
                        new_content,
                        keep_length,
                    )
                if python_major_minor_version == DEFAULT_PROD_IMAGE_PYTHON_VERSION:
                    for line_pattern, quoting in AIRFLOW_IMAGE_PYTHON_PATTERNS:
                        new_content = replace_version(
                            line_pattern,
                            get_replacement(latest_python_version, quoting),
                            new_content,
                            keep_length,
                        )
        if UPGRADE_GOLANG:
            for line_pattern, quoting in GOLANG_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(golang_version, quoting), new_content, keep_length
                )
        if UPGRADE_UV:
            for line_pattern, quoting in UV_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(uv_version, quoting), new_content, keep_length
                )
        if UPGRADE_PREK:
            for line_pattern, quoting in PREK_PATTERNS:
                new_content = replace_version(
                    line_pattern, get_replacement(prek_version, quoting), new_content, keep_length
                )
        if UPGRADE_NODE_LTS:
            for line_pattern, quoting in NODE_LTS_PATTERNS:
                new_content = replace_version(
                    line_pattern,
                    get_replacement(node_lts_version, quoting),
                    new_content,
                    keep_length,
                )
        if UPGRADE_HATCH:
            new_content = re.sub(
                r"(HATCH_VERSION = )(\"[0-9.abrc]+\")",
                f'HATCH_VERSION = "{hatch_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(HATCH_VERSION=)(\"[0-9.abrc]+\")",
                f'HATCH_VERSION="{hatch_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(hatch==)([0-9.abrc]+)",
                f"hatch=={hatch_version}",
                new_content,
            )
            new_content = re.sub(
                r"(hatch>=)([0-9.abrc]+)",
                f"hatch>={hatch_version}",
                new_content,
            )
        if UPGRADE_PYYAML:
            new_content = re.sub(
                r"(PYYAML_VERSION = )(\"[0-9.abrc]+\")",
                f'PYYAML_VERSION = "{pyyaml_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(PYYAML_VERSION=)(\"[0-9.abrc]+\")",
                f'PYYAML_VERSION="{pyyaml_version}"',
                new_content,
            )
        if UPGRADE_GITPYTHON:
            new_content = re.sub(
                r"(GITPYTHON_VERSION = )(\"[0-9.abrc]+\")",
                f'GITPYTHON_VERSION = "{gitpython_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(GITPYTHON_VERSION=)(\"[0-9.abrc]+\")",
                f'GITPYTHON_VERSION="{gitpython_version}"',
                new_content,
            )
        if UPGRADE_RICH:
            new_content = re.sub(
                r"(RICH_VERSION = )(\"[0-9.abrc]+\")",
                f'RICH_VERSION = "{rich_version}"',
                new_content,
            )
            new_content = re.sub(
                r"(RICH_VERSION=)(\"[0-9.abrc]+\")",
                f'RICH_VERSION="{rich_version}"',
                new_content,
            )
        if UPGRADE_RUFF:
            new_content = re.sub(
                r"(ruff==)([0-9.abrc]+)",
                f"ruff=={ruff_version}",
                new_content,
            )
            new_content = re.sub(
                r"(ruff>=)([0-9.abrc]+)",
                f"ruff>={ruff_version}",
                new_content,
            )
        if UPGRADE_MYPY:
            console.print(f"[bright_blue]Latest mypy version: {mypy_version}")

            new_content = re.sub(
                r"(mypy==)([0-9.]+)",
                f"mypy=={mypy_version}",
                new_content,
            )

        if new_content != file_content:
            file.write_text(new_content)
            console.print(f"[bright_blue]Updated {file}")
            changed = True
    if changed:
        console.print("[bright_blue]Running breeze's uv sync to update the lock file")
        copy_env = os.environ.copy()
        del copy_env["VIRTUAL_ENV"]
        subprocess.run(
            ["uv", "sync", "--resolution", "highest", "--upgrade"],
            check=True,
            cwd=AIRFLOW_ROOT_PATH / "dev" / "breeze",
            env=copy_env,
        )
        if not os.environ.get("CI"):
            console.print("[bright_blue]Please commit the changes")
        sys.exit(1)
