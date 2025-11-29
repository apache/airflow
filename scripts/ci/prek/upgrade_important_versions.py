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
#   are set by UPGRADE_NNNNNNN (NNNNNNN > thing to upgrade version)
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
    (AIRFLOW_ROOT_PATH / "dev" / "provider_db_inventory.py", False),
    (AIRFLOW_ROOT_PATH / "dev" / "pyproject.toml", False),
    (AIRFLOW_ROOT_PATH / "go-sdk" / ".pre-commit-config.yaml", False),
]
for file in DOCKER_IMAGES_EXAMPLE_DIR_PATH.rglob("*.sh"):
    FILES_TO_UPDATE.append((file, False))

PREK_DIR_PATH = AIRFLOW_ROOT_PATH / "scripts" / "ci" / "prek"
for file in PREK_DIR_PATH.rglob("*"):
    if file.is_file() and file.name != "upgrade_important_versions.py" and not file.suffix == ".pyc":
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


def get_latest_image_version(image: str) -> str:
    """
    Fetch the latest tag released for a DockerHub image.

    Args:
        image: DockerHub image name in the format "namespace/repository" or just "repository" for official images

    Returns:
        The latest tag version as a string
    """
    if VERBOSE:
        console.print(f"[bright_blue]Fetching latest tag for DockerHub image: {image}")

    # Split image into namespace and repository
    if "/" in image:
        namespace, repository = image.split("/", 1)
    else:
        # Official images use 'library' as namespace
        namespace = "library"
        repository = image

    # DockerHub API endpoint for tags
    url = f"https://registry.hub.docker.com/v2/repositories/{namespace}/{repository}/tags"
    params = {"page_size": 100, "ordering": "last_updated"}

    headers = {"User-Agent": "Python requests"}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    tags = data.get("results", [])

    if not tags:
        console.print(f"[bright_red]No tags found for image {image}")
        return ""

    # Filter out non-version tags and sort by version
    version_tags = []
    for tag in tags:
        tag_name = tag["name"]
        # Skip tags like 'latest', 'stable', etc.
        if tag_name in ["latest", "stable", "main", "master"]:
            continue
        try:
            # Try to parse as version to filter out non-version tags
            # Remove leading 'v' if present
            version_str = tag_name.lstrip("v")
            version_obj = Version(version_str)
            version_tags.append((version_obj, tag_name))
        except Exception:
            # Skip tags that don't parse as versions
            continue

    if not version_tags:
        # If no version tags found, return the first tag
        latest_tag = tags[0]["name"]
        if VERBOSE:
            console.print(f"[bright_blue]Latest tag for {image}: {latest_tag} (no version tags found)")
        return latest_tag

    # Sort by version and get the latest
    version_tags.sort(key=lambda x: x[0], reverse=True)
    latest_tag = version_tags[0][1]

    if VERBOSE:
        console.print(f"[bright_blue]Latest tag for {image}: {latest_tag}")

    return latest_tag


def get_latest_github_release_version(repo: str) -> str:
    """
    Fetch the latest release version from a GitHub repository.

    Args:
        repo: GitHub repository in the format "owner/repo"

    Returns:
        The latest release version as a string (without leading 'v')
    """
    if VERBOSE:
        console.print(f"[bright_blue]Fetching latest release for GitHub repo: {repo}")

    url = f"https://api.github.com/repos/{repo}/releases/latest"
    headers = {"User-Agent": "Python requests"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    data = response.json()
    tag_name = data.get("tag_name", "")

    if not tag_name:
        console.print(f"[bright_red]No release tag found for {repo}")
        return ""

    # Remove leading 'v' if present
    version = tag_name.lstrip("v")

    if VERBOSE:
        console.print(f"[bright_blue]Latest version for {repo}: {version}")

    return version


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
            r"(\")([0-9.abrc]+)(\" {2}# Keep this comment to "
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
            r"(\")([0-9.abrc]+)(\" {2}# Keep this comment to allow automatic "
            r"replacement of prek version)"
        ),
        Quoting.UNQUOTED,
    ),
]

NODE_LTS_PATTERNS: list[tuple[re.Pattern, Quoting]] = [
    (re.compile(r"(^ {2}node: )([0-9.abrc]+)^"), Quoting.UNQUOTED),
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


def get_env_bool(name: str, default: bool = True) -> bool:
    """Get boolean value from environment variable."""
    default_str = str(default).lower()
    upgrade_all_str = str(UPGRADE_ALL_BY_DEFAULT).lower()
    fallback = upgrade_all_str if name.startswith("UPGRADE_") else default_str
    return os.environ.get(name, fallback).lower() == "true"


VERBOSE: bool = os.environ.get("VERBOSE", "false") == "true"
UPGRADE_ALL_BY_DEFAULT: bool = os.environ.get("UPGRADE_ALL_BY_DEFAULT", "true") == "true"

if UPGRADE_ALL_BY_DEFAULT and VERBOSE:
    console.print("[bright_blue]Upgrading all important versions")

# Package upgrade flags
UPGRADE_GITPYTHON: bool = get_env_bool("UPGRADE_GITPYTHON")
UPGRADE_GOLANG: bool = get_env_bool("UPGRADE_GOLANG")
UPGRADE_HATCH: bool = get_env_bool("UPGRADE_HATCH")
UPGRADE_MPROCS: bool = get_env_bool("UPGRADE_MPROCS")
UPGRADE_NODE_LTS: bool = get_env_bool("UPGRADE_NODE_LTS")
UPGRADE_PIP: bool = get_env_bool("UPGRADE_PIP")
UPGRADE_PREK: bool = get_env_bool("UPGRADE_PREK")
UPGRADE_PYTHON: bool = get_env_bool("UPGRADE_PYTHON")
UPGRADE_PYYAML: bool = get_env_bool("UPGRADE_PYYAML")
UPGRADE_RICH: bool = get_env_bool("UPGRADE_RICH")
UPGRADE_RUFF: bool = get_env_bool("UPGRADE_RUFF")
UPGRADE_UV: bool = get_env_bool("UPGRADE_UV")
UPGRADE_MYPY: bool = get_env_bool("UPGRADE_MYPY")
UPGRADE_PROTOC: bool = get_env_bool("UPGRADE_PROTOC")

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


def apply_simple_regex_replacements(
    text: str,
    version: str,
    patterns: list[tuple[str, str]],
) -> str:
    """Apply a list of simple regex replacements where the version is substituted."""
    result = text
    for pattern, replacement_template in patterns:
        result = re.sub(pattern, replacement_template.format(version=version), result)
    return result


def apply_pattern_replacements(
    text: str,
    version: str,
    patterns: list[tuple[re.Pattern, Quoting]],
    keep_length: bool,
) -> str:
    """Apply pattern-based replacements with quoting."""
    result = text
    for line_pattern, quoting in patterns:
        result = replace_version(line_pattern, get_replacement(version, quoting), result, keep_length)
    return result


# Configuration for packages that follow simple version constant patterns
SIMPLE_VERSION_PATTERNS = {
    "hatch": [
        (r"(HATCH_VERSION = )(\"[0-9.abrc]+\")", 'HATCH_VERSION = "{version}"'),
        (r"(HATCH_VERSION=)(\"[0-9.abrc]+\")", 'HATCH_VERSION="{version}"'),
        (r"(hatch==)([0-9.abrc]+)", "hatch=={version}"),
        (r"(hatch>=)([0-9.abrc]+)", "hatch>={version}"),
    ],
    "pyyaml": [
        (r"(PYYAML_VERSION = )(\"[0-9.abrc]+\")", 'PYYAML_VERSION = "{version}"'),
        (r"(PYYAML_VERSION=)(\"[0-9.abrc]+\")", 'PYYAML_VERSION="{version}"'),
        (r"(pyyaml>=)(\"[0-9.abrc]+\")", 'pyyaml>="{version}"'),
        (r"(pyyaml>=)([0-9.abrc]+)", "pyyaml>={version}"),
    ],
    "gitpython": [
        (r"(GITPYTHON_VERSION = )(\"[0-9.abrc]+\")", 'GITPYTHON_VERSION = "{version}"'),
        (r"(GITPYTHON_VERSION=)(\"[0-9.abrc]+\")", 'GITPYTHON_VERSION="{version}"'),
    ],
    "rich": [
        (r"(RICH_VERSION = )(\"[0-9.abrc]+\")", 'RICH_VERSION = "{version}"'),
        (r"(RICH_VERSION=)(\"[0-9.abrc]+\")", 'RICH_VERSION="{version}"'),
    ],
    "ruff": [
        (r"(ruff==)([0-9.abrc]+)", "ruff=={version}"),
        (r"(ruff>=)([0-9.abrc]+)", "ruff>={version}"),
    ],
    "mypy": [
        (r"(mypy==)([0-9.]+)", "mypy=={version}"),
    ],
    "protoc": [
        (r"(rvolosatovs/protoc:)(v[0-9.]+)", "rvolosatovs/protoc:{version}"),
    ],
    "mprocs": [
        (r"(ARG MPROCS_VERSION=)(\"[0-9.]+\")", 'ARG MPROCS_VERSION="{version}"'),
    ],
}


# Configuration mapping pattern variables to their patterns and upgrade flags
PATTERN_REGISTRY = {
    "pip": (PIP_PATTERNS, UPGRADE_PIP),
    "golang": (GOLANG_PATTERNS, UPGRADE_GOLANG),
    "uv": (UV_PATTERNS, UPGRADE_UV),
    "prek": (PREK_PATTERNS, UPGRADE_PREK),
    "node_lts": (NODE_LTS_PATTERNS, UPGRADE_NODE_LTS),
}


def fetch_all_package_versions() -> dict[str, str]:
    """Fetch latest versions for all packages that need to be upgraded."""
    return {
        "golang": get_latest_golang_version() if UPGRADE_GOLANG else "",
        "pip": get_latest_pypi_version("pip", UPGRADE_PIP),
        "uv": get_latest_pypi_version("uv", UPGRADE_UV),
        "prek": get_latest_pypi_version("prek", UPGRADE_PREK),
        "hatch": get_latest_pypi_version("hatch", UPGRADE_HATCH),
        "pyyaml": get_latest_pypi_version("PyYAML", UPGRADE_PYYAML),
        "gitpython": get_latest_pypi_version("GitPython", UPGRADE_GITPYTHON),
        "ruff": get_latest_pypi_version("ruff", UPGRADE_RUFF),
        "rich": get_latest_pypi_version("rich", UPGRADE_RICH),
        "mypy": get_latest_pypi_version("mypy", UPGRADE_MYPY),
        "node_lts": get_latest_lts_node_version() if UPGRADE_NODE_LTS else "",
        "protoc": get_latest_image_version("rvolosatovs/protoc") if UPGRADE_PROTOC else "",
        "mprocs": get_latest_github_release_version("pvolok/mprocs") if UPGRADE_MPROCS else "",
    }


def log_special_versions(versions: dict[str, str]) -> None:
    """Log versions that need special attention."""
    if UPGRADE_MYPY and versions["mypy"]:
        console.print(f"[bright_blue]Latest mypy version: {versions['mypy']}")
    if UPGRADE_PROTOC and versions["protoc"]:
        console.print(f"[bright_blue]Latest protoc image version: {versions['protoc']}")


def fetch_python_versions() -> dict[str, str]:
    """Fetch latest Python versions for all supported major.minor versions."""
    latest_python_versions: dict[str, str] = {}
    if not UPGRADE_PYTHON:
        return latest_python_versions

    all_python_versions = get_all_python_versions()
    for python_major_minor_version in ALL_PYTHON_MAJOR_MINOR_VERSIONS:
        latest_python_versions[python_major_minor_version] = get_latest_python_version(
            python_major_minor_version, all_python_versions
        )
        if python_major_minor_version == DEFAULT_PROD_IMAGE_PYTHON_VERSION:
            console.print(
                f"[bright_blue]Latest image python {python_major_minor_version} "
                f"version: {latest_python_versions[python_major_minor_version]}"
            )
    return latest_python_versions


def update_file_with_versions(
    file_content: str,
    keep_length: bool,
    versions: dict[str, str],
    latest_python_versions: dict[str, str],
) -> str:
    """Update file content with all version replacements."""
    new_content = file_content

    # Apply pattern-based replacements using registry
    for package_name, (patterns, should_upgrade) in PATTERN_REGISTRY.items():
        version = versions.get(package_name, "")
        if should_upgrade and version:
            new_content = apply_pattern_replacements(new_content, version, patterns, keep_length)

    # Handle Python version updates (special case due to multiple versions)
    if UPGRADE_PYTHON:
        for python_major_minor_version in ALL_PYTHON_MAJOR_MINOR_VERSIONS:
            latest_python_version = latest_python_versions[python_major_minor_version]
            for line_format, quoting in PYTHON_PATTERNS:
                line_pattern = re.compile(line_format.format(python_major_minor=python_major_minor_version))
                new_content = replace_version(
                    line_pattern,
                    get_replacement(latest_python_version, quoting),
                    new_content,
                    keep_length,
                )
            if python_major_minor_version == DEFAULT_PROD_IMAGE_PYTHON_VERSION:
                new_content = apply_pattern_replacements(
                    new_content, latest_python_version, AIRFLOW_IMAGE_PYTHON_PATTERNS, keep_length
                )

    # Apply simple regex replacements
    for package_name, patterns in SIMPLE_VERSION_PATTERNS.items():
        should_upgrade = globals().get(f"UPGRADE_{package_name.upper()}", False)
        version = versions.get(package_name, "")
        if should_upgrade and version:
            new_content = apply_simple_regex_replacements(new_content, version, patterns)

    return new_content


def process_all_files(versions: dict[str, str], latest_python_versions: dict[str, str]) -> bool:
    """
    Process all files and apply version updates.

    Returns:
        True if any files were changed, False otherwise.
    """
    changed = False
    for file_to_update, keep_length in FILES_TO_UPDATE:
        console.print(f"[bright_blue]Updating {file_to_update}")
        file_content = file_to_update.read_text()
        new_content = update_file_with_versions(file_content, keep_length, versions, latest_python_versions)

        if new_content != file_content:
            file_to_update.write_text(new_content)
            console.print(f"[bright_blue]Updated {file_to_update}")
            changed = True
    return changed


def sync_breeze_lock_file() -> None:
    """Run uv sync to update breeze's lock file."""
    console.print("[bright_blue]Running breeze's uv sync to update the lock file")
    copy_env = os.environ.copy()
    del copy_env["VIRTUAL_ENV"]
    subprocess.run(
        ["uv", "sync", "--resolution", "highest", "--upgrade"],
        check=True,
        cwd=AIRFLOW_ROOT_PATH / "dev" / "breeze",
        env=copy_env,
    )


def main() -> None:
    """Main entry point for the version upgrade script."""
    retrieve_gh_token(description="airflow-upgrade-important-versions", scopes="public_repo")

    versions = fetch_all_package_versions()
    log_special_versions(versions)
    latest_python_versions = fetch_python_versions()

    changed = process_all_files(versions, latest_python_versions)

    if changed:
        sync_breeze_lock_file()
        if not os.environ.get("CI"):
            console.print("[bright_blue]Please commit the changes")
        sys.exit(1)


if __name__ == "__main__":
    main()
