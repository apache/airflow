#!/usr/bin/env python
#
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
#   "rich>=13.6.0",
#   "tomli>=2.0.1",
# ]
# ///
"""
Test for an order of dependencies in setup.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from packaging.version import Version, parse as parse_version

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_ROOT_PATH, console, get_all_provider_ids, insert_documentation

AIRFLOW_PYPROJECT_TOML_FILE = AIRFLOW_ROOT_PATH / "pyproject.toml"
AIRFLOW_CORE_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-core"
AIRFLOW_CORE_PYPROJECT_TOML_FILE = AIRFLOW_CORE_ROOT_PATH / "pyproject.toml"

AIRFLOW_TASK_SDK_ROOT_PATH = AIRFLOW_ROOT_PATH / "task-sdk"
AIRFLOW_TASK_SDK_PYPROJECT_TOML_FILE = AIRFLOW_TASK_SDK_ROOT_PATH / "pyproject.toml"

PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "providers"

START_OPTIONAL_DEPENDENCIES = (
    "# Automatically generated airflow optional dependencies (update_airflow_pyproject_toml.py)"
)
END_OPTIONAL_DEPENDENCIES = "# End of automatically generated airflow optional dependencies"

START_MYPY_PATHS = "    # Automatically generated mypy paths (update_airflow_pyproject_toml.py)"
END_MYPY_PATHS = "    # End of automatically generated mypy paths"

START_WORKSPACE_ITEMS = (
    "# Automatically generated provider workspace items (update_airflow_pyproject_toml.py)"
)
END_WORKSPACE_ITEMS = "# End of automatically generated provider workspace items"

START_PROVIDER_WORKSPACE_MEMBERS = (
    "    # Automatically generated provider workspace members (update_airflow_pyproject_toml.py)"
)
END_PROVIDER_WORKSPACE_MEMBERS = "    # End of automatically generated provider workspace members"

CUT_OFF_TIMEDELTA = timedelta(days=6 * 30)

# Temporary override for providers that are not yet included in constraints or when they need
# minimum versions for compatibility with Airflow 3
MIN_VERSION_OVERRIDE: dict[str, Version] = {
    "amazon": parse_version("2.1.3"),
    "fab": parse_version("2.2.0"),
    "openlineage": parse_version("2.3.0"),
    "git": parse_version("0.0.2"),
    "common.messaging": parse_version("2.0.0"),
}


def get_optional_dependencies(pyproject_toml_path: Path) -> list[str]:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    airflow_core_toml_dict = tomllib.loads(pyproject_toml_path.read_text())
    return airflow_core_toml_dict["project"]["optional-dependencies"].keys()


def provider_distribution_name(provider_id: str) -> str:
    return f"apache-airflow-providers-{provider_id.replace('.', '-')}"


def provider_path(provider_id: str) -> str:
    return f"{provider_id.replace('.', '/')}"


PROVIDER_METADATA_FILE_PATH = AIRFLOW_ROOT_PATH / "generated" / "provider_metadata.json"
PROVIDER_DEPENDENCIES_FILE_PATH = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"

file_list = sys.argv[1:]
console.print("[bright_blue]Updating min-provider versions in apache-airflow\n")

all_providers_metadata = json.loads(PROVIDER_METADATA_FILE_PATH.read_text())
all_providers_dependencies = json.loads(PROVIDER_DEPENDENCIES_FILE_PATH.read_text())


def find_min_provider_version(provider_id: str) -> tuple[Version | None, str]:
    metadata = all_providers_metadata.get(provider_id)
    # We should periodically update the starting date to avoid pip install resolution issues
    # TODO: when min Python version is 3.11 change back the code to fromisoformat
    # https://github.com/apache/airflow/pull/49155/files
    cut_off_date = datetime.strptime("2024-10-12T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    last_version_newer_than_cutoff: Version | None = None
    date_released: datetime | None = None
    min_version_override = MIN_VERSION_OVERRIDE.get(provider_id)
    if not metadata:
        if not min_version_override:
            return None, ""
        last_version_newer_than_cutoff = min_version_override
    else:
        versions: list[Version] = sorted([parse_version(version) for version in metadata], reverse=True)
        for version in versions:
            provider_info = metadata[str(version)]
            date_released = datetime.strptime(provider_info["date_released"], "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            if date_released < cut_off_date:
                break
            last_version_newer_than_cutoff = version
    console.print(
        f"[bright_blue]Provider id {provider_id} min version found:[/] "
        f"{last_version_newer_than_cutoff} (date {date_released}"
    )
    override_comment = ""
    if last_version_newer_than_cutoff:
        if min_version_override and min_version_override > last_version_newer_than_cutoff:
            console.print(
                f"[yellow]Overriding provider id {provider_id} min version:[/] {min_version_override} "
                f"set from hard-coded versions.\n\n"
                f"[yellow]Modify MIN_VERSION_OVERRIDE in {__file__} to set different version![/]\n"
            )
            last_version_newer_than_cutoff = min_version_override
            override_comment = f" # Set from MIN_VERSION_OVERRIDE in {Path(__file__).name}"
    return last_version_newer_than_cutoff, override_comment


PROVIDER_MIN_VERSIONS: dict[str, str | None] = {}


def get_python_exclusion(provider_dependencies: dict[str, Any]) -> str:
    """
    Return a Python version exclusion marker string based on provider metadata.

    If there are excluded Python versions in the metadata, this function returns a
    marker string like: '; python_version != "3.8" and python_version != "3.11"'

    If none are found, it returns an empty str.
    """
    if not provider_dependencies:
        return ""
    python_exclusions = provider_dependencies.get("excluded-python-versions", [])
    if python_exclusions:
        python_exclusions_str = "and ".join(
            f'python_version !=\\"{version}\\"' for version in python_exclusions
        )
        return f"; {python_exclusions_str}"
    return ""


if __name__ == "__main__":
    all_optional_dependencies = []
    optional_airflow_core_dependencies = get_optional_dependencies(AIRFLOW_CORE_PYPROJECT_TOML_FILE)
    for optional in sorted(optional_airflow_core_dependencies):
        if optional == "all":
            all_optional_dependencies.append('"all-core" = [\n    "apache-airflow-core[all]"\n]\n')
        else:
            all_optional_dependencies.append(f'"{optional}" = [\n    "apache-airflow-core[{optional}]"\n]\n')
    optional_airflow_task_sdk_dependencies = get_optional_dependencies(AIRFLOW_TASK_SDK_PYPROJECT_TOML_FILE)
    for optional in sorted(optional_airflow_task_sdk_dependencies):
        if optional == "all":
            all_optional_dependencies.append('"all-task-sdk" = [\n    "apache-airflow-task-sdk[all]"\n]\n')
        else:
            all_optional_dependencies.append(
                f'"{optional}" = [\n    "apache-airflow-task-sdk[{optional}]"\n]\n'
            )
    all_providers = sorted(get_all_provider_ids())
    all_provider_lines = []
    for provider_id in all_providers:
        distribution_name = provider_distribution_name(provider_id)
        min_provider_version, comment = find_min_provider_version(provider_id)
        python_exclusion = get_python_exclusion(all_providers_dependencies.get(provider_id, {}))

        if min_provider_version:
            all_provider_lines.append(
                f'    "{distribution_name}>={min_provider_version}{python_exclusion}",{comment}\n'
            )
            all_optional_dependencies.append(
                f'"{provider_id}" = [\n    "{distribution_name}>={min_provider_version}{python_exclusion}"{comment}\n]\n'
            )
        else:
            all_optional_dependencies.append(f'"{provider_id}" = [\n    "{distribution_name}"\n]\n')
            all_provider_lines.append(f'    "{distribution_name}",\n')
    all_optional_dependencies.append('"all" = [\n')
    optional_apache_airflow_dependencies = get_optional_dependencies(AIRFLOW_PYPROJECT_TOML_FILE)
    all_local_extras = [
        extra
        for extra in sorted(optional_apache_airflow_dependencies)
        if extra not in all_providers and not extra.startswith("all")
    ]
    all_optional_dependencies.append(f'    "apache-airflow[{",".join(all_local_extras)}]",\n')
    all_optional_dependencies.append('    "apache-airflow-core[all]",\n')
    all_optional_dependencies.extend(all_provider_lines)
    all_optional_dependencies.append("]\n")
    insert_documentation(
        AIRFLOW_PYPROJECT_TOML_FILE,
        all_optional_dependencies,
        START_OPTIONAL_DEPENDENCIES,
        END_OPTIONAL_DEPENDENCIES,
    )
    all_mypy_paths = []
    for provider_id in all_providers:
        provider_mypy_path = f"$MYPY_CONFIG_FILE_DIR/providers/{provider_path(provider_id)}"
        all_mypy_paths.append(f'    "{provider_mypy_path}/src",\n')
        all_mypy_paths.append(f'    "{provider_mypy_path}/tests",\n')
    insert_documentation(AIRFLOW_PYPROJECT_TOML_FILE, all_mypy_paths, START_MYPY_PATHS, END_MYPY_PATHS)
    all_workspace_items = []
    for provider_id in all_providers:
        all_workspace_items.append(f"{provider_distribution_name(provider_id)} = {{ workspace = true }}\n")
    insert_documentation(
        AIRFLOW_PYPROJECT_TOML_FILE,
        all_workspace_items,
        START_WORKSPACE_ITEMS,
        END_WORKSPACE_ITEMS,
    )
    all_workspace_members = []
    for provider_id in all_providers:
        all_workspace_members.append(f'    "providers/{provider_path(provider_id)}",\n')
    insert_documentation(
        AIRFLOW_PYPROJECT_TOML_FILE,
        all_workspace_members,
        START_PROVIDER_WORKSPACE_MEMBERS,
        END_PROVIDER_WORKSPACE_MEMBERS,
    )
