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
"""
Test for an order of dependencies in setup.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import get_all_provider_ids, insert_documentation

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
AIRFLOW_PYPROJECT_TOML_FILE = AIRFLOW_ROOT_PATH / "pyproject.toml"
AIRFLOW_CORE_ROOT_PATH = AIRFLOW_ROOT_PATH / "airflow-core"
AIRFLOW_CORE_PYPROJECT_TOML_FILE = AIRFLOW_CORE_ROOT_PATH / "pyproject.toml"

PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "providers"

START_OPTIONAL_DEPENDENCIES = "# Automatically generated airflow optional dependencies"
END_OPTIONAL_DEPENDENCIES = "# End of automatically generated airflow optional dependencies"

START_MYPY_PATHS = "    # Automatically generated mypy paths"
END_MYPY_PATHS = "    # End of automatically generated mypy paths"

START_WORKSPACE_ITEMS = "# Automatically generated provider workspace items"
END_WORKSPACE_ITEMS = "# End of automatically generated provider workspace items"

START_PROVIDER_WORKSPACE_MEMBERS = "    # Automatically generated provider workspace members"
END_PROVIDER_WORKSPACE_MEMBERS = "    # End of automatically generated provider workspace members"


def get_optional_dependencies_from_airflow_core() -> list[str]:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    airflow_core_toml_dict = tomllib.loads(AIRFLOW_CORE_PYPROJECT_TOML_FILE.read_text())
    return airflow_core_toml_dict["project"]["optional-dependencies"].keys()


def provider_distribution_name(provider_id: str) -> str:
    return f"apache-airflow-providers-{provider_id.replace('.', '-')}"


def provider_path(provider_id: str) -> str:
    return f"{provider_id.replace('.', '/')}"


if __name__ == "__main__":
    all_optional_dependencies = []
    optional_airflow_core_dependencies = get_optional_dependencies_from_airflow_core()
    for optional in sorted(optional_airflow_core_dependencies):
        if optional == "all":
            all_optional_dependencies.append('"all-core" = [\n    "apache-airflow-core[all]"\n]\n')
        else:
            all_optional_dependencies.append(f'"{optional}" = [\n    "apache-airflow-core[{optional}]"\n]\n')
    all_providers = sorted(get_all_provider_ids())
    all_provider_lines = []
    for provider_id in all_providers:
        distribution_name = provider_distribution_name(provider_id)
        all_optional_dependencies.append(f'"{provider_id}" = [\n    "{distribution_name}"\n]\n')
        all_provider_lines.append(f'    "{distribution_name}",\n')
    all_optional_dependencies.append('"all" = [\n    "apache-airflow-core[all]",\n')
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
