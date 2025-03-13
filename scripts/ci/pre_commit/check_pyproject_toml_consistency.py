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
from common_precommit_utils import console, get_all_provider_ids

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
PYPROJECT_TOML_FILE = AIRFLOW_ROOT_PATH / "pyproject.toml"
PROVIDERS_DIR = AIRFLOW_ROOT_PATH / "providers"

if __name__ == "__main__":
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib

    error = False
    toml_dict = tomllib.loads(PYPROJECT_TOML_FILE.read_text())
    all_providers = get_all_provider_ids()
    for provider_id in all_providers:
        expected_provider_package = f"apache-airflow-providers-{provider_id.replace('.', '-')}"
        expected_member = "providers/" + provider_id.replace(".", "/")
        dev_dependency_group = toml_dict["dependency-groups"]["dev"]
        if expected_provider_package not in dev_dependency_group:
            console.print(
                f"[red]ERROR: {expected_provider_package} is not found in airflow's pyproject.toml "
                f"in dev dependency-group: {dev_dependency_group}[/red]"
            )
            error = True
        tool_uv_sources = toml_dict["tool"]["uv"]["sources"]
        if expected_provider_package not in tool_uv_sources:
            console.print(
                f"[red]ERROR: {expected_provider_package} is not found in airflow's pyproject.toml "
                f"in tool.uv.sources: {tool_uv_sources}[/red]"
            )
            error = True
        tool_uv_workspace_members = toml_dict["tool"]["uv"]["workspace"]["members"]
        if expected_member not in tool_uv_workspace_members:
            console.print(
                f"[red]ERROR: {expected_member} is not found in airflow's pyproject.toml "
                f"in tool.uv.workspace members: {tool_uv_workspace_members}[/red]"
            )
            error = True
    if error:
        sys.exit(1)
