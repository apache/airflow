#!/usr/bin/env python3

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
# requires-python = ">=3.9"
# dependencies = [
#   "packaging>=23.2",
#   "click>=8.1.8",
#   "rich-click>=1.7.1",
#   "rich>=13.6.0",
# ]
# ///

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import AIRFLOW_ROOT_PATH, click, console, run_command
from packaging.requirements import Requirement


@click.command()
@click.option(
    "--constraint",
    required=True,
    envvar="CONSTRAINT",
    help="Constraints file or url to use for installation",
)
@click.option(
    "--github-actions",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="GITHUB_ACTIONS",
    help="Running in GitHub Actions",
)
def install_development_dependencies(constraint: str, github_actions: bool):
    pyproject_toml_of_devel_commons = (AIRFLOW_ROOT_PATH / "devel-common" / "pyproject.toml").read_text()
    development_dependencies: list[str] = []
    in_devel_common_dependencies = False
    for line in pyproject_toml_of_devel_commons.splitlines():
        stripped_line = line.strip()
        if stripped_line.startswith("dependencies = ["):
            in_devel_common_dependencies = True
            continue
        if in_devel_common_dependencies and stripped_line.startswith("]"):
            break
        if in_devel_common_dependencies:
            if not stripped_line.startswith("#"):
                dependency = stripped_line.strip('",')
                requirement = Requirement(dependency)
                marker = requirement.marker
                if marker and not marker.evaluate():
                    continue
                dep = dependency.split(";")[0]
                if dep.startswith("apache-airflow-devel-common"):
                    local_dep = dep[len("apache-airflow-") :]
                    dep = f"{AIRFLOW_ROOT_PATH}/{local_dep}"
                development_dependencies.append(dep)
    providers_dependencies = json.loads(
        (AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json").read_text()
    )
    for provider_id in providers_dependencies:
        development_dependencies.extend(providers_dependencies[provider_id]["devel-deps"])
    command = ["uv", "pip", "install", *development_dependencies, "--constraints", constraint]
    result = run_command(command, check=False, github_actions=github_actions)
    if result.returncode != 0:
        console.print("[yellow]Failed to install development dependencies with constraints[/]\n")
        console.print("Trying without constraints\n")
        command = ["uv", "pip", "install", *development_dependencies]
        result = run_command(command, check=False, github_actions=github_actions)
        if result.returncode != 0:
            console.print("[red]Failed to install development dependencies even without constraints[/]")
            sys.exit(1)


if __name__ == "__main__":
    install_development_dependencies()
