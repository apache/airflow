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

from __future__ import annotations

import json
from pathlib import Path

from in_container_utils import click, run_command

AIRFLOW_SOURCES_DIR = Path(__file__).resolve().parents[2]


def get_devel_test_deps() -> list[str]:
    # Pre-install the tests_common pytest plugin/utils, in case sources aren't mounted
    devel_deps: list[str] = ["./tests_common"]
    hatch_build_content = (
        (AIRFLOW_SOURCES_DIR / "hatch_build.py").read_text().splitlines()
    )
    store = False
    for line in hatch_build_content:
        if line.strip().startswith('"devel-tests": ['):
            store = True
            continue
        if line.strip().startswith("],"):
            store = False
        if store and not line.strip().startswith("#"):
            devel_deps.append(line.strip().replace('"', "").split(">=")[0])
    return devel_deps


def get_devel_deps_from_providers():
    devel_deps_from_providers = []
    deps = json.loads(
        (AIRFLOW_SOURCES_DIR / "generated" / "provider_dependencies.json").read_text()
    )
    for dep in deps:
        devel_deps = [short_dep.split(">=")[0] for short_dep in deps[dep]["devel-deps"]]
        if devel_deps:
            devel_deps_from_providers.extend(devel_deps)
    return devel_deps_from_providers


@click.command()
@click.option(
    "--github-actions",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="GITHUB_ACTIONS",
    help="Running in GitHub Actions",
)
@click.option(
    "--constraint",
    required=True,
    help="Constraint used to install the dependencies",
)
def install_devel_deps(github_actions: bool, constraint: str):
    deps = get_devel_test_deps() + get_devel_deps_from_providers()
    run_command(
        [
            "uv",
            "pip",
            "install",
            "--python",
            "/usr/local/bin/python",
            *deps,
            "--constraint",
            constraint,
        ],
        github_actions=github_actions,
    )


if __name__ == "__main__":
    install_devel_deps()
