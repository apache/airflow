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

import pytest

from airflow_breeze.commands.ci_commands import UPGRADE_COMMANDS, build_upgrade_pr_body, upgrade

STEP_NAMES = [name for name, _ in UPGRADE_COMMANDS]


def test_floor_step_runs_after_important_versions_and_before_lock_upgrade():
    assert (
        STEP_NAMES.index("upgrade-important-versions")
        < STEP_NAMES.index("upgrade-dependency-floors")
        < STEP_NAMES.index("update-uv-lock")
    )


def test_floor_step_runs_the_manual_hook():
    command = dict(UPGRADE_COMMANDS)["upgrade-dependency-floors"]
    assert command.endswith("--stage manual upgrade-dependency-floors")


@pytest.mark.parametrize(
    ("report", "expected_suffix"),
    [
        pytest.param(None, "environment.", id="no-report"),
        pytest.param("", "environment.", id="empty-report"),
        pytest.param("### Dependency floors\n\nRaised: none\n", "Raised: none\n", id="report"),
    ],
)
def test_build_upgrade_pr_body(report, expected_suffix):
    body = build_upgrade_pr_body(report)
    assert body.startswith("This PR upgrades important dependencies of the CI environment.")
    assert body.endswith(expected_suffix)


def test_upgrade_has_floor_flag():
    option = next(p for p in upgrade.params if p.name == "upgrade_dependency_floors")
    assert option.default is True
