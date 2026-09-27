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

from airflow_breeze.commands.ci_commands import (
    UPGRADE_COMMANDS,
    build_update_pr_body_command,
    build_upgrade_pr_body,
    get_step_enabled,
    read_floors_report,
    upgrade,
)

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


def test_existing_pr_body_is_replaced_with_the_new_report():
    # The upgrade branch name is stable, so most runs update an open PR instead of creating one.
    assert build_update_pr_body_command("ci-upgrade-main", "body with report") == [
        "gh",
        "pr",
        "edit",
        "ci-upgrade-main",
        "--repo",
        "apache/airflow",
        "--body",
        "body with report",
    ]


ALL_STEPS_ON = dict(
    autoupdate=True,
    update_chart_dependencies=True,
    upgrade_important_versions=True,
    upgrade_dependency_floors=True,
    update_uv_lock=True,
)


def test_step_enabled_covers_every_step():
    assert set(get_step_enabled(**ALL_STEPS_ON)) == set(STEP_NAMES)


def test_no_upgrade_dependency_floors_disables_only_that_step():
    enabled = get_step_enabled(**{**ALL_STEPS_ON, "upgrade_dependency_floors": False})
    assert [name for name, on in enabled.items() if not on] == ["upgrade-dependency-floors"]


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        pytest.param("### Dependency floors\n", "### Dependency floors\n", id="report"),
        pytest.param(None, None, id="missing"),
    ],
)
def test_read_floors_report_removes_its_temp_dir(tmp_path, content, expected):
    report_dir = tmp_path / "floors"
    report_dir.mkdir()
    report_path = report_dir / "dependency-floors.md"
    if content is not None:
        report_path.write_text(content)
    assert read_floors_report(report_path) == expected
    assert not report_dir.exists()
