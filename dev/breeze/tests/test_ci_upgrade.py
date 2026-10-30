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
from subprocess import CompletedProcess
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import (
    UPGRADE_COMMANDS,
    build_update_pr_body_command,
    build_upgrade_pr_body,
    get_step_enabled,
    mirror_base_images_before_upgrade,
    push_upgrade_to_target_branch,
    read_floors_report,
    upgrade,
)
from airflow_breeze.utils.git_remotes import Remote

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


def test_provider_requirement_tables_are_regenerated_after_the_floor_step():
    assert STEP_NAMES.index("upgrade-dependency-floors") < STEP_NAMES.index(
        "regenerate-provider-requirement-tables"
    )
    command = dict(UPGRADE_COMMANDS)["regenerate-provider-requirement-tables"]
    assert command.startswith("prek --all-files ")
    assert command.endswith(" update-providers-build-files sync-provider-readme")


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


def test_upgrade_mirrors_the_base_images_by_default():
    option = next(p for p in upgrade.params if p.name == "mirror_base_images")
    assert option.default is True


def test_upgrade_has_latest_package_managers_flag():
    option = next(p for p in upgrade.params if p.name == "latest_package_managers")
    assert option.default is False


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


def test_no_upgrade_dependency_floors_disables_only_the_floor_steps():
    enabled = get_step_enabled(**{**ALL_STEPS_ON, "upgrade_dependency_floors": False})
    assert [name for name, on in enabled.items() if not on] == [
        "upgrade-dependency-floors",
        "regenerate-provider-requirement-tables",
    ]


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


@pytest.mark.parametrize(
    ("target_branch", "trigger_upgrade_check"),
    [
        pytest.param("main", False, id="main-upgrade-runs-once"),
        pytest.param("v3-4-test", True, id="release-branch-also-upgrades-main"),
    ],
)
@mock.patch("airflow_breeze.commands.ci_commands.mirror_hardened_base_images", autospec=True)
def test_upgrade_mirrors_the_base_images_with_its_token(
    mock_mirror, monkeypatch, target_branch, trigger_upgrade_check
):
    monkeypatch.setenv("GITHUB_TOKEN", "ambient-token")

    mirror_base_images_before_upgrade(target_branch, "explicit-token")

    assert os.environ["GITHUB_TOKEN"] == "explicit-token"
    mock_mirror.assert_called_once_with(
        "apache/airflow",
        hint="Or skip it with --no-mirror-base-images.",
        trigger_upgrade_check=trigger_upgrade_check,
    )


APACHE_REMOTE = Remote(name="upstream", url="https://github.com/apache/airflow.git", repo="apache/airflow")


class FakeGit:
    """Answers the git calls of the upgrade for a checkout on a branch, with or without local changes."""

    def __init__(self, branch: str, head: str, upstream_head: str, porcelain: str):
        self.answers = {
            ("rev-parse", "--abbrev-ref"): branch,
            ("rev-parse", "HEAD"): head,
            ("status", "--porcelain"): porcelain,
        }
        self.upstream_head = upstream_head
        self.calls: list[list[str]] = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(cmd)
        if cmd[:2] == ["git", "rev-parse"] and cmd[-1].startswith("upstream/"):
            return CompletedProcess(cmd, 0, stdout=self.upstream_head)
        return CompletedProcess(cmd, 0, stdout=self.answers.get(tuple(cmd[1:3]), ""))


@pytest.mark.parametrize(
    ("branch", "head", "porcelain"),
    [
        pytest.param("v3-4-test", "abc", " M Dockerfile\n", id="uncommitted-changes"),
        pytest.param("v3-4-test", "abc", "?? notes.txt\n", id="untracked-files"),
        pytest.param("my-feature", "abc", "", id="other-branch"),
        pytest.param("v3-4-test", "old", "", id="behind-the-tip"),
    ],
)
@mock.patch("airflow_breeze.commands.ci_commands.get_remotes", autospec=True, return_value=[APACHE_REMOTE])
def test_push_to_target_branch_fails_instead_of_touching_the_checkout(mock_remotes, branch, head, porcelain):
    git = FakeGit(branch=branch, head=head, upstream_head="abc", porcelain=porcelain)
    with mock.patch("airflow_breeze.commands.ci_commands.run_command", autospec=True, side_effect=git):
        result = CliRunner().invoke(
            upgrade, ["--target-branch", "v3-4-test", "--push-to-target-branch", "--switch-to-base"]
        )

    assert result.exit_code == 1
    assert "git pull --ff-only upstream" in result.output
    assert not [
        call for call in git.calls if call[:2] in (["git", "checkout"], ["git", "reset"], ["git", "clean"])
    ]


def test_push_to_target_branch_refuses_main():
    result = CliRunner().invoke(upgrade, ["--target-branch", "main", "--push-to-target-branch"])

    assert result.exit_code == 1


@mock.patch("airflow_breeze.commands.ci_commands.run_command", autospec=True)
def test_push_upgrade_to_target_branch_pushes_the_commit_onto_the_branch(mock_run_command):
    mock_run_command.return_value = CompletedProcess([], 0, stdout="", stderr="")

    push_upgrade_to_target_branch("upstream", "v3-4-test", floors_report=None)

    commands = [call.args[0] for call in mock_run_command.call_args_list]
    assert commands[-1] == ["git", "push", "upstream", "HEAD:refs/heads/v3-4-test"]
    assert ["git", "commit", "--message", "[v3-4-test] CI: Upgrade important CI environment"] in commands
