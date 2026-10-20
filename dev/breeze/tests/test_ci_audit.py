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
import subprocess
from datetime import UTC, datetime
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import audit
from airflow_breeze.utils.ci_audit import CheckResult, CheckStatus, Role, run_audit, run_tool
from airflow_breeze.utils.confirm import Answer
from airflow_breeze.utils.fork_ci_gate import GateConfig

REMOTES_OK = (
    "origin\tgit@github.com:someone/airflow.git (fetch)\n"
    "upstream\thttps://github.com/apache/airflow.git (fetch)\n"
)


# (return code, stdout) or (return code, stdout, stderr) of a command.
Output = tuple[int, str] | tuple[int, str, str]


def _machine(**overrides: Output | None) -> dict[str, Output | None]:
    """Outputs of every command the audit runs, keyed by a short name."""
    machine: dict[str, Output | None] = {
        "gh_version": (0, "gh version 2.80.0 (2026-09-01)"),
        "login": (0, "someone"),
        "remotes": (0, REMOTES_OK),
        "repo": (0, json.dumps({"owner": "someone", "parent": "apache/airflow"})),
        "membership": (1, ""),
        "workflow": (0, "active"),
        "behind": (0, "12"),
        "branch": (0, "my-change"),
        "tracking": (0, "origin/my-change"),
    }
    machine.update(overrides)
    return machine


def _runner(machine):
    keys = {
        ("gh", "--version"): "gh_version",
        ("gh", "api", "user", "--jq", ".login"): "login",
        ("git", "remote", "-v"): "remotes",
        ("gh", "api", "user/memberships/orgs/apache", "--jq", ".state"): "membership",
        ("git", "rev-parse", "--abbrev-ref", "HEAD"): "branch",
        ("git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"): "tracking",
    }

    def run(command):
        command = tuple(command)
        key = keys.get(command)
        if key is None and command[:2] == ("gh", "api"):
            if "/actions/workflows/" in command[2]:
                key = "workflow"
            elif "/compare/" in command[2]:
                key = "behind"
            elif command[2].startswith("repos/"):
                key = "repo"
        if key is None:
            raise AssertionError(f"unexpected command {command}")
        returncode, stdout, *stderr = machine[key]
        return subprocess.CompletedProcess(list(command), returncode, stdout=stdout, stderr="".join(stderr))

    return run


ENABLED = GateConfig(enabled=True)


def _audit(machine, config=ENABLED, collaborators=frozenset()):
    return {
        result.number: result
        for result in run_audit(run=_runner(machine), config=config, collaborators=collaborators)
    }


def test_healthy_external_setup():
    results = _audit(_machine())

    assert [r.status for r in results.values()] == [
        CheckStatus.OK,
        CheckStatus.OK,
        CheckStatus.OK,
        CheckStatus.INFO,
        CheckStatus.OK,
        CheckStatus.OK,
        CheckStatus.INFO,
    ]
    assert results[4].finding == f"you are a {Role.EXTERNAL.value}"


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        pytest.param({"gh_version": (1, "")}, "not installed", id="no-gh"),
        pytest.param({"gh_version": (0, "gh version 2.10.0")}, "older than", id="old-gh"),
        pytest.param({"login": (1, "")}, "not authenticated", id="not-logged-in"),
    ],
)
def test_gh_problems_stop_the_audit(overrides, message):
    results = _audit(_machine(**overrides))

    assert list(results) == [1]
    assert results[1].status == CheckStatus.FAIL
    assert message in results[1].finding


@pytest.mark.parametrize(
    ("remotes", "fix"),
    [
        pytest.param(
            "origin\thttps://github.com/apache/airflow.git (fetch)\n",
            (
                "git remote rename origin upstream",
                "git remote add origin https://github.com/someone/airflow.git",
            ),
            id="cloned-upstream-as-origin",
        ),
        pytest.param(
            "origin\tgit@github.com:someone/airflow.git (fetch)\napache\thttps://github.com/apache/airflow (fetch)\n",
            ("git remote rename apache upstream",),
            id="upstream-named-apache",
        ),
        pytest.param(
            "origin\tgit@github.com:someone/airflow.git (fetch)\n",
            ("git remote add upstream https://github.com/apache/airflow.git",),
            id="no-upstream",
        ),
        pytest.param(
            "origin\thttps://github.com/apache/airflow.git (fetch)\n"
            "upstream\thttps://github.com/apache/airflow.git (fetch)\n",
            ("git remote set-url origin https://github.com/someone/airflow.git",),
            id="origin-also-upstream",
        ),
        pytest.param(
            "origin\tgit@github.com:someone/airflow.git (fetch)\n"
            "upstream\thttps://github.com/someone-else/airflow.git (fetch)\n",
            ("git remote set-url upstream https://github.com/apache/airflow.git",),
            id="upstream-points-elsewhere",
        ),
    ],
)
def test_remote_fixes(remotes, fix):
    results = _audit(_machine(remotes=(0, remotes)))

    assert results[2].status == CheckStatus.FAIL
    assert results[2].fix == fix


def test_origin_is_added_once_when_upstream_was_cloned_as_origin():
    results = _audit(_machine(remotes=(0, "origin\thttps://github.com/apache/airflow.git (fetch)\n")))

    assert results[3].fix == ("gh repo fork apache/airflow --remote=false",)


@pytest.mark.parametrize(
    ("repo", "message"),
    [
        pytest.param(json.dumps({"owner": "someone", "parent": None}), "is not a fork", id="not-a-fork"),
        pytest.param(
            json.dumps({"owner": "other", "parent": "apache/airflow"}), "is not owned", id="not-owned"
        ),
    ],
)
def test_fork_problems(repo, message):
    results = _audit(_machine(repo=(0, repo)))

    assert results[3].status == CheckStatus.FAIL
    assert message in results[3].finding
    assert 5 not in results


@pytest.mark.parametrize(
    ("config", "membership", "collaborators", "workflow", "role", "status", "fix"),
    [
        pytest.param(
            GateConfig(enabled=True),
            (1, ""),
            frozenset(),
            (0, "disabled_fork"),
            Role.EXTERNAL,
            CheckStatus.FAIL,
            ("gh workflow enable ci-amd.yml -R someone/airflow",),
            id="external-disabled",
        ),
        pytest.param(
            GateConfig(enabled=True),
            (1, ""),
            frozenset(),
            (1, "", "gh: Not Found (HTTP 404)"),
            Role.EXTERNAL,
            CheckStatus.FAIL,
            (
                "gh repo sync someone/airflow --branch main",
                "gh workflow enable ci-amd.yml -R someone/airflow",
            ),
            id="external-missing-workflow",
        ),
        pytest.param(
            GateConfig(enabled=False),
            (1, ""),
            frozenset(),
            (0, "disabled_fork"),
            Role.EXTERNAL,
            CheckStatus.INFO,
            (),
            id="external-before-switch-on",
        ),
        pytest.param(
            GateConfig(enabled=False, pilot={"someone": datetime(2026, 10, 20, tzinfo=UTC)}),
            (1, ""),
            frozenset(),
            (0, "disabled_fork"),
            Role.PILOT,
            CheckStatus.FAIL,
            ("gh workflow enable ci-amd.yml -R someone/airflow",),
            id="pilot-disabled",
        ),
        pytest.param(
            GateConfig(enabled=True),
            (0, "active"),
            frozenset(),
            (0, "active"),
            Role.COMMITTER,
            CheckStatus.INFO,
            ("gh workflow disable ci-amd.yml -R someone/airflow",),
            id="committer-enabled-is-not-a-failure",
        ),
        pytest.param(
            GateConfig(enabled=True),
            (1, ""),
            frozenset(),
            (1, "", "HTTP 502: Bad Gateway"),
            Role.EXTERNAL,
            CheckStatus.INFO,
            (),
            id="workflow-call-failed",
        ),
        pytest.param(
            GateConfig(enabled=True, pilot={"someone": datetime(2026, 10, 20, tzinfo=UTC)}),
            (0, "active"),
            frozenset(),
            (0, "disabled_fork"),
            Role.PILOT,
            CheckStatus.FAIL,
            ("gh workflow enable ci-amd.yml -R someone/airflow",),
            id="committer-in-the-pilot",
        ),
        pytest.param(
            GateConfig(enabled=True),
            (1, ""),
            frozenset({"SomeOne"}),
            (0, "disabled_manually"),
            Role.COLLABORATOR,
            CheckStatus.OK,
            (),
            id="collaborator-disabled",
        ),
        pytest.param(
            GateConfig(enabled=True, allowlist={"Org": ("someone",)}),
            (1, ""),
            frozenset(),
            (0, "disabled_fork"),
            Role.EXEMPTED,
            CheckStatus.OK,
            (),
            id="exempted-disabled",
        ),
    ],
)
def test_fork_workflows_by_role(config, membership, collaborators, workflow, role, status, fix):
    results = _audit(_machine(membership=membership, workflow=workflow), config, collaborators)

    assert results[4].finding == f"you are a {role.value}"
    assert results[5].status == status
    assert results[5].fix == fix


@pytest.mark.parametrize(
    ("behind", "status"),
    [
        pytest.param((0, "200"), CheckStatus.OK, id="at-threshold"),
        pytest.param((0, "201"), CheckStatus.FAIL, id="too-far-behind"),
        pytest.param((1, ""), CheckStatus.INFO, id="compare-failed"),
    ],
)
def test_fork_main_freshness(behind, status):
    results = _audit(_machine(behind=behind))

    assert results[6].status == status


@mock.patch("airflow_breeze.commands.ci_commands.run_command", autospec=True)
@mock.patch("airflow_breeze.commands.ci_commands.user_confirm", autospec=True, return_value=Answer.YES)
@mock.patch("airflow_breeze.utils.ci_audit.run_audit", autospec=True)
@pytest.mark.parametrize(
    ("args", "second_report", "exit_code", "expected_commands"),
    [
        pytest.param([], None, 1, [], id="report-only"),
        pytest.param(["--fix"], [], 0, [["git", "remote", "add", "upstream", "u"]], id="fixed"),
    ],
)
def test_audit_command(
    mock_run_audit, mock_confirm, mock_run_command, args, second_report, exit_code, expected_commands
):
    failing = [CheckResult(2, "remotes", CheckStatus.FAIL, "no upstream", ("git remote add upstream u",))]
    mock_run_audit.side_effect = [failing] if second_report is None else [failing, second_report]

    result = CliRunner().invoke(audit, args)

    assert result.exit_code == exit_code, result.output
    assert [c.args[0] for c in mock_run_command.call_args_list] == expected_commands


@pytest.mark.parametrize(
    ("branch", "tracking", "expected"),
    [
        pytest.param((0, "main"), (0, "origin/main"), "create a branch", id="on-main"),
        pytest.param((0, "fix"), (1, ""), "has no upstream", id="untracked"),
        pytest.param((0, "fix"), (0, "upstream/fix"), "tracks upstream/fix; push it", id="tracks-upstream"),
        pytest.param((0, "fix"), (0, "origin/fix"), "`fix` tracks origin/fix", id="tracks-origin"),
    ],
)
def test_branch(branch, tracking, expected):
    results = _audit(_machine(branch=branch, tracking=tracking))

    assert expected in results[7].finding


def test_run_tool_reports_a_missing_executable():
    result = run_tool(["breeze-audit-no-such-executable", "--version"])

    assert result.returncode == 127
    assert "breeze-audit-no-such-executable" in result.stderr


def test_missing_fork_fails():
    results = _audit(_machine(repo=(1, "", "gh: Not Found (HTTP 404)")))

    assert results[3].status == CheckStatus.FAIL
    assert results[3].finding == "someone/airflow does not exist"
    assert 5 not in results


def test_failed_fork_lookup_is_not_reported_as_a_wrong_fork():
    results = _audit(_machine(repo=(1, "", "HTTP 502: Bad Gateway")))

    assert results[3].status == CheckStatus.INFO
    assert results[3].finding.startswith("could not check someone/airflow")
    assert 5 not in results


@pytest.mark.parametrize(
    ("membership", "status"),
    [
        pytest.param((0, "active"), CheckStatus.INFO, id="committer"),
        pytest.param((1, ""), CheckStatus.FAIL, id="external"),
    ],
)
def test_fork_setup_matters_only_for_fork_ci(membership, status):
    results = _audit(
        _machine(
            membership=membership, remotes=(0, "origin\thttps://github.com/apache/airflow.git (fetch)\n")
        )
    )

    assert results[2].status == status
    assert results[3].status == status


def test_wrong_upstream_finding_does_not_print_credentials():
    remotes = (
        "origin\tgit@github.com:someone/airflow.git (fetch)\n"
        "upstream\thttps://someone:ghp_secret@github.com/someone-else/airflow.git (fetch)\n"
    )

    results = _audit(_machine(remotes=(0, remotes)))

    assert "ghp_secret" not in results[2].finding
    assert "https://github.com/someone-else/airflow.git" in results[2].finding
