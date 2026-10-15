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

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

import pytest
import yaml

from airflow_breeze.utils.fork_ci_gate import (
    CANONICAL_PUSH_BRANCH_FILTERS,
    Decision,
    GateConfig,
    GateEvent,
    Reason,
    compile_branch_filter,
    decide,
    load_config,
    main,
    parse_config,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

SINCE = datetime(2026, 10, 1, tzinfo=UTC)
JOINED = datetime(2026, 10, 20, tzinfo=UTC)
AFTER = datetime(2026, 11, 1, tzinfo=UTC)


def _config(**overrides) -> GateConfig:
    values = {
        "enabled": True,
        "enabled_since": SINCE,
        "close_after_days": 7,
        "allowlist": {"Example Org": ("org-member",)},
        "pilot": {"pilot-user": JOINED},
    }
    values.update(overrides)
    return GateConfig(**values)


def _pr(
    *,
    author: str = "someone",
    association: str = "CONTRIBUTOR",
    created_at: datetime = AFTER,
    labels: tuple[str, ...] = (),
    head_repository: str = "someone/airflow",
) -> GateEvent:
    return GateEvent(
        event_name="pull_request",
        repository="apache/airflow",
        ref="refs/pull/1/merge",
        head_repository=head_repository,
        author_login=author,
        author_association=association,
        created_at=created_at,
        labels=labels,
    )


def _push(repository: str, branch: str) -> GateEvent:
    return GateEvent(event_name="push", repository=repository, ref=f"refs/heads/{branch}")


@pytest.mark.parametrize(
    ("event", "config_overrides", "expected"),
    [
        pytest.param(
            GateEvent("schedule", "someone/airflow", "refs/heads/main"),
            {},
            Decision(run=False, reason=Reason.FORK_SCHEDULE),
            id="schedule-in-fork",
        ),
        pytest.param(
            GateEvent("schedule", "apache/airflow", "refs/heads/main"),
            {},
            Decision(run=True, reason=Reason.SCHEDULED),
            id="schedule-upstream",
        ),
        pytest.param(
            GateEvent("workflow_dispatch", "someone/airflow", "refs/heads/main"),
            {},
            Decision(run=True, reason=Reason.SCHEDULED),
            id="dispatch-in-fork",
        ),
        pytest.param(
            _push("someone/airflow", "my-feature"),
            {"enabled": False},
            Decision(run=True, reason=Reason.FORK_PUSH),
            id="push-in-fork",
        ),
        pytest.param(
            _push("apache/airflow", "v3-1-test"),
            {},
            Decision(run=True, reason=Reason.CANONICAL_BRANCH),
            id="push-v-test",
        ),
        pytest.param(
            _push("apache/airflow", "providers-fab/v1-5"),
            {},
            Decision(run=True, reason=Reason.CANONICAL_BRANCH),
            id="push-providers-branch",
        ),
        pytest.param(
            _push("apache/airflow", "providers-cncf-kubernetes/v10-1"),
            {},
            Decision(run=True, reason=Reason.CANONICAL_BRANCH),
            id="push-providers-branch-two-words",
        ),
        pytest.param(
            _push("apache/airflow", "chart/v1-18x-test"),
            {},
            Decision(run=True, reason=Reason.CANONICAL_BRANCH),
            id="push-chart-test",
        ),
        pytest.param(
            _push("apache/airflow", "airflow-ctl/v0-1-test"),
            {},
            Decision(run=True, reason=Reason.CANONICAL_BRANCH),
            id="push-airflow-ctl-test",
        ),
        pytest.param(
            _push("apache/airflow", "main"),
            {"enabled": False},
            Decision(run=False, reason=Reason.ADHOC_BRANCH),
            id="push-main-upstream",
        ),
        pytest.param(
            _push("apache/airflow", "v3-1-stable"),
            {},
            Decision(run=False, reason=Reason.ADHOC_BRANCH),
            id="push-stable-upstream",
        ),
        pytest.param(
            _push("apache/airflow", "some-adhoc-branch"),
            {},
            Decision(run=False, reason=Reason.ADHOC_BRANCH),
            id="push-adhoc-upstream",
        ),
        pytest.param(
            _pr(labels=("use project ci",)),
            {},
            Decision(run=True, reason=Reason.LABEL_PROJECT_CI),
            id="label-project-ci",
        ),
        pytest.param(
            _pr(association="MEMBER", labels=("use fork ci",)),
            {},
            Decision(run=False, reason=Reason.LABEL_FORK_CI),
            id="label-fork-ci-enabled",
        ),
        pytest.param(
            _pr(association="MEMBER", labels=("use fork ci",)),
            {"enabled": False},
            Decision(run=False, reason=Reason.LABEL_FORK_CI),
            id="label-fork-ci-disabled",
        ),
        pytest.param(
            _pr(labels=("use fork ci", "use project ci")),
            {},
            Decision(run=True, reason=Reason.LABEL_PROJECT_CI),
            id="both-labels-project-wins",
        ),
        pytest.param(
            _pr(author="pilot-user", created_at=JOINED),
            {"enabled": False},
            Decision(run=False, reason=Reason.PILOT),
            id="pilot-at-join-instant-disabled",
        ),
        pytest.param(
            _pr(author="pilot-user", created_at=AFTER),
            {},
            Decision(run=False, reason=Reason.PILOT),
            id="pilot-enabled",
        ),
        pytest.param(
            _pr(author="Pilot-User", created_at=AFTER),
            {"enabled": False},
            Decision(run=False, reason=Reason.PILOT),
            id="pilot-login-case-insensitive",
        ),
        pytest.param(
            _pr(author="pilot-user", created_at=JOINED - timedelta(seconds=1)),
            {"enabled": False},
            Decision(run=True, reason=Reason.GATE_DISABLED),
            id="pilot-before-join-instant",
        ),
        pytest.param(
            _pr(author="pilot-user", created_at=AFTER, labels=("use project ci",)),
            {},
            Decision(run=True, reason=Reason.LABEL_PROJECT_CI),
            id="pilot-released-by-label",
        ),
        pytest.param(
            _pr(),
            {"enabled": False},
            Decision(run=True, reason=Reason.GATE_DISABLED),
            id="external-gate-disabled",
        ),
        pytest.param(
            _pr(head_repository="apache/airflow"),
            {},
            Decision(run=True, reason=Reason.HEAD_IS_UPSTREAM),
            id="head-is-upstream",
        ),
        pytest.param(
            _pr(association="MEMBER", labels=("use fork ci",), head_repository="apache/airflow"),
            {},
            Decision(run=True, reason=Reason.HEAD_IS_UPSTREAM),
            id="use-fork-ci-on-an-upstream-branch",
        ),
        pytest.param(
            _pr(created_at=SINCE - timedelta(seconds=1)),
            {},
            Decision(run=True, reason=Reason.BEFORE_ENABLED_SINCE),
            id="before-enabled-since",
        ),
        pytest.param(
            _pr(created_at=SINCE),
            {},
            Decision(run=False, reason=Reason.EXTERNAL),
            id="at-enabled-since",
        ),
        pytest.param(
            _pr(association="MEMBER"),
            {},
            Decision(run=True, reason=Reason.TRUSTED_AUTHOR),
            id="member",
        ),
        pytest.param(
            _pr(association="OWNER"),
            {},
            Decision(run=True, reason=Reason.TRUSTED_AUTHOR),
            id="owner",
        ),
        pytest.param(
            _pr(association="COLLABORATOR"),
            {},
            Decision(run=True, reason=Reason.TRUSTED_AUTHOR),
            id="collaborator",
        ),
        pytest.param(
            _pr(author="ORG-Member"),
            {},
            Decision(run=True, reason=Reason.ALLOWLISTED),
            id="allowlisted-case-insensitive",
        ),
        pytest.param(
            _pr(association="FIRST_TIME_CONTRIBUTOR"),
            {},
            Decision(run=False, reason=Reason.EXTERNAL),
            id="first-time-contributor",
        ),
        pytest.param(
            _pr(association="NONE"),
            {},
            Decision(run=False, reason=Reason.EXTERNAL),
            id="no-association",
        ),
    ],
)
def test_decide(event: GateEvent, config_overrides: dict, expected: Decision):
    assert decide(event, _config(**config_overrides)) == expected


def test_canonical_branch_filters_match_arm_push_trigger():
    workflow = yaml.safe_load((AIRFLOW_ROOT_PATH / ".github" / "workflows" / "ci-arm.yml").read_text())
    # PyYAML parses the bare `on:` key as boolean True.
    arm_branches = workflow[True]["push"]["branches"]

    assert list(CANONICAL_PUSH_BRANCH_FILTERS) == arm_branches


@pytest.mark.parametrize(
    ("pattern", "branch", "matches"),
    [
        pytest.param(
            "providers-[a-z]+-?[a-z]*/v[0-9]+-[0-9]+", "providers-apache-hive2/v1-2", True, id="star"
        ),
        pytest.param(
            "providers-[a-z]+-?[a-z]*/v[0-9]+-[0-9]+", "providers-amazon/x/v1-2", False, id="no-slash"
        ),
        pytest.param("v[0-9]+-[0-9]+-test", "v3-10-test", True, id="plus"),
        pytest.param("release/**", "release/a/b", True, id="double-star"),
        pytest.param("v1.2", "v1x2", False, id="literal-dot"),
    ],
)
def test_compile_branch_filter(pattern, branch, matches):
    assert bool(compile_branch_filter(pattern).match(branch)) is matches


def test_repository_config_parses():
    load_config(AIRFLOW_ROOT_PATH / ".github" / "fork-ci-gate.toml")


def test_parse_config_defaults():
    config = parse_config("")

    assert config.enabled is False
    assert config.close_after_days == 7
    assert config.allowlist == {}
    assert config.pilot == {}


def test_parse_config_lowercases_pilot_logins():
    config = parse_config('[pilot]\n"Some-User" = 2026-10-20T00:00:00Z\n')

    assert config.pilot_joined_at("some-user") == JOINED
    assert config.pilot_joined_at("SOME-USER") == JOINED
    assert config.pilot_joined_at("other") is None


@pytest.mark.parametrize(
    ("text", "message"),
    [
        pytest.param("enabled = 1\n", "enabled must be", id="enabled-not-bool"),
        pytest.param(
            "enabled_since = 2026-01-01T00:00:00\n", "enabled_since must be", id="enabled-since-naive"
        ),
        pytest.param('enabled_since = "2026-01-01"\n', "enabled_since must be", id="enabled-since-string"),
        pytest.param(
            "close_after_days = 0\n", "close_after_days must be", id="close-after-days-not-positive"
        ),
        pytest.param('[allowlist]\n"Org" = "login"\n', "allowlist entry 'Org'", id="allowlist-not-list"),
        pytest.param('[pilot]\n"login" = "2026-10-20"\n', "pilot entry 'login'", id="pilot-not-datetime"),
        pytest.param('[pilot]\n"login" = 2026-10-20T00:00:00\n', "pilot entry 'login'", id="pilot-naive"),
        pytest.param("enabled = \n", "Invalid fork CI gate configuration", id="invalid-toml"),
    ],
)
def test_parse_config_rejects_invalid(text: str, message: str):
    with pytest.raises(ValueError, match=message):
        parse_config(text)


@pytest.fixture
def gate_env(tmp_path):
    config = tmp_path / "fork-ci-gate.toml"
    config.write_text("enabled = true\nenabled_since = 2026-10-01T00:00:00Z\n")
    return {
        "GATE_CONFIG": str(config),
        "GITHUB_OUTPUT": str(tmp_path / "output"),
        "GITHUB_STEP_SUMMARY": str(tmp_path / "summary"),
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_REPOSITORY": "apache/airflow",
        "GITHUB_REF": "refs/pull/1/merge",
        "PR_NUMBER": "1",
        "PR_HEAD_REPO": "someone/airflow",
        "PR_AUTHOR": "someone",
        "PR_AUTHOR_ASSOCIATION": "CONTRIBUTOR",
        "PR_CREATED_AT": "2026-11-01T00:00:00Z",
    }


def _outputs(env: dict[str, str]) -> list[str]:
    return Path(env["GITHUB_OUTPUT"]).read_text().splitlines()


@mock.patch("airflow_breeze.utils.fork_ci_gate.subprocess.run", autospec=True)
@pytest.mark.parametrize(
    ("returncode", "stdout", "event_labels", "expected"),
    [
        pytest.param(0, "[]\n", "[]", ["project-ci=false", "reason=external"], id="external"),
        pytest.param(
            0, '["use project ci"]\n', "[]", ["project-ci=true", "reason=label_project_ci"], id="labelled"
        ),
        pytest.param(1, "", "[]", ["project-ci=false", "reason=external"], id="gh-fails-stays-gated"),
        pytest.param(
            0, "not json", "[]", ["project-ci=false", "reason=external"], id="bad-output-stays-gated"
        ),
        pytest.param(
            1,
            "",
            '["use project ci"]',
            ["project-ci=true", "reason=label_project_ci"],
            id="gh-fails-uses-event-labels",
        ),
    ],
)
def test_main_pull_request(mock_run, gate_env, returncode, stdout, event_labels, expected):
    mock_run.return_value = subprocess.CompletedProcess([], returncode, stdout=stdout, stderr="boom")
    gate_env["PR_EVENT_LABELS"] = event_labels

    assert main(gate_env) == 0

    assert _outputs(gate_env) == expected
    assert mock_run.call_args_list[0].args[0][:3] == ["gh", "api", "repos/apache/airflow/pulls/1"]


@mock.patch("airflow_breeze.utils.fork_ci_gate.subprocess.run", autospec=True)
@pytest.mark.parametrize(
    ("permission", "expected"),
    [
        pytest.param(
            "write\n", ["project-ci=true", "reason=trusted_author"], id="committer-with-private-membership"
        ),
        pytest.param("read\n", ["project-ci=false", "reason=external"], id="contributor"),
    ],
)
def test_main_checks_the_permission_of_an_external_author(mock_run, gate_env, permission, expected):
    mock_run.side_effect = [
        subprocess.CompletedProcess([], 0, stdout="[]\n", stderr=""),
        subprocess.CompletedProcess([], 0, stdout=permission, stderr=""),
    ]

    main(gate_env)

    assert _outputs(gate_env) == expected
    assert mock_run.call_args.args[0][:3] == [
        "gh",
        "api",
        "repos/apache/airflow/collaborators/someone/permission",
    ]


@mock.patch(
    "airflow_breeze.utils.fork_ci_gate.subprocess.run", autospec=True, side_effect=FileNotFoundError("gh")
)
def test_main_without_gh_uses_the_event_labels(mock_run, gate_env):
    gate_env["PR_EVENT_LABELS"] = '["use project ci"]'

    main(gate_env)

    assert _outputs(gate_env) == ["project-ci=true", "reason=label_project_ci"]


@mock.patch("airflow_breeze.utils.fork_ci_gate.subprocess.run", autospec=True)
def test_main_fork_push_does_not_read_labels(mock_run, gate_env):
    gate_env.update(
        GITHUB_EVENT_NAME="push", GITHUB_REPOSITORY="someone/airflow", GITHUB_REF="refs/heads/fix"
    )

    main(gate_env)

    assert _outputs(gate_env) == ["project-ci=true", "reason=fork_push"]
    mock_run.assert_not_called()
