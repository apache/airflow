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

from datetime import UTC, datetime
from unittest import mock

import pytest
from check_fork_ci_gate_config import find_asf_yaml_sync_violations, find_violations, main, resolve_base_ref

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
BASE = 'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"old-user" = 2026-09-01T00:00:00Z\n'
VALID_ASF_YAML = """
github:
  pull_requests:
    creation_cap:
      bypass_users:
        - alice
        - Bob
        - charlie
"""


@pytest.mark.parametrize(
    ("base", "new"),
    [
        pytest.param(BASE, BASE, id="unchanged"),
        pytest.param(BASE, BASE + '"new-user" = 2026-10-16T00:00:00Z\n', id="new-pilot-in-future"),
        pytest.param(BASE, BASE.replace("2026-01-01", "2026-10-20"), id="enabled-since-moved-to-future"),
        pytest.param(
            BASE.replace("2026-01-01", "2026-10-01"), BASE, id="enabled-since-moved-back-for-backlog-reset"
        ),
        pytest.param(
            "enabled = false\n" + BASE,
            "enabled = true\n" + BASE.replace("2026-01-01", "2026-10-20"),
            id="switched-on-with-future-enabled-since",
        ),
        pytest.param(None, "enabled_since = 2026-01-01T00:00:00Z\n", id="no-base-file"),
        pytest.param(None, BASE, id="backported-to-a-branch-without-the-file"),
        pytest.param("not = [valid", "enabled_since = 2026-01-01T00:00:00Z\n", id="unparsable-base"),
    ],
)
def test_accepts(base, new):
    assert find_violations(base, new, now=NOW) == []


@pytest.mark.parametrize(
    ("base", "new", "message"),
    [
        pytest.param(
            BASE, BASE + '"new-user" = 2026-10-15T00:00:00Z\n', "'new-user'", id="new-pilot-in-past"
        ),
        pytest.param(
            BASE,
            BASE.replace("2026-09-01", "2026-10-01"),
            "'old-user'",
            id="changed-pilot-in-past",
        ),
        pytest.param(
            BASE, BASE.replace("2026-01-01", "2026-10-01"), "enabled_since moved later", id="since-in-past"
        ),
        pytest.param(
            "enabled = false\n" + BASE,
            "enabled = true\n" + BASE,
            "enabled is switched on",
            id="switched-on-with-past-enabled-since",
        ),
        pytest.param(BASE, "enabled = 1\n", "enabled must be", id="invalid-config"),
        pytest.param("not = [valid", BASE, "'old-user'", id="past-pilot-with-unparsable-base"),
    ],
)
def test_rejects(base, new, message):
    violations = find_violations(base, new, now=NOW)

    assert len(violations) == 1
    assert message in violations[0]


@mock.patch("check_fork_ci_gate_config._git", autospec=True)
@pytest.mark.parametrize(
    ("base_ref_env", "git_results", "expected"),
    [
        pytest.param("v3-1-test", {"fetch": ""}, "FETCH_HEAD", id="ci-fetches-base-branch"),
        pytest.param(None, {"rev-parse": "x", "merge-base": "abc\n"}, "abc", id="local-merge-base"),
        pytest.param(None, {"fetch": ""}, "FETCH_HEAD", id="fork-push-fetches-upstream-main"),
        pytest.param(
            None,
            {"rev-parse": "x\n", "merge-base": "x\n", "fetch": ""},
            "FETCH_HEAD",
            id="fork-main-push-is-its-own-merge-base",
        ),
        pytest.param(None, {}, None, id="no-base-available"),
    ],
)
def test_resolve_base_ref(mock_git, monkeypatch, base_ref_env, git_results, expected):
    if base_ref_env:
        monkeypatch.setenv("GITHUB_BASE_REF", base_ref_env)
    else:
        monkeypatch.delenv("GITHUB_BASE_REF", raising=False)
    mock_git.side_effect = lambda *args: git_results.get(args[0])

    assert resolve_base_ref() == expected
    if base_ref_env:
        mock_git.assert_called_once_with("fetch", "--quiet", "--depth=1", "origin", "refs/heads/v3-1-test")


@mock.patch("check_fork_ci_gate_config.resolve_base_ref", autospec=True, return_value=None)
def test_main_fails_without_a_base(mock_resolve, capsys):
    assert main() == 1
    assert "no base revision to compare with" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("asf_yaml", "pilot_users"),
    [
        pytest.param(VALID_ASF_YAML, ["alice", "Bob", "charlie"], id="exact-sync"),
        pytest.param(VALID_ASF_YAML, ["charlie", "alice", "Bob"], id="unordered-pilot-set-matches"),
        pytest.param(
            """
github:
  pull_requests:
    creation_cap:
      bypass_users: []
""",
            [],
            id="empty-lists-in-sync",
        ),
    ],
)
def test_asf_yaml_sync_accepts(asf_yaml, pilot_users):
    assert find_asf_yaml_sync_violations(asf_yaml, pilot_users) == []


@pytest.mark.parametrize(
    ("asf_yaml", "pilot_users", "expected_message"),
    [
        pytest.param(
            VALID_ASF_YAML,
            ["alice", "Bob", "charlie", "david"],
            "missing pilot users in bypass_users: david",
            id="pilot-missing-in-asf-yaml",
        ),
        pytest.param(
            VALID_ASF_YAML,
            ["alice", "Bob"],
            "extra users in bypass_users not in [pilot]: charlie",
            id="extra-user-in-asf-yaml",
        ),
        pytest.param(
            """
github:
  pull_requests:
    creation_cap:
      bypass_users:
        - charlie
        - alice
""",
            ["alice", "charlie"],
            "bypass_users must be sorted alphabetically (case-insensitive)",
            id="unsorted-bypass-users",
        ),
        pytest.param(
            """
github:
  pull_requests:
    creation_cap:
      bypass_users:
        - alice
        - alice
""",
            ["alice"],
            "duplicate users in bypass_users: alice",
            id="duplicate-bypass-user",
        ),
        pytest.param(
            """
github:
  pull_requests:
    creation_cap:
      bypass_users:
        - Alice
        - alice
""",
            ["Alice"],
            "duplicate users in bypass_users: alice",
            id="duplicate-bypass-user-case-insensitive",
        ),
        pytest.param(
            "not: [valid: yaml",
            ["alice"],
            "failed to parse YAML",
            id="malformed-yaml",
        ),
        pytest.param(
            "42",
            ["alice"],
            "root must be a YAML mapping",
            id="root-not-mapping",
        ),
        pytest.param(
            "github: null",
            ["alice"],
            "github must be a mapping",
            id="github-null",
        ),
        pytest.param(
            "github:\n  pull_requests: null",
            ["alice"],
            "github.pull_requests must be a mapping",
            id="pull-requests-null",
        ),
        pytest.param(
            "github:\n  pull_requests:\n    creation_cap: null",
            ["alice"],
            "github.pull_requests.creation_cap must be a mapping",
            id="creation-cap-null",
        ),
        pytest.param(
            "github:\n  pull_requests:\n    creation_cap: {}",
            ["alice"],
            "github.pull_requests.creation_cap.bypass_users is missing",
            id="bypass-users-missing",
        ),
        pytest.param(
            "github:\n  pull_requests:\n    creation_cap:\n      bypass_users: 'alice'",
            ["alice"],
            "github.pull_requests.creation_cap.bypass_users must be a list",
            id="bypass-users-not-list",
        ),
        pytest.param(
            "github:\n  pull_requests:\n    creation_cap:\n      bypass_users:\n        - 123",
            ["123"],
            "all entries in bypass_users must be strings",
            id="bypass-users-non-string-entries",
        ),
        pytest.param(
            """
github:
  pull_requests:
    creation_cap:
      bypass_users:
        - Alice
""",
            ["alice"],
            "casing mismatch for pilot users: Alice in .asf.yaml vs alice in [pilot]",
            id="casing-mismatch",
        ),
    ],
)
def test_asf_yaml_sync_rejects(asf_yaml, pilot_users, expected_message):
    violations = find_asf_yaml_sync_violations(asf_yaml, pilot_users)
    assert any(expected_message in v for v in violations)


@mock.patch("check_fork_ci_gate_config.resolve_base_ref", autospec=True, return_value="HEAD")
@mock.patch("check_fork_ci_gate_config._git", autospec=True)
def test_main_passes_when_gate_and_asf_yaml_are_in_sync(mock_git, mock_resolve, tmp_path, monkeypatch):
    monkeypatch.setattr("check_fork_ci_gate_config.AIRFLOW_ROOT_PATH", tmp_path)
    mock_git.return_value = 'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"alice" = 2026-01-01T00:00:00Z\n'
    (tmp_path / ".github").mkdir(parents=True)
    (tmp_path / ".github" / "fork-ci-gate.toml").write_text(
        'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"alice" = 2026-01-01T00:00:00Z\n'
    )
    (tmp_path / ".asf.yaml").write_text(
        "github:\n  pull_requests:\n    creation_cap:\n      bypass_users:\n        - alice\n"
    )
    assert main() == 0


@mock.patch("check_fork_ci_gate_config.resolve_base_ref", autospec=True, return_value="HEAD")
@mock.patch("check_fork_ci_gate_config._git", autospec=True)
def test_main_fails_when_asf_yaml_is_out_of_sync(mock_git, mock_resolve, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("check_fork_ci_gate_config.AIRFLOW_ROOT_PATH", tmp_path)
    mock_git.return_value = 'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"alice" = 2026-01-01T00:00:00Z\n'
    (tmp_path / ".github").mkdir(parents=True)
    (tmp_path / ".github" / "fork-ci-gate.toml").write_text(
        'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"alice" = 2026-01-01T00:00:00Z\n'
    )
    (tmp_path / ".asf.yaml").write_text(
        "github:\n  pull_requests:\n    creation_cap:\n      bypass_users:\n        - bob\n"
    )
    assert main() == 1
    err = capsys.readouterr().err
    assert ".asf.yaml: missing pilot users in bypass_users: alice" in err
    assert ".asf.yaml: extra users in bypass_users not in [pilot]: bob" in err


@mock.patch("check_fork_ci_gate_config.resolve_base_ref", autospec=True, return_value="HEAD")
@mock.patch("check_fork_ci_gate_config._git", autospec=True)
def test_main_fails_when_asf_yaml_is_missing(mock_git, mock_resolve, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("check_fork_ci_gate_config.AIRFLOW_ROOT_PATH", tmp_path)
    mock_git.return_value = 'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"alice" = 2026-01-01T00:00:00Z\n'
    (tmp_path / ".github").mkdir(parents=True)
    (tmp_path / ".github" / "fork-ci-gate.toml").write_text(
        'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"alice" = 2026-01-01T00:00:00Z\n'
    )
    assert main() == 1
    assert ".asf.yaml: file does not exist" in capsys.readouterr().err


@mock.patch("check_fork_ci_gate_config.resolve_base_ref", autospec=True, return_value="HEAD")
@mock.patch("check_fork_ci_gate_config._git", autospec=True)
def test_main_reports_a_pilot_that_is_not_a_table(mock_git, mock_resolve, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("check_fork_ci_gate_config.AIRFLOW_ROOT_PATH", tmp_path)
    mock_git.return_value = None
    (tmp_path / ".github").mkdir(parents=True)
    (tmp_path / ".github" / "fork-ci-gate.toml").write_text('pilot = ["alice"]\n')
    (tmp_path / ".asf.yaml").write_text(
        "github:\n  pull_requests:\n    creation_cap:\n      bypass_users:\n        - alice\n"
    )
    assert main() == 1
    err = capsys.readouterr().err
    assert ".github/fork-ci-gate.toml: pilot must be a table" in err
    assert ".asf.yaml" not in err
