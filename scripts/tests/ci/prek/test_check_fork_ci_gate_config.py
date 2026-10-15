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
from check_fork_ci_gate_config import find_violations, main, resolve_base_ref

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
BASE = 'enabled_since = 2026-01-01T00:00:00Z\n[pilot]\n"old-user" = 2026-09-01T00:00:00Z\n'


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
