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
import os
import re
import subprocess
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.verify_commands import (
    find_default_base_ref,
    get_changed_files_against,
    is_base_possibly_stale,
    run_verification_commands,
    verify,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _default_base_ref() -> str:
    return "main"


def _no_merged_commits_missing(base_ref: str) -> bool:
    return False


@pytest.fixture(autouse=True)
def _no_git_lookups_against_the_checkout():
    """Keep CLI tests independent of the developer's remotes; tests that need these patch them again."""
    # Plain functions rather than mocks, so tests can still patch these with autospec=True.
    with (
        patch("airflow_breeze.commands.verify_commands.find_default_base_ref", new=_default_base_ref),
        patch(
            "airflow_breeze.commands.verify_commands.is_base_possibly_stale",
            new=_no_merged_commits_missing,
        ),
    ):
        yield


@pytest.fixture(autouse=True)
def _pin_branch_to_main():
    """Assert ``main`` behaviour regardless of the checked-out branch.

    These tests hardcode ``main`` expectations, but ``branch_defaults.AIRFLOW_BRANCH`` is different on a
    release branch (``vX-Y-test``), which flows into the base ref and the ``default_branch`` passed to
    selective checks. Pin the constants so the suite passes on any branch.
    """
    with (
        patch("airflow_breeze.commands.verify_commands.AIRFLOW_BRANCH", "main"),
        patch(
            "airflow_breeze.commands.verify_commands.DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH", "constraints-main"
        ),
    ):
        yield


@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_json_output_keeps_stdout_parseable(mock_files, monkeypatch):
    # GitHub Actions makes breeze print a "how to reproduce" banner after every command.
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "0" * 40)
    result = CliRunner().invoke(verify, ["--json"], catch_exceptions=False)
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload) == {
        "base_ref",
        "default_python_version",
        "full_tests_needed",
        "changed_files",
        "items",
        "manual_prek_hooks",
    }
    assert payload["changed_files"] == ["airflow-core/docs/index.rst"]
    assert payload["items"] == [
        {"kind": "docs", "command": "breeze build-docs apache-airflow", "runs_in": "breeze"}
    ]


@patch(
    "airflow_breeze.commands.verify_commands.run_command",
    autospec=True,
    return_value=CompletedProcess(
        args=[], returncode=128, stdout="", stderr="fatal: Not a valid object name"
    ),
)
def test_unknown_base_ref_is_a_clean_usage_error(mock_run):
    result = CliRunner().invoke(verify, ["--base-ref", "no-such-branch"])
    assert result.exit_code == 1
    rendered = " ".join(ANSI.sub("", result.output).replace("│", " ").split())
    assert "git merge-base failed for base ref 'no-such-branch': fatal: Not a valid object name" in rendered


@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_selective_checks_narration_is_hidden_unless_verbose(mock_files):
    result = CliRunner().invoke(verify, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert "FileGroupForCi" not in result.output
    assert "FileGroupForCi" not in result.stderr


@pytest.mark.parametrize("args", [[], ["--full"]])
@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("providers/amazon/src/airflow/providers/amazon/hooks/s3.py",),
)
def test_static_checks_are_left_to_prek(mock_files, args: list[str]):
    result = CliRunner().invoke(verify, args, catch_exceptions=False)
    assert result.exit_code == 0
    output = " ".join(result.output.split())
    assert "Static checks are not listed. Run prek as usual." in output
    assert "prek run --stage manual mypy-providers --from-ref main" in output
    assert "migration-round-trip" not in output


@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_no_manual_prek_hint_when_no_hook_is_triggered(mock_files):
    result = CliRunner().invoke(verify, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert "--stage manual" not in " ".join(result.output.split())


@pytest.mark.parametrize(
    ("files", "expanded"),
    [
        (("airflow-core/docs/index.rst",), False),
        (("dev/breeze/src/airflow_breeze/breeze.py",), True),
    ],
)
def test_full_suite_expansion_is_explained(files: tuple[str, ...], expanded: bool):
    with patch(
        "airflow_breeze.commands.verify_commands.get_changed_files_against", autospec=True, return_value=files
    ):
        result = CliRunner().invoke(verify, [], catch_exceptions=False)
        full = CliRunner().invoke(verify, ["--full"], catch_exceptions=False)
    assert result.exit_code == 0
    assert ("CI also runs the full suite" in " ".join(result.output.split())) is expanded
    assert "CI also runs the full suite" not in " ".join(full.output.split())
    assert ("breeze testing core-tests" in " ".join(full.output.split())) is expanded
    assert "breeze testing core-tests" not in " ".join(result.output.split())


@pytest.mark.parametrize(
    ("files", "has_commands"),
    [
        (("README.md",), False),
        (("airflow-core/docs/index.rst",), True),
    ],
)
def test_table_is_shown_only_when_there_are_commands(files: tuple[str, ...], has_commands: bool):
    with patch(
        "airflow_breeze.commands.verify_commands.get_changed_files_against", autospec=True, return_value=files
    ):
        result = CliRunner().invoke(verify, [], catch_exceptions=False)
    assert result.exit_code == 0
    output = " ".join(result.output.split())
    assert ("What to run for this change" in output) is has_commands
    assert ("No tests or docs builds to run locally for the changed files." in output) is not has_commands


@patch("airflow_breeze.commands.verify_commands.run_command", autospec=True)
def test_changed_files_list_renames_like_ci_diff_tree(mock_run):
    mock_run.side_effect = [
        CompletedProcess(args=[], returncode=0, stdout="abc123\n", stderr=""),
        CompletedProcess(args=[], returncode=0, stdout="new.py\nold.py\n", stderr=""),
        CompletedProcess(args=[], returncode=0, stdout="untracked.py\n", stderr=""),
    ]
    assert get_changed_files_against("main") == ("new.py", "old.py", "untracked.py")
    assert mock_run.call_args_list[1].args[0] == ["git", "diff", "--name-only", "--no-renames", "abc123"]


@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/src/airflow/models/dag.py",),
)
def test_long_commands_are_folded_not_truncated(mock_files):
    result = CliRunner().invoke(verify, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert "\u2026" not in result.output
    compact = re.sub(r"[\u2502\s]", "", result.output)
    assert "breezetestingcore-tests--use-xdist--skip-db-tests--no-db-cleanup--backendnone" in compact


FORK = "https://github.com/someone/airflow.git"
APACHE_HTTPS = "https://github.com/apache/airflow.git"
APACHE_SSH = "git@github.com:apache/airflow.git"


@pytest.mark.parametrize(
    ("remotes", "fetched", "expected"),
    [
        pytest.param(
            {"origin": FORK, "upstream": APACHE_HTTPS},
            {"origin", "upstream"},
            "upstream/main",
            id="convention",
        ),
        pytest.param(
            {"origin": FORK, "apache": APACHE_SSH}, {"origin", "apache"}, "apache/main", id="other-name-ssh"
        ),
        pytest.param(
            {"aaa": APACHE_HTTPS, "upstream": APACHE_HTTPS},
            {"aaa", "upstream"},
            "upstream/main",
            id="prefers-upstream",
        ),
        pytest.param(
            {"aaa": APACHE_HTTPS, "upstream": APACHE_HTTPS}, {"aaa"}, "aaa/main", id="upstream-not-fetched"
        ),
        pytest.param(
            {"origin": "https://github.com/apache/airflow-site.git"}, {"origin"}, None, id="other-apache-repo"
        ),
        pytest.param({"origin": FORK}, {"origin"}, None, id="fork-only"),
    ],
)
@patch("airflow_breeze.commands.verify_commands.run_command", autospec=True)
def test_default_base_ref_is_main_on_the_apache_remote(mock_run, remotes, fetched, expected):
    def fake_git(cmd, **kwargs):
        if cmd[1] == "config":
            stdout = "".join(f"remote.{name}.url {url}\n" for name, url in remotes.items())
            return CompletedProcess(args=cmd, returncode=0, stdout=stdout, stderr="")
        exists = cmd[-1].removeprefix("refs/remotes/").removesuffix("/main") in fetched
        return CompletedProcess(args=cmd, returncode=0 if exists else 1, stdout="", stderr="")

    mock_run.side_effect = fake_git
    assert find_default_base_ref() == expected


@pytest.mark.parametrize(
    ("found", "compared_with", "warned"),
    [("upstream/main", "upstream/main", False), (None, "main", True)],
)
def test_verify_compares_with_the_default_base_ref(found, compared_with: str, warned: bool):
    with (
        patch(
            "airflow_breeze.commands.verify_commands.find_default_base_ref", autospec=True, return_value=found
        ),
        patch(
            "airflow_breeze.commands.verify_commands.is_base_possibly_stale",
            autospec=True,
            return_value=False,
        ),
        patch(
            "airflow_breeze.commands.verify_commands.get_changed_files_against",
            autospec=True,
            return_value=("airflow-core/docs/index.rst",),
        ) as mock_files,
    ):
        result = CliRunner().invoke(verify, [], catch_exceptions=False)
    assert result.exit_code == 0
    mock_files.assert_called_once_with(compared_with)
    output = " ".join(ANSI.sub("", result.output).split())
    assert ("No git remote points at apache/airflow" in output) is warned
    assert f"changed file(s) against {compared_with}," in output


def _git(repo, *args: str) -> None:
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "commit.gpgsign=false",
            "-c",
            f"core.hooksPath={os.devnull}",
            *args,
        ],
        cwd=repo,
        check=True,
        capture_output=True,
    )


def _commit(repo, name: str) -> None:
    (repo / name).parent.mkdir(parents=True, exist_ok=True)
    (repo / name).write_text(name)
    _git(repo, "add", name)
    _git(repo, "commit", "-m", name)


@pytest.mark.parametrize(
    ("remote", "update", "base", "expected"),
    [
        pytest.param("upstream", None, "main", False, id="never-updated"),
        pytest.param("upstream", "merge", "main", True, id="merged-newer-than-base"),
        pytest.param("upstream", "merge", "upstream/main", False, id="merged-and-base-fetched"),
        pytest.param("upstream", "rebase", "main", True, id="rebased-newer-than-base"),
        pytest.param("upstream", "rebase", "upstream/main", False, id="rebased-and-base-fetched"),
        pytest.param("origin", "rebase", "main", True, id="rebased-onto-a-fork-remote"),
    ],
)
def test_detects_target_branch_commits_the_base_may_not_have(
    tmp_path, remote: str, update: str | None, base: str, expected: bool
):
    _git(tmp_path, "init", "-b", "main")
    _commit(tmp_path, "B")
    _git(tmp_path, "switch", "-c", "feature")
    _commit(tmp_path, "X")
    _git(tmp_path, "switch", "-c", "newer", "main")
    _commit(tmp_path, "C")
    _git(tmp_path, "update-ref", f"refs/remotes/{remote}/main", "newer")
    _git(tmp_path, "switch", "feature")
    if update:
        _git(tmp_path, update, *(["--no-edit"] if update == "merge" else []), f"{remote}/main")
    with patch("airflow_breeze.commands.verify_commands.AIRFLOW_ROOT_PATH", tmp_path):
        assert is_base_possibly_stale(base.replace("upstream/", f"{remote}/")) is expected


@pytest.mark.parametrize("args", [[], ["--run"], ["--json"]])
@patch("airflow_breeze.commands.verify_commands.run_verification_commands", autospec=True, return_value=[0])
@patch("airflow_breeze.commands.verify_commands.is_base_possibly_stale", autospec=True, return_value=True)
@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_a_possibly_stale_base_is_only_a_warning(mock_files, mock_stale, mock_run, args: list[str]):
    result = CliRunner().invoke(verify, ["--base-ref", "upstream/main", *args], catch_exceptions=False)
    mock_stale.assert_called_once_with("upstream/main")
    assert mock_run.called is ("--run" in args)
    assert result.exit_code == 0
    stream = " ".join(ANSI.sub("", result.stderr if "--json" in args else result.stdout).split())
    assert "upstream/main may be out of date, so the list can include commits that are not yours" in stream
    if "--json" in args:
        assert json.loads(result.stdout)["base_ref"] == "upstream/main"


@patch("airflow_breeze.commands.verify_commands.run_command", autospec=True)
def test_verification_commands_run_in_a_shell_and_carry_on_after_a_failure(mock_run):
    mock_run.side_effect = [CompletedProcess(args=[], returncode=1), CompletedProcess(args=[], returncode=0)]
    assert run_verification_commands(["false", "cd dev && true"]) == [1, 0]
    assert [call.args[0] for call in mock_run.call_args_list] == [
        ["bash", "-c", "false"],
        ["bash", "-c", "cd dev && true"],
    ]
    assert all(call.kwargs == {"check": False, "cwd": AIRFLOW_ROOT_PATH} for call in mock_run.call_args_list)


@patch("airflow_breeze.commands.verify_commands.run_command", autospec=True)
def test_ctrl_c_stops_the_run_and_keeps_the_finished_return_codes(mock_run):
    mock_run.side_effect = [CompletedProcess(args=[], returncode=1), KeyboardInterrupt]
    assert run_verification_commands(["false", "sleep 600", "true"]) == [1]
    assert mock_run.call_count == 2


def _plan(items: list[str], manual_prek_hooks: list[str]) -> dict:
    return {
        "base_ref": "main",
        "default_python_version": "3.10",
        "full_tests_needed": False,
        "changed_files": ["airflow-core/docs/index.rst"],
        "items": [{"kind": "unit", "command": command, "runs_in": "host"} for command in items],
        "manual_prek_hooks": manual_prek_hooks,
    }


@pytest.mark.parametrize(
    ("return_codes", "exit_code", "summary"),
    [
        ([0, 0, 0], 0, "All 3 command(s) passed."),
        ([0, 1, 0], 1, "1 of 3 command(s) failed: second command"),
    ],
)
@patch("airflow_breeze.commands.verify_commands.run_verification_commands", autospec=True)
@patch(
    "airflow_breeze.commands.verify_commands.build_local_verification_plan",
    autospec=True,
    return_value=_plan(["first command", "second command"], ["migration-round-trip"]),
)
@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_verify_runs_the_listed_commands_and_the_manual_prek_hooks(
    mock_files, mock_plan, mock_run, return_codes, exit_code: int, summary: str
):
    mock_run.return_value = return_codes
    result = CliRunner().invoke(verify, ["--run", "--base-ref", "my branch"], catch_exceptions=False)
    mock_run.assert_called_once_with(
        [
            "first command",
            "second command",
            "prek run --stage manual migration-round-trip --from-ref 'my branch'",
        ]
    )
    assert result.exit_code == exit_code
    assert summary in " ".join(ANSI.sub("", result.output).split())


@pytest.mark.parametrize(
    ("items", "args", "message"),
    [
        pytest.param([], ["--run"], "Nothing to run.", id="empty-list"),
        pytest.param(["first command"], [], None, id="list-only"),
        pytest.param(["first command"], ["--json"], None, id="json"),
    ],
)
@patch("airflow_breeze.commands.verify_commands.run_verification_commands", autospec=True)
@patch("airflow_breeze.commands.verify_commands.build_local_verification_plan", autospec=True)
@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_nothing_is_run_for_an_empty_list_or_without_run(
    mock_files, mock_plan, mock_run, items: list[str], args: list[str], message: str | None
):
    mock_plan.return_value = _plan(items, [])
    result = CliRunner().invoke(verify, args, catch_exceptions=False)
    assert result.exit_code == 0
    mock_run.assert_not_called()
    if message:
        assert message in ANSI.sub("", result.output)


@patch("airflow_breeze.commands.verify_commands.run_verification_commands", autospec=True, return_value=[1])
@patch(
    "airflow_breeze.commands.verify_commands.build_local_verification_plan",
    autospec=True,
    return_value=_plan(["first command", "second command", "third command"], []),
)
@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    autospec=True,
    return_value=("airflow-core/docs/index.rst",),
)
def test_an_interrupted_run_lists_what_failed_and_what_did_not_finish(mock_files, mock_plan, mock_run):
    result = CliRunner().invoke(verify, ["--run"], catch_exceptions=False)
    assert result.exit_code == 130
    output = " ".join(ANSI.sub("", result.output).split())
    assert "1 of 3 command(s) failed: first command" in output
    assert "Interrupted. 2 of 3 command(s) did not finish: second command third command" in output


def test_json_cannot_be_combined_with_run():
    result = CliRunner().invoke(verify, ["--json", "--run"])
    assert result.exit_code == 2
    assert "--json only lists the commands and cannot be combined with --run." in result.output
