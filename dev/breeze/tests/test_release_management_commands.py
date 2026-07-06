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

from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_breeze.commands import release_management_commands
from airflow_breeze.commands.release_management_commands import (
    _ensure_default_python_for_reproducible_client,
    _is_initial_provider_release,
    _should_include_provider_in_issue,
    get_package_version_possibly_from_stable_txt,
    get_prs_from_git_log_for_new_provider,
    get_suffix_from_package_in_dist,
    is_package_in_dist,
)
from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION


@pytest.mark.parametrize(
    ("provider_yaml_dict", "expected"),
    [
        ({"versions": ["0.1.0"]}, True),
        ({"versions": ["2.1.0", "2.0.0"]}, False),
        ({"versions": []}, False),
        (None, False),
    ],
)
def test_is_initial_provider_release(provider_yaml_dict: dict | None, expected: bool):
    assert _is_initial_provider_release(provider_yaml_dict) is expected


@pytest.mark.parametrize(
    ("provider_yaml_dict", "prs_for_current_release", "prs_after_exclusions", "expected"),
    [
        ({"versions": ["0.1.0"]}, [12345], [12345], True),
        ({"versions": ["0.1.0"]}, [], [], True),
        ({"versions": ["0.2.0", "0.1.0"]}, [], [], False),
        ({"versions": ["0.1.0"]}, [12345], [], False),
    ],
)
def test_should_include_provider_in_issue(
    provider_yaml_dict: dict | None,
    prs_for_current_release: list[int],
    prs_after_exclusions: list[int],
    expected: bool,
):
    assert (
        _should_include_provider_in_issue(
            provider_yaml_dict=provider_yaml_dict,
            prs_for_current_release=prs_for_current_release,
            prs_after_exclusions=prs_after_exclusions,
        )
        is expected
    )


def test_get_prs_from_git_log_for_new_provider(monkeypatch):
    git_output = (
        # Release-management commits must be skipped (release-process noise).
        "Prepare provider documentation 2026-06-16 (#68642)\n"
        "Bump `clickhouse-connect>=1.3.0` (#68400)\n"
        "docs: clarify when to use custom `handler` (#68345)\n"
        "Prepare providers release 2026-06-08 (#68203)\n"
        # A real provider PR whose title merely starts with "Prepare" must NOT be filtered.
        "Prepare bteq command with subprocess arg list (#67999)\n"
        "Add ClickHouse Provider (#67080)\n"
        # A duplicate PR reference must be collected only once.
        "Add ClickHouse Provider (#67080)\n"
        # A commit without a PR reference must be ignored.
        "Initial scaffolding commit"
    )
    monkeypatch.setattr(
        "airflow_breeze.commands.release_management_commands.get_provider_details",
        lambda provider_id: SimpleNamespace(root_provider_path=Path("/repo/providers/clickhousedb")),
    )
    monkeypatch.setattr(
        "airflow_breeze.commands.release_management_commands.run_command",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=git_output, stderr=""),
    )

    prs = get_prs_from_git_log_for_new_provider("clickhousedb")

    assert prs == [68400, 68345, 67999, 67080]


@pytest.mark.parametrize(
    ("subject", "is_release_commit"),
    [
        ("Prepare provider documentation 2026-06-16 (#68642)", True),
        ("Prepare providers release 2026-06-08 (#68203)", True),
        ("Prepare Providers Release 2026-06-08 (#1)", True),
        ("Prepare provider's documentation (#2)", True),
        ("Prepare ad-hoc RC2 providers release (#3)", True),
        ("Prepare documentation for next release of providers (#4)", True),
        # Real provider PRs that must not be mistaken for release-management commits.
        ("Prepare bteq command with subprocess arg list (#67999)", False),
        ("Add ClickHouse Provider (#67080)", False),
        ("Bump `clickhouse-connect>=1.3.0` (#68400)", False),
    ],
)
def test_release_management_commit_pattern(subject: str, is_release_commit: bool):
    from airflow_breeze.commands.release_management_commands import RELEASE_MANAGEMENT_COMMIT_PATTERN

    assert bool(RELEASE_MANAGEMENT_COMMIT_PATTERN.match(subject)) is is_release_commit


def test_get_prs_from_git_log_for_new_provider_returns_empty_on_git_failure(monkeypatch):
    monkeypatch.setattr(
        "airflow_breeze.commands.release_management_commands.get_provider_details",
        lambda provider_id: SimpleNamespace(root_provider_path=Path("/repo/providers/clickhousedb")),
    )
    monkeypatch.setattr(
        "airflow_breeze.commands.release_management_commands.run_command",
        lambda *args, **kwargs: SimpleNamespace(returncode=128, stdout="", stderr="fatal: bad revision"),
    )

    assert get_prs_from_git_log_for_new_provider("clickhousedb") == []


def _fake_version_info(version: str) -> SimpleNamespace:
    major, minor = (int(part) for part in version.split("."))
    return SimpleNamespace(major=major, minor=minor, micro=0, releaselevel="final", serial=0)


def test_ensure_default_python_for_reproducible_client_passes_on_default(monkeypatch):
    monkeypatch.setattr(
        "airflow_breeze.commands.release_management_commands.sys.version_info",
        _fake_version_info(DEFAULT_PYTHON_MAJOR_MINOR_VERSION),
    )
    # Must not raise / exit when running under the default Python.
    _ensure_default_python_for_reproducible_client()


@pytest.mark.parametrize("wrong_version", ["3.11", "3.13", "3.9"])
def test_ensure_default_python_for_reproducible_client_exits_on_mismatch(monkeypatch, wrong_version):
    assert wrong_version != DEFAULT_PYTHON_MAJOR_MINOR_VERSION
    monkeypatch.setattr(
        "airflow_breeze.commands.release_management_commands.sys.version_info",
        _fake_version_info(wrong_version),
    )
    with pytest.raises(SystemExit) as exc_info:
        _ensure_default_python_for_reproducible_client()
    assert exc_info.value.code == 1


# A short provider id must not be matched as a prefix of a longer one. The classic trap is
# ``git`` vs ``github`` - their distribution filenames share the ``..._git`` / ``...-git`` prefix.
GIT_AND_GITHUB_DIST_FILES = [
    "apache_airflow_providers_github-2.11.3.tar.gz",
    "apache_airflow_providers_github-2.11.3-py3-none-any.whl",
    "apache-airflow-providers-github-2.11.3.tar.gz",
    "apache_airflow_providers_apache_spark-6.1.0rc1-py3-none-any.whl",
]


@pytest.mark.parametrize(
    ("dist_files", "package", "expected"),
    [
        # github files present, git files absent -> git must NOT be reported as in dist
        (GIT_AND_GITHUB_DIST_FILES, "git", False),
        (GIT_AND_GITHUB_DIST_FILES, "github", True),
        # exact match for a regular provider
        (GIT_AND_GITHUB_DIST_FILES, "apache.spark", True),
        # git really present alongside github
        (
            [*GIT_AND_GITHUB_DIST_FILES, "apache_airflow_providers_git-0.4.0-py3-none-any.whl"],
            "git",
            True,
        ),
        ([], "git", False),
    ],
)
def test_is_package_in_dist(dist_files: list[str], package: str, expected: bool):
    assert is_package_in_dist(dist_files, package) is expected


@pytest.mark.parametrize(
    ("dist_files", "package", "expected"),
    [
        # github sdist present, git absent -> must not leak github's suffix to git
        (["apache_airflow_providers_github-2.11.3rc1.tar.gz"], "git", None),
        (["apache_airflow_providers_github-2.11.3rc1.tar.gz"], "github", "rc1"),
        (["apache_airflow_providers_git-0.4.0rc2.tar.gz"], "git", "rc2"),
        (["apache_airflow_providers_apache_spark-6.1.0.tar.gz"], "apache.spark", ""),
    ],
)
def test_get_suffix_from_package_in_dist(dist_files: list[str], package: str, expected: str | None):
    assert get_suffix_from_package_in_dist(dist_files, package) == expected


@pytest.mark.parametrize(
    ("stable_txt_content", "expected_version"),
    [
        # No stable.txt staged (docs not built for this ref) -> None, not an error
        (None, None),
        ("0.1.0\n", "0.1.0"),
    ],
)
def test_get_package_version_possibly_from_stable_txt_for_java_sdk(
    tmp_path: Path, monkeypatch, stable_txt_content: str | None, expected_version: str | None
):
    monkeypatch.setattr(release_management_commands, "AIRFLOW_ROOT_PATH", tmp_path)
    if stable_txt_content is not None:
        stable_txt = tmp_path / "generated" / "_build" / "docs" / "java-sdk" / "stable.txt"
        stable_txt.parent.mkdir(parents=True)
        stable_txt.write_text(stable_txt_content)
    assert get_package_version_possibly_from_stable_txt("java-sdk") == expected_version
