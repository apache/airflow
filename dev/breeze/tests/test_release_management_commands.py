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

from types import SimpleNamespace

import pytest

from airflow_breeze.commands.release_management_commands import (
    _ensure_default_python_for_reproducible_client,
    _is_initial_provider_release,
    _should_include_provider_in_issue,
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
