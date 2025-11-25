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
import re
from typing import Any

import pytest
from rich.console import Console

from airflow_breeze.global_constants import (
    COMMITTERS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    PROVIDERS_COMPATIBILITY_TESTS_MATRIX,
    GithubEvents,
)
from airflow_breeze.utils.functools_cache import clearable_cache
from airflow_breeze.utils.packages import get_available_packages
from airflow_breeze.utils.selective_checks import (
    ALL_CI_SELECTIVE_TEST_TYPES,
    ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
    SelectiveChecks,
)

ANSI_COLORS_MATCHER = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")


ALL_DOCS_SELECTED_FOR_BUILD = ""
ALL_PROVIDERS_AFFECTED = ""
LIST_OF_ALL_PROVIDER_TESTS = " ".join(
    f"Providers[{provider}]" for provider in get_available_packages(include_not_ready=True)
)


# commit that is neutral - allows to keep pyproject.toml-changing PRS neutral for unit tests
NEUTRAL_COMMIT = "938f0c1f3cc4cbe867123ee8aa9f290f9f18100a"

# for is_legacy_ui_api_labeled tests
LEGACY_UI_LABEL = "legacy ui"
LEGACY_API_LABEL = "legacy api"


def escape_ansi_colors(line):
    return ANSI_COLORS_MATCHER.sub("", line)


@clearable_cache
def get_rich_console() -> Console:
    return Console(color_system="truecolor", force_terminal=True)


def print_in_color(s: Any = ""):
    get_rich_console().print(s)


def assert_outputs_are_printed(expected_outputs: dict[str, str], stderr: str):
    escaped_stderr = escape_ansi_colors(stderr)
    received_output_as_dict = dict(line.split("=", 1) for line in escaped_stderr.splitlines() if "=" in line)
    for expected_key, expected_value in expected_outputs.items():
        if expected_value is None:
            if expected_key in received_output_as_dict:
                print_in_color(f"\n[red]ERROR: The '{expected_key}' should not be present in:[/]")
                print_in_color(received_output_as_dict)
                print_in_color("\n")
                assert expected_key is not None

        else:
            received_value = received_output_as_dict.get(expected_key)
            if received_value != expected_value:
                if received_value is not None:
                    print_in_color(f"\n[red]ERROR: The key '{expected_key}' has unexpected value:")
                    print(received_value)
                    print_in_color("Expected value:\n")
                    print(expected_value)
                    print_in_color("\nOutput received:")
                    print_in_color(received_output_as_dict)
                    print_in_color()
                    assert received_value == expected_value, f"Correct value for {expected_key!r}"
                else:
                    print_in_color(
                        f"\n[red]ERROR: The key '{expected_key}' missing but "
                        f"it is expected. Expected value:"
                    )
                    print_in_color(expected_value)
                    print_in_color("\nOutput received:")
                    print(received_output_as_dict)
                    print_in_color()
                    print(received_output_as_dict)
                    print_in_color()
                    assert received_value is not None


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        (
            pytest.param(
                ("INTHEWILD.md",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": None,
                    "providers-test-types-list-as-string": None,
                    "individual-providers-test-types-list-as-string": None,
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                },
                id="No tests on simple change",
            )
        ),
        (
            pytest.param(
                ("pyproject.toml",),
                {
                    "ci-image-build": "true",
                },
                id="CI image build and when pyproject.toml change",
            )
        ),
        (
            pytest.param(
                ("tests/api/file.py",),
                {
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": "API Always",
                    "providers-test-types-list-as-string": "",
                    "individual-providers-test-types-list-as-string": "",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only API tests should run (no provider tests) and no DOCs build when only test API files changed",
            )
        ),
        (
            pytest.param(
                ("airflow/operators/file.py",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": "Always Operators",
                    "providers-test-types-list-as-string": "",
                    "individual-providers-test-types-list-as-string": "",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only Operator tests and DOCS should run",
            )
        ),
        (
            pytest.param(
                ("airflow/serialization/python.py",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": "Always Serialization",
                    "providers-test-types-list-as-string": "",
                    "individual-providers-test-types-list-as-string": "",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only Serialization tests",
            )
        ),
        (
            pytest.param(
                ("docs/file.rst",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": None,
                    "providers-test-types-list-as-string": None,
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                },
                id="Only docs builds should run - no tests needed",
            )
        ),
        (
            pytest.param(
                (
                    "INTHEWILD.md",
                    "chart/aaaa.txt",
                    "foo/other.py",
                ),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-amazon-tests": "false",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": "Always",
                    "providers-test-types-list-as-string": "",
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                },
                id="Docs should run even if unimportant files were added and prod image "
                "should be build for chart changes",
            )
        ),
        (
            pytest.param(
                ("generated/provider_dependencies.json",),
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": "3.10 3.11 3.12",
                    "python-versions": "['3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": "3.10 3.11 3.12",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "true",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run - including all providers and upgrading to "
                "newer requirements as pyproject.toml changed and all Python versions",
            )
        ),
        (
            pytest.param(
                ("generated/provider_dependencies.json",),
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": "3.10 3.11 3.12",
                    "python-versions": "['3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": "3.10 3.11 3.12",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "true",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run and upgrading to newer requirements as dependencies change",
            )
        ),
        (
            pytest.param(
                ("tests/utils/test_cli_util.py",),
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="All tests should be run when tests/utils/ change",
            )
        ),
        (
            pytest.param(
                ("tests_common/__init__.py",),
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "testable-core-integrations": "['kerberos']",
                    "testable-providers-integrations": "['cassandra', 'drill', 'kafka', 'mongo', 'pinot', 'qdrant', 'redis', 'trino', 'ydb']",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="All tests should be run when tests_common/ change",
            )
        ),
        (
            pytest.param(
                ("airflow/ui/src/index.tsx",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "ci-image-build": "false",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "full-tests-needed": "false",
                    "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                    "run-ui-tests": "true",
                    "only-new-ui-files": "true",
                },
                id="Run only ui tests for PR with new UI only changes.",
            )
        ),
    ],
)
def test_expected_output_pull_request_main(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, commit_ref, expected_outputs",
    [
        (
            pytest.param(
                ("pyproject.toml",),
                "2bc8e175b3a4cc84fe33e687f1a00d2a49563090",
                {
                    "full-tests-needed": "false",
                    "all-versions": "false",
                },
                id="No full tests needed / all versions when pyproject.toml changes in insignificant way",
            )
        ),
        (
            pytest.param(
                ("pyproject.toml",),
                "c381fdaff42bbda480eee70fb15c5b26a2a3a77d",
                {
                    "full-tests-needed": "true",
                    "all-versions": "true",
                },
                id="Full tests needed / all versions  when build-system changes in pyproject.toml",
            )
        ),
    ],
)
def test_full_test_needed_when_pyproject_toml_changes(
    files: tuple[str, ...], commit_ref: str, expected_outputs: dict[str, str]
):
    stderr = SelectiveChecks(
        files=files,
        github_event=GithubEvents.PULL_REQUEST,
        commit_ref=commit_ref,
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


def test_hatch_build_py_changes():
    stderr = SelectiveChecks(
        files=("hatch_build.py",),
        github_event=GithubEvents.PULL_REQUEST,
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "full-tests-needed": "true",
            "all-versions": "true",
        },
        str(stderr),
    )


def test_excluded_providers():
    stderr = SelectiveChecks(
        files=(),
        github_event=GithubEvents.PULL_REQUEST,
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "excluded-providers-as-string": json.dumps({"3.12": ["apache.beam", "papermill"]}),
        },
        str(stderr),
    )


@pytest.mark.parametrize(
    "files, expected_outputs",
    [
        (
            pytest.param(
                ("scripts/ci/pre_commit/file.sh",),
                {
                    "full-tests-needed": "false",
                },
                id="No full tests needed when pre-commit scripts change",
            )
        ),
        (
            pytest.param(
                ("scripts/docker-compose/test.yml",),
                {
                    "full-tests-needed": "true",
                },
                id="Full tests needed when docker-compose changes",
            )
        ),
        (
            pytest.param(
                ("scripts/ci/kubernetes/some_file.txt",),
                {
                    "full-tests-needed": "true",
                },
                id="Full tests needed when ci/kubernetes changes",
            )
        ),
        (
            pytest.param(
                ("scripts/in_container/script.sh",),
                {
                    "full-tests-needed": "true",
                },
                id="Full tests needed when in_container script changes",
            )
        ),
    ],
)
def test_full_test_needed_when_scripts_changes(files: tuple[str, ...], expected_outputs: dict[str, str]):
    stderr = SelectiveChecks(
        files=files,
        github_event=GithubEvents.PULL_REQUEST,
        commit_ref=NEUTRAL_COMMIT,
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, pr_labels, default_branch, expected_outputs,",
    [
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed", "all versions"),
                "main",
                {
                    "all-versions": "true",
                    "all-python-versions": "['3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": "3.10 3.11 3.12",
                    "mysql-versions": "['8.0', '8.4']",
                    "postgres-versions": "['13', '14', '15', '16', '17']",
                    "python-versions": "['3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": "3.10 3.11 3.12",
                    "kubernetes-versions": "['v1.28.15', 'v1.29.12', 'v1.30.8', 'v1.31.4', 'v1.32.0']",
                    "kubernetes-versions-list-as-string": "v1.28.15 v1.29.12 v1.30.8 v1.31.4 v1.32.0",
                    "kubernetes-combos-list-as-string": "3.10-v1.28.15 3.11-v1.29.12 3.12-v1.30.8 3.10-v1.31.4 3.11-v1.32.0",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "test-groups": "['core', 'providers']",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run including all providers when full tests are needed, "
                "and all versions are required.",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed", "default versions only"),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['13']",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "kubernetes-versions": "['v1.28.15']",
                    "kubernetes-versions-list-as-string": "v1.28.15",
                    "kubernetes-combos-list-as-string": "3.10-v1.28.15",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "test-groups": "['core', 'providers']",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run including all providers when full tests are needed "
                "but with single python and kubernetes if `default versions only` label is set",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed",),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['13']",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "kubernetes-versions": "['v1.28.15']",
                    "kubernetes-versions-list-as-string": "v1.28.15",
                    "kubernetes-combos-list-as-string": "3.10-v1.28.15",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "test-groups": "['core', 'providers']",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run including all providers when full tests are needed "
                "but with single python and kubernetes if no version label is set",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed", "latest versions only"),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.12']",
                    "all-python-versions-list-as-string": "3.12",
                    "all-versions": "false",
                    "default-python-version": "3.12",
                    "mysql-versions": "['8.4']",
                    "postgres-versions": "['17']",
                    "python-versions": "['3.12']",
                    "python-versions-list-as-string": "3.12",
                    "kubernetes-versions": "['v1.32.0']",
                    "kubernetes-versions-list-as-string": "v1.32.0",
                    "kubernetes-combos-list-as-string": "3.12-v1.32.0",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "test-groups": "['core', 'providers']",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run including all providers when full tests are needed "
                "but with single python and kubernetes if `latest versions only` label is set",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                (
                    "another label",
                    "full tests needed",
                ),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "all-versions": "false",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "kubernetes-versions": "['v1.28.15']",
                    "kubernetes-versions-list-as-string": "v1.28.15",
                    "kubernetes-combos-list-as-string": "3.10-v1.28.15",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "test-groups": "['core', 'providers']",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run including full providers when full "
                "tests are needed even with different label set as well",
            )
        ),
        (
            pytest.param(
                (),
                ("full tests needed",),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "all-versions": "false",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "kubernetes-versions": "['v1.28.15']",
                    "kubernetes-versions-list-as-string": "v1.28.15",
                    "kubernetes-combos-list-as-string": "3.10-v1.28.15",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "test-groups": "['core', 'providers']",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "individual-providers-test-types-list-as-string": LIST_OF_ALL_PROVIDER_TESTS,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run including full providers when "
                "full tests are needed even if no files are changed",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md", "tests/providers/asana.py"),
                ("full tests needed",),
                "v2-7-stable",
                {
                    "all-python-versions": "['3.10']",
                    "all-python-versions-list-as-string": "3.10",
                    "python-versions": "['3.10']",
                    "python-versions-list-as-string": "3.10",
                    "all-versions": "false",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "true",
                    "test-groups": "['core']",
                    "docs-build": "true",
                    "docs-list-as-string": "apache-airflow docker-stack",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "check-airflow-provider-compatibility,check-extra-packages-references,check-provider-yaml-valid,identity,kubeconform,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,validate-operators-init",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-string": "API Always CLI Core Operators Other "
                    "Serialization WWW",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-docs', 'mypy-dev']",
                },
                id="Everything should run except Providers and lint pre-commit "
                "when full tests are needed for non-main branch",
            )
        ),
    ],
)
def test_expected_output_full_tests_needed(
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    default_branch: str,
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=pr_labels,
        default_branch=default_branch,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "skip-providers-tests": "true",
                "test-groups": "[]",
                "docs-build": "false",
                "docs-list-as-string": None,
                "full-tests-needed": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            (
                "chart/aaaa.txt",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "needs-helm-tests": "false",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "test-groups": "['core']",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": "Always",
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="No Helm tests, No providers no lint charts, should run if "
            "only chart/providers changed in non-main but PROD image should be built",
        ),
        pytest.param(
            (
                "airflow/cli/test.py",
                "chart/aaaa.txt",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "test-groups": "['core']",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": "Always CLI",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Only CLI tests and Kubernetes tests should run if cli/chart files changed in non-main branch",
        ),
        pytest.param(
            (
                "airflow/file.py",
                "tests/providers/google/file.py",
            ),
            {
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "test-groups": "['core']",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": "API Always CLI Core Operators Other Serialization WWW",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="All tests except Providers and helm lint pre-commit "
            "should run if core file changed in non-main branch",
        ),
    ],
)
def test_expected_output_pull_request_v2_7(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="v2-7-stable",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, pr_labels, default_branch, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "main",
            {
                "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": "3.10 3.11 3.12",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                "upgrade-to-newer-dependencies": "true",
                "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
            },
            id="All tests run on push even if unimportant file changed",
        ),
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "v2-3-stable",
            {
                "all-python-versions": "['3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": "3.10 3.11 3.12",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": "check-airflow-provider-compatibility,check-extra-packages-references,check-provider-yaml-valid,identity,kubeconform,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,validate-operators-init",
                "docs-list-as-string": "apache-airflow docker-stack",
                "upgrade-to-newer-dependencies": "true",
                "core-test-types-list-as-string": "API Always CLI Core Operators Other Serialization WWW",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-docs', 'mypy-dev']",
            },
            id="All tests except Providers and Helm run on push"
            " even if unimportant file changed in non-main branch",
        ),
        pytest.param(
            ("airflow/api.py",),
            (),
            "main",
            {
                "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": "3.10 3.11 3.12",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "upgrade-to-newer-dependencies": "true",
                "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
            },
            id="All tests run on push if core file changed",
        ),
    ],
)
def test_expected_output_push(
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    default_branch: str,
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PUSH,
        pr_labels=pr_labels,
        default_branch=default_branch,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "skip-providers-tests": "true",
                "test-groups": "[]",
                "docs-build": "false",
                "docs-list-as-string": None,
                "upgrade-to-newer-dependencies": "false",
                "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,"
                "mypy-airflow,mypy-dev,mypy-docs,mypy-providers,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "core-test-types-list-as-string": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            ("tests/system/any_file.py",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "test-groups": "['core']",
                "docs-build": "true",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": "Always",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Only Always and docs build should run if only system tests changed",
        ),
        pytest.param(
            ("airflow/models/test.py",),
            {
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "test-groups": "['core']",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow",
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Tests for all airflow core types except providers should run if model file changed",
        ),
        pytest.param(
            ("airflow/file.py",),
            {
                "all-python-versions": "['3.10']",
                "all-python-versions-list-as-string": "3.10",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "test-groups": "['core']",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow",
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Tests for all airflow core types except providers should run if "
            "any other than API/WWW/CLI/Operators file changed.",
        ),
    ],
)
def test_expected_output_pull_request_target(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST_TARGET,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "github_event",
    [
        GithubEvents.PUSH,
        GithubEvents.PULL_REQUEST,
        GithubEvents.PULL_REQUEST_TARGET,
        GithubEvents.PULL_REQUEST_WORKFLOW,
        GithubEvents.SCHEDULE,
    ],
)
def test_no_commit_provided_trigger_full_build_for_any_event_type(github_event):
    stderr = SelectiveChecks(
        files=(),
        commit_ref="",
        github_event=github_event,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "all-python-versions": "['3.10', '3.11', '3.12']",
            "all-python-versions-list-as-string": "3.10 3.11 3.12",
            "ci-image-build": "true",
            "prod-image-build": "true",
            "needs-helm-tests": "true",
            "run-tests": "true",
            "docs-build": "true",
            "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
            "upgrade-to-newer-dependencies": (
                "true" if github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE] else "false"
            ),
            "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
            "needs-mypy": "true",
            "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
        },
        str(stderr),
    )


@pytest.mark.parametrize(
    "github_event",
    [
        GithubEvents.PUSH,
        GithubEvents.SCHEDULE,
    ],
)
def test_files_provided_trigger_full_build_for_any_event_type(github_event):
    stderr = SelectiveChecks(
        files=("airflow/ui/src/pages/Run/Details.tsx", "airflow/ui/src/router.tsx"),
        commit_ref="",
        github_event=github_event,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "all-python-versions": "['3.10', '3.11', '3.12']",
            "all-python-versions-list-as-string": "3.10 3.11 3.12",
            "ci-image-build": "true",
            "prod-image-build": "true",
            "needs-helm-tests": "true",
            "run-tests": "true",
            "docs-build": "true",
            "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers",
            "upgrade-to-newer-dependencies": (
                "true" if github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE] else "false"
            ),
            "core-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
            "needs-mypy": "true",
            "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
        },
        str(stderr),
    )


@pytest.mark.parametrize(
    "files, expected_outputs, pr_labels, commit_ref",
    [
        pytest.param(
            ("airflow/models/dag.py",),
            {
                "upgrade-to-newer-dependencies": "false",
            },
            (),
            None,
            id="Regular source changed",
        ),
        pytest.param(
            ("pyproject.toml",),
            {
                "upgrade-to-newer-dependencies": "false",
            },
            (),
            # In this commit only ruff configuration changed
            "2bc8e175b3a4cc84fe33e687f1a00d2a49563090",
            id="pyproject.toml changed but no dependency change",
        ),
        pytest.param(
            ("airflow/providers/microsoft/azure/provider.yaml",),
            {
                "upgrade-to-newer-dependencies": "false",
            },
            (),
            None,
            id="Provider.yaml changed",
        ),
        pytest.param(
            ("generated/provider_dependencies.json",),
            {
                "upgrade-to-newer-dependencies": "true",
            },
            (),
            "None",
            id="Generated provider_dependencies changed",
        ),
        pytest.param(
            ("airflow/models/dag.py",),
            {
                "upgrade-to-newer-dependencies": "true",
            },
            ("upgrade to newer dependencies",),
            None,
            id="Regular source changed",
        ),
    ],
)
def test_upgrade_to_newer_dependencies(
    files: tuple[str, ...],
    expected_outputs: dict[str, str],
    pr_labels: tuple[str, ...],
    commit_ref: str | None,
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=commit_ref,
        github_event=GithubEvents.PULL_REQUEST,
        default_branch="main",
        pr_labels=pr_labels,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("docs/apache-airflow/docs.rst",),
            {
                "docs-list-as-string": "apache-airflow",
            },
            id="Only Airflow docs changed",
        ),
        pytest.param(
            ("docs/conf.py",),
            {
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
            },
            id="Docs conf.py changed",
        ),
        pytest.param(
            ("airflow/test.py",),
            {
                "docs-list-as-string": "apache-airflow",
            },
            id="Core files changed. Apache-Airflow docs should also be built",
        ),
        pytest.param(
            ("docs/docker-stack/test.rst",),
            {"docs-list-as-string": "docker-stack"},
            id="Docker stack files changed. No provider docs to build",
        ),
        pytest.param(
            ("airflow/test.py", "chart/airflow/values.yaml"),
            {
                "docs-list-as-string": "apache-airflow helm-chart",
            },
            id="Core files and helm chart files changed. Apache Airflow and helm chart docs to build",
        ),
        pytest.param(
            ("chart/airflow/values.yaml",),
            {
                "docs-list-as-string": "helm-chart",
            },
            id="Helm chart files changed. No provider, airflow docs to build",
        ),
        pytest.param(
            ("docs/helm-chart/airflow/values.yaml",),
            {
                "docs-list-as-string": "helm-chart",
            },
            id="Docs helm chart files changed. No provider, airflow docs to build",
        ),
    ],
)
def test_docs_filter(files: tuple[str, ...], expected_outputs: dict[str, str]):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs,",
    [
        pytest.param(
            ("helm_tests/random_helm_test.py",),
            {
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
            },
            id="Only helm test files changed",
        )
    ],
)
def test_helm_tests_trigger_ci_build(files: tuple[str, ...], expected_outputs: dict[str, str]):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    (
        "github_event, github_actor, github_repository, pr_labels, "
        "github_context_dict, runs_on_as_json_default, runs_on_as_docs_build, is_self_hosted_runner, "
        "is_airflow_runner, is_amd_runner, is_arm_runner, is_vm_runner, is_k8s_runner, exception"
    ),
    [
        pytest.param(
            GithubEvents.PUSH,
            "user",
            "apache/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            # "true",
            # "true",
            "true",
            "false",
            # TODO: revert it when we fix self-hosted runners
            "false",
            # "true",
            "false",
            False,
            id="Push event",
        ),
        pytest.param(
            GithubEvents.PUSH,
            "user",
            "private/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Push event for private repo",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            "user",
            "apache/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            "user",
            "private/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request private repo",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            COMMITTERS[0],
            "apache/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request committer",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            COMMITTERS[0],
            "apache/airflow",
            (),
            dict(event=dict(pull_request=dict(user=dict(login="user")))),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request committer pr non-committer",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            COMMITTERS[0],
            "private/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request private repo committer",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST_TARGET,
            "user",
            "apache/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request target",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST_TARGET,
            "user",
            "private/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request target private repo",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST_TARGET,
            COMMITTERS[0],
            "apache/airflow",
            [],
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request target committer",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            COMMITTERS[0],
            "apache/airflow",
            (),
            dict(event=dict(pull_request=dict(user=dict(login="user")))),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request target committer pr non-committer",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST_TARGET,
            COMMITTERS[0],
            "private/airflow",
            (),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request targe private repo committer",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            "user",
            "apache/airflow",
            ("use self-hosted runners",),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            True,
            id="Pull request by non committer with 'use self-hosted runners' label.",
        ),
        pytest.param(
            GithubEvents.PULL_REQUEST,
            COMMITTERS[0],
            "apache/airflow",
            ("use public runners",),
            dict(),
            '["ubuntu-22.04"]',
            '["ubuntu-22.04"]',
            "false",
            "false",
            "true",
            "false",
            "false",
            "false",
            False,
            id="Pull request by committer with 'use public runners' label.",
        ),
    ],
)
def test_runs_on(
    github_event: GithubEvents,
    github_actor: str,
    github_repository: str,
    pr_labels: tuple[str, ...],
    github_context_dict: dict[str, Any],
    runs_on_as_json_default: str,
    runs_on_as_docs_build: str,
    is_self_hosted_runner: str,
    is_airflow_runner: str,
    is_amd_runner: str,
    is_arm_runner: str,
    is_vm_runner: str,
    is_k8s_runner: str,
    exception: bool,
):
    def get_output() -> str:
        return str(
            SelectiveChecks(
                files=(),
                commit_ref="",
                github_repository=github_repository,
                github_event=github_event,
                github_actor=github_actor,
                github_context_dict=github_context_dict,
                pr_labels=pr_labels,
                default_branch="main",
            )
        )

    if exception:
        with pytest.raises(SystemExit):
            get_output()
    else:
        stderr = get_output()
        assert_outputs_are_printed({"runs-on-as-json-default": runs_on_as_json_default}, str(stderr))
        assert_outputs_are_printed({"runs-on-as-json-docs-build": runs_on_as_docs_build}, str(stderr))
        assert_outputs_are_printed({"is-self-hosted-runner": is_self_hosted_runner}, str(stderr))
        assert_outputs_are_printed({"is-airflow-runner": is_airflow_runner}, str(stderr))
        assert_outputs_are_printed({"is-amd-runner": is_amd_runner}, str(stderr))
        assert_outputs_are_printed({"is-arm-runner": is_arm_runner}, str(stderr))
        assert_outputs_are_printed({"is-vm-runner": is_vm_runner}, str(stderr))
        assert_outputs_are_printed({"is-k8s-runner": is_k8s_runner}, str(stderr))


@pytest.mark.parametrize(
    "files, has_migrations",
    [
        pytest.param(
            ("airflow/test.py",),
            False,
            id="No migrations",
        ),
        pytest.param(
            ("airflow/migrations/test_sql", "airflow/test.py"),
            True,
            id="With migrations",
        ),
    ],
)
def test_has_migrations(files: tuple[str, ...], has_migrations: bool):
    stderr = str(
        SelectiveChecks(
            files=files,
            commit_ref=NEUTRAL_COMMIT,
            github_event=GithubEvents.PULL_REQUEST,
            default_branch="main",
        )
    )
    assert_outputs_are_printed({"has-migrations": str(has_migrations).lower()}, str(stderr))


@pytest.mark.parametrize(
    "labels, expected_outputs,",
    [
        pytest.param(
            (),
            {
                "providers-compatibility-tests-matrix": json.dumps(
                    [
                        check
                        for check in PROVIDERS_COMPATIBILITY_TESTS_MATRIX
                        if check["python-version"] == DEFAULT_PYTHON_MAJOR_MINOR_VERSION
                    ]
                ),
            },
            id="Regular tests",
        ),
        pytest.param(
            ("all versions",),
            {"providers-compatibility-tests-matrix": json.dumps(PROVIDERS_COMPATIBILITY_TESTS_MATRIX)},
            id="full tests",
        ),
    ],
)
def test_provider_compatibility_checks(labels: tuple[str, ...], expected_outputs: dict[str, str]):
    stderr = SelectiveChecks(
        files=(),
        commit_ref=NEUTRAL_COMMIT,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=labels,
        default_branch="main",
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs, default_branch, pr_labels",
    [
        pytest.param(
            ("README.md",),
            {
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            "main",
            (),
            id="No mypy checks on non-python files",
        ),
        pytest.param(
            ("airflow/cli/file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            "main",
            (),
            id="Airflow mypy checks on airflow regular files",
        ),
        pytest.param(
            ("airflow/models/file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            "main",
            (),
            id="Airflow mypy checks on airflow files with model changes.",
        ),
        pytest.param(
            ("airflow/providers/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            "main",
            (),
            id="Airflow mypy checks on provider files",
        ),
        pytest.param(
            ("docs/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-docs']",
            },
            "main",
            (),
            id="Doc checks on doc files",
        ),
        pytest.param(
            ("dev/a_package/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
            },
            "main",
            (),
            id="All mypy checks on def files changed (full tests needed are implicit)",
        ),
        pytest.param(
            ("readme.md",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev']",
            },
            "main",
            ("full tests needed",),
            id="All mypy checks on full tests needed",
        ),
    ],
)
def test_mypy_matches(
    files: tuple[str, ...], expected_outputs: dict[str, str], default_branch: str, pr_labels: tuple[str, ...]
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        default_branch=default_branch,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=pr_labels,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, expected_outputs, github_actor, pr_labels",
    [
        pytest.param(
            ("README.md",),
            {
                "is-committer-build": "false",
                "runs-on-as-json-default": '["ubuntu-22.04"]',
            },
            "",
            (),
            id="Regular pr",
        ),
        pytest.param(
            ("README.md",),
            {
                "is-committer-build": "true",
                "runs-on-as-json-default": '["ubuntu-22.04"]',
            },
            "potiuk",
            (),
            id="Committer regular PR",
        ),
        pytest.param(
            ("README.md",),
            {
                "is-committer-build": "false",
                "runs-on-as-json-default": '["ubuntu-22.04"]',
            },
            "potiuk",
            ("non committer build",),
            id="Committer regular PR - forcing non-committer build",
        ),
        pytest.param(
            ("README.md",),
            {
                "docker-cache": "disabled",
                "disable-airflow-repo-cache": "true",
            },
            "potiuk",
            ("disable image cache",),
            id="Disabled cache",
        ),
        pytest.param(
            ("README.md",),
            {
                "docker-cache": "registry",
                "disable-airflow-repo-cache": "false",
            },
            "potiuk",
            (),
            id="Standard cache",
        ),
    ],
)
def test_pr_labels(
    files: tuple[str, ...], expected_outputs: dict[str, str], github_actor: str, pr_labels: tuple[str, ...]
):
    stderr = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        default_branch="main",
        github_actor=github_actor,
        github_event=GithubEvents.PULL_REQUEST,
        pr_labels=pr_labels,
    )
    assert_outputs_are_printed(expected_outputs, str(stderr))


@pytest.mark.parametrize(
    "files, pr_labels, github_event, expected_label",
    [
        pytest.param(
            ("airflow/www/package.json",),
            (),
            GithubEvents.PULL_REQUEST,
            LEGACY_UI_LABEL,
            id="Legacy UI file without label",
        ),
        pytest.param(
            ("airflow/api_connexion/endpoints/health_endpoint.py", "airflow/www/package.json"),
            (LEGACY_UI_LABEL,),
            GithubEvents.PULL_REQUEST,
            LEGACY_API_LABEL,
            id="Legacy API and UI files without one of the labels API missing",
        ),
        pytest.param(
            ("airflow/api_connexion/endpoints/health_endpoint.py",),
            (),
            GithubEvents.PULL_REQUEST,
            LEGACY_API_LABEL,
            id="Legacy API file without label",
        ),
        pytest.param(
            ("airflow/api_connexion/endpoints/health_endpoint.py", "airflow/www/package.json"),
            (LEGACY_API_LABEL,),
            GithubEvents.PULL_REQUEST,
            LEGACY_UI_LABEL,
            id="Legacy API and UI files without one of the labels UI missing",
        ),
    ],
)
def test_is_legacy_ui_api_labeled_should_fail(
    files: tuple[str, ...], pr_labels: tuple[str, ...], github_event: GithubEvents, expected_label: str
):
    try:
        stdout = SelectiveChecks(
            files=files,
            commit_ref=NEUTRAL_COMMIT,
            github_event=github_event,
            pr_labels=pr_labels,
            default_branch="main",
        )
    except SystemExit:
        assert (
            f"[error]Please ask maintainer to assign the '{expected_label}' label to the PR in order to continue"
            in escape_ansi_colors(str(stdout))
        )


@pytest.mark.parametrize(
    "files, pr_labels, github_event, expected_label",
    [
        pytest.param(
            ("airflow/www/package.json",),
            (LEGACY_UI_LABEL,),
            GithubEvents.PULL_REQUEST,
            LEGACY_UI_LABEL,
            id="Legacy UI file with label",
        ),
        pytest.param(
            ("airflow/api_connexion/endpoints/health_endpoint.py",),
            (LEGACY_API_LABEL,),
            GithubEvents.PULL_REQUEST,
            LEGACY_API_LABEL,
            id="Legacy API file with label",
        ),
        pytest.param(
            ("airflow/api_connexion/endpoints/health_endpoint.py",),
            (),
            GithubEvents.SCHEDULE,
            LEGACY_API_LABEL,
            id="Legacy API file in canary schedule",
        ),
        pytest.param(
            ("airflow/www/package.json",),
            (LEGACY_UI_LABEL,),
            GithubEvents.SCHEDULE,
            LEGACY_API_LABEL,
            id="Legacy UI file in canary schedule",
        ),
        pytest.param(
            ("airflow/api_connexion/endpoints/health_endpoint.py",),
            (LEGACY_API_LABEL,),
            GithubEvents.PUSH,
            LEGACY_API_LABEL,
            id="Legacy API file in canary push",
        ),
        pytest.param(
            ("airflow/www/package.json",),
            (LEGACY_UI_LABEL,),
            GithubEvents.PUSH,
            LEGACY_UI_LABEL,
            id="Legacy UI file in canary push",
        ),
    ],
)
def test_is_legacy_ui_api_labeled_should_not_fail(
    files: tuple[str, ...], pr_labels: tuple[str, ...], github_event: GithubEvents, expected_label: str
):
    stdout = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        github_event=github_event,
        pr_labels=pr_labels,
        default_branch="main",
    )
    assert (
        f"[error]Please ask maintainer to assign the '{expected_label}' label to the PR in order to continue"
        not in escape_ansi_colors(str(stdout))
    )
