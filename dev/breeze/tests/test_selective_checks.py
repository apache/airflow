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
from functools import cache
from typing import Any

import pytest
from rich.console import Console

from airflow_breeze.global_constants import (
    BASE_PROVIDERS_COMPATIBILITY_CHECKS,
    COMMITTERS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    GithubEvents,
)
from airflow_breeze.utils.packages import get_available_packages
from airflow_breeze.utils.selective_checks import (
    ALL_CI_SELECTIVE_TEST_TYPES,
    ALL_CI_SELECTIVE_TEST_TYPES_WITHOUT_PROVIDERS,
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


def escape_ansi_colors(line):
    return ANSI_COLORS_MATCHER.sub("", line)


@cache
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
                    print(
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
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": None,
                    "providers-test-types-list-as-string": None,
                    "separate-test-types-list-as-string": None,
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
                ("airflow/api/file.py",),
                {
                    "affected-providers-list-as-string": "common.compat fab",
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "API Always Providers[common.compat,fab]",
                    "providers-test-types-list-as-string": "Providers[common.compat,fab]",
                    "separate-test-types-list-as-string": "API Always Providers[common.compat] Providers[fab]",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only API tests and DOCS and common.compat, FAB providers should run",
            )
        ),
        (
            pytest.param(
                ("airflow/api_internal/file.py",),
                {
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "API Always",
                    "separate-test-types-list-as-string": "API Always",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only API tests and DOCS should run (no provider tests) when only internal api changed",
            )
        ),
        (
            pytest.param(
                ("airflow/api_fastapi/file.py",),
                {
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "API Always",
                    "separate-test-types-list-as-string": "API Always",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only API tests and DOCS should run (no provider tests) when only ui api changed",
            )
        ),
        (
            pytest.param(
                ("tests/api/file.py",),
                {
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "API Always",
                    "separate-test-types-list-as-string": "API Always",
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
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always Operators",
                    "providers-test-types-list-as-string": "",
                    "separate-test-types-list-as-string": "Always Operators",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only Operator tests and DOCS should run",
            )
        ),
        (
            pytest.param(
                ("airflow/operators/python.py",),
                {
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always BranchExternalPython BranchPythonVenv "
                    "ExternalPython Operators PythonVenv",
                    "providers-test-types-list-as-string": "",
                    "separate-test-types-list-as-string": "Always BranchExternalPython BranchPythonVenv "
                    "ExternalPython Operators PythonVenv",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only Python tests",
            )
        ),
        (
            pytest.param(
                ("airflow/serialization/python.py",),
                {
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always Serialization",
                    "providers-test-types-list-as-string": "",
                    "separate-test-types-list-as-string": "Always Serialization",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Only Serialization tests",
            )
        ),
        (
            pytest.param(
                (
                    "airflow/api/file.py",
                    "providers/tests/postgres/file.py",
                ),
                {
                    "affected-providers-list-as-string": "amazon common.compat common.sql fab google openlineage "
                    "pgvector postgres",
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                    "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "API Always Providers[amazon] "
                    "Providers[common.compat,common.sql,fab,openlineage,pgvector,postgres] Providers[google]",
                    "providers-test-types-list-as-string": "Providers[amazon] "
                    "Providers[common.compat,common.sql,fab,openlineage,pgvector,postgres] Providers[google]",
                    "separate-test-types-list-as-string": "API Always Providers[amazon] Providers[common.compat] Providers[common.sql] "
                    "Providers[fab] Providers[google] Providers[openlineage] Providers[pgvector] "
                    "Providers[postgres]",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers']",
                },
                id="API and providers tests and docs should run",
            )
        ),
        (
            pytest.param(
                ("providers/tests/apache/beam/file.py",),
                {
                    "affected-providers-list-as-string": "apache.beam google",
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": (
                        "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs"
                        ",mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www"
                    ),
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always Providers[apache.beam] Providers[google]",
                    "providers-test-types-list-as-string": "Providers[apache.beam] Providers[google]",
                    "separate-test-types-list-as-string": "Always Providers[apache.beam] Providers[google]",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                },
                id="Selected Providers and docs should run",
            )
        ),
        (
            pytest.param(
                ("docs/file.rst",),
                {
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": None,
                    "providers-test-types-list-as-string": None,
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                },
                id="Only docs builds should run - no tests needed",
            )
        ),
        (
            pytest.param(
                ("task_sdk/src/airflow/sdk/random.py",),
                {
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-api-tests": "false",
                    "needs-helm-tests": "false",
                    "run-kubernetes-tests": "false",
                    "run-tests": "true",
                    "run-task-sdk-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "false",
                    "skip-pre-commits": (
                        "check-provider-yaml-valid,identity,lint-helm-chart"
                        ",mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk"
                        ",ts-compile-format-lint-ui,ts-compile-format-lint-www"
                    ),
                    "skip-provider-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": (
                        "API Always BranchExternalPython BranchPythonVenv CLI Core ExternalPython "
                        "Operators Other PlainAsserts PythonVenv Serialization WWW"
                    ),
                    "providers-test-types-list-as-string": "",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-task-sdk']",
                },
                id="Task SDK source file changed - Task SDK & Core tests should run",
            )
        ),
        (
            pytest.param(
                (
                    "chart/aaaa.txt",
                    "providers/tests/postgres/file.py",
                ),
                {
                    "affected-providers-list-as-string": "amazon common.sql google openlineage "
                    "pgvector postgres",
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                    "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always Providers[amazon] "
                    "Providers[common.sql,openlineage,pgvector,postgres] Providers[google]",
                    "providers-test-types-list-as-string": "Providers[amazon] "
                    "Providers[common.sql,openlineage,pgvector,postgres] Providers[google]",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                },
                id="Helm tests, providers (both upstream and downstream),"
                "kubernetes tests and docs should run",
            )
        ),
        (
            pytest.param(
                (
                    "INTHEWILD.md",
                    "chart/aaaa.txt",
                    "providers/tests/http/file.py",
                ),
                {
                    "affected-providers-list-as-string": "amazon apache.livy "
                    "dbt.cloud dingding discord http",
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                    "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always "
                    "Providers[amazon] Providers[apache.livy,dbt.cloud,dingding,discord,http]",
                    "providers-test-types-list-as-string": "Providers[amazon] Providers[apache.livy,dbt.cloud,dingding,discord,http]",
                    "separate-test-types-list-as-string": "Always Providers[amazon] "
                    "Providers[apache.livy] Providers[dbt.cloud] "
                    "Providers[dingding] Providers[discord] Providers[http]",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                },
                id="Helm tests, http and all relevant providers, kubernetes tests and "
                "docs should run even if unimportant files were added",
            )
        ),
        (
            pytest.param(
                (
                    "INTHEWILD.md",
                    "chart/aaaa.txt",
                    "providers/tests/airbyte/file.py",
                ),
                {
                    "affected-providers-list-as-string": "airbyte",
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                    "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always Providers[airbyte]",
                    "providers-test-types-list-as-string": "Providers[airbyte]",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                },
                id="Helm tests, airbyte providers, kubernetes tests and "
                "docs should run even if unimportant files were added",
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
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": "check-provider-yaml-valid,identity,mypy-airflow,mypy-dev,"
                    "mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                    "run-amazon-tests": "false",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always",
                    "providers-test-types-list-as-string": "",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow']",
                },
                id="Docs should run even if unimportant files were added and prod image "
                "should be build for chart changes",
            )
        ),
        (
            pytest.param(
                ("generated/provider_dependencies.json",),
                {
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                    "python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "true",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
                },
                id="Everything should run - including all providers and upgrading to "
                "newer requirements as pyproject.toml changed and all Python versions",
            )
        ),
        (
            pytest.param(
                ("generated/provider_dependencies.json",),
                {
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                    "python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "true",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
                },
                id="Everything should run and upgrading to newer requirements as dependencies change",
            )
        ),
        pytest.param(
            ("providers/src/airflow/providers/amazon/__init__.py",),
            {
                "affected-providers-list-as-string": "amazon apache.hive cncf.kubernetes "
                "common.compat common.sql exasol ftp google http imap microsoft.azure "
                "mongo mysql openlineage postgres salesforce ssh teradata",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "python-versions": "['3.9']",
                "python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "run-amazon-tests": "true",
                "parallel-test-types-list-as-string": "Always Providers[amazon] "
                "Providers[apache.hive,cncf.kubernetes,common.compat,common.sql,exasol,ftp,http,"
                "imap,microsoft.azure,mongo,mysql,openlineage,postgres,salesforce,ssh,teradata] Providers[google]",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run including amazon tests if amazon provider files changed",
        ),
        pytest.param(
            ("providers/tests/airbyte/__init__.py",),
            {
                "affected-providers-list-as-string": "airbyte",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "python-versions": "['3.9']",
                "python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "false",
                "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "parallel-test-types-list-as-string": "Always Providers[airbyte]",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run without amazon tests if no amazon file changed",
        ),
        pytest.param(
            ("providers/src/airflow/providers/amazon/file.py",),
            {
                "affected-providers-list-as-string": "amazon apache.hive cncf.kubernetes "
                "common.compat common.sql exasol ftp google http imap microsoft.azure "
                "mongo mysql openlineage postgres salesforce ssh teradata",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "python-versions": "['3.9']",
                "python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "parallel-test-types-list-as-string": "Always Providers[amazon] "
                "Providers[apache.hive,cncf.kubernetes,common.compat,common.sql,exasol,ftp,http,"
                "imap,microsoft.azure,mongo,mysql,openlineage,postgres,salesforce,ssh,teradata] Providers[google]",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run including amazon tests if amazon provider files changed",
        ),
        pytest.param(
            (
                "tests/always/test_project_structure.py",
                "providers/tests/common/io/operators/__init__.py",
                "providers/tests/common/io/operators/test_file_transfer.py",
            ),
            {
                "affected-providers-list-as-string": "common.compat common.io openlineage",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "python-versions": "['3.9']",
                "python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "false",
                "run-kubernetes-tests": "false",
                "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "upgrade-to-newer-dependencies": "false",
                "parallel-test-types-list-as-string": "Always Providers[common.compat,common.io,openlineage]",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers']",
            },
            id="Only Always and common providers tests should run when only common.io and tests/always changed",
        ),
        pytest.param(
            ("providers/src/airflow/providers/standard/operators/bash.py",),
            {
                "affected-providers-list-as-string": "standard",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "python-versions": "['3.9']",
                "python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "true",
                "run-kubernetes-tests": "false",
                "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "upgrade-to-newer-dependencies": "false",
                "parallel-test-types-list-as-string": "Always Core Providers[standard] Serialization",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers standard tests and Serialization tests to run when airflow bash.py changed",
        ),
        pytest.param(
            ("providers/tests/standard/operators/bash.py",),
            {
                "affected-providers-list-as-string": None,
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "python-versions": "['3.9']",
                "python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "false",
                "run-kubernetes-tests": "false",
                "skip-pre-commits": "identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "upgrade-to-newer-dependencies": "false",
                "parallel-test-types-list-as-string": "Always Core Providers[standard] Serialization",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Force Core and Serialization tests to run when tests bash changed",
        ),
        (
            pytest.param(
                ("tests/utils/test_cli_util.py",),
                {
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
                },
                id="All tests should be run when tests/utils/ change",
            )
        ),
        (
            pytest.param(
                ("tests_common/__init__.py",),
                {
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
                },
                id="All tests should be run when tests_common/ change",
            )
        ),
        (
            pytest.param(
                ("airflow/ui/src/index.tsx",),
                {
                    "affected-providers-list-as-string": None,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "full-tests-needed": "false",
                    "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-www",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "Always",
                    "providers-test-types-list-as-string": "",
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                    "run-ui-tests": "true",
                    "only-new-ui-files": "true",
                    "testable-integrations": "[]",
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
            "excluded-providers-as-string": json.dumps({"3.9": ["cloudant"], "3.12": ["apache.beam"]}),
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
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-versions": "true",
                    "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                    "mysql-versions": "['8.0', '8.4']",
                    "postgres-versions": "['12', '13', '14', '15', '16', '17']",
                    "python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                    "kubernetes-versions": "['v1.28.13', 'v1.29.8', 'v1.30.4', 'v1.31.0']",
                    "kubernetes-versions-list-as-string": "v1.28.13 v1.29.8 v1.30.4 v1.31.0",
                    "kubernetes-combos-list-as-string": "3.9-v1.28.13 3.10-v1.29.8 3.11-v1.30.4 3.12-v1.31.0",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['12']",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "kubernetes-versions": "['v1.28.13']",
                    "kubernetes-versions-list-as-string": "v1.28.13",
                    "kubernetes-combos-list-as-string": "3.9-v1.28.13",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['12']",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "kubernetes-versions": "['v1.28.13']",
                    "kubernetes-versions-list-as-string": "v1.28.13",
                    "kubernetes-combos-list-as-string": "3.9-v1.28.13",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.12']",
                    "all-python-versions-list-as-string": "3.12",
                    "all-versions": "false",
                    "default-python-version": "3.12",
                    "mysql-versions": "['8.4']",
                    "postgres-versions": "['17']",
                    "python-versions": "['3.12']",
                    "python-versions-list-as-string": "3.12",
                    "kubernetes-versions": "['v1.31.0']",
                    "kubernetes-versions-list-as-string": "v1.31.0",
                    "kubernetes-combos-list-as-string": "3.12-v1.31.0",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "all-versions": "false",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "kubernetes-versions": "['v1.28.13']",
                    "kubernetes-versions-list-as-string": "v1.28.13",
                    "kubernetes-combos-list-as-string": "3.9-v1.28.13",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "all-versions": "false",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "kubernetes-versions": "['v1.28.13']",
                    "kubernetes-versions-list-as-string": "v1.28.13",
                    "kubernetes-combos-list-as-string": "3.9-v1.28.13",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                    "providers-test-types-list-as-string": ALL_PROVIDERS_SELECTIVE_TEST_TYPES,
                    "separate-test-types-list-as-string": "API Always BranchExternalPython BranchPythonVenv "
                    "CLI Core ExternalPython Operators Other PlainAsserts "
                    + LIST_OF_ALL_PROVIDER_TESTS
                    + " PythonVenv Serialization WWW",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
                },
                id="Everything should run including full providers when "
                "full tests are needed even if no files are changed",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md", "providers/tests/asana.py"),
                ("full tests needed",),
                "v2-7-stable",
                {
                    "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": "['3.9']",
                    "all-python-versions-list-as-string": "3.9",
                    "python-versions": "['3.9']",
                    "python-versions-list-as-string": "3.9",
                    "all-versions": "false",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": "apache-airflow docker-stack",
                    "full-tests-needed": "true",
                    "skip-pre-commits": "check-airflow-provider-compatibility,check-extra-packages-references,check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,validate-operators-init",
                    "skip-provider-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "parallel-test-types-list-as-string": "API Always BranchExternalPython "
                    "BranchPythonVenv CLI Core ExternalPython Operators Other PlainAsserts "
                    "PythonVenv Serialization WWW",
                    "separate-test-types-list-as-string": "API Always BranchExternalPython "
                    "BranchPythonVenv CLI Core ExternalPython Operators Other PlainAsserts "
                    "PythonVenv Serialization WWW",
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
                "affected-providers-list-as-string": None,
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "docs-build": "false",
                "docs-list-as-string": None,
                "full-tests-needed": "false",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            (
                "chart/aaaa.txt",
                "providers/tests/google/file.py",
            ),
            {
                "affected-providers-list-as-string": "amazon apache.beam apache.cassandra cncf.kubernetes "
                "common.compat common.sql facebook google hashicorp microsoft.azure microsoft.mssql "
                "mysql openlineage oracle postgres presto salesforce samba sftp ssh trino",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "needs-helm-tests": "false",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": "Always",
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
                "providers/tests/google/file.py",
            ),
            {
                "affected-providers-list-as-string": "amazon apache.beam apache.cassandra "
                "cncf.kubernetes common.compat common.sql facebook google "
                "hashicorp microsoft.azure microsoft.mssql mysql openlineage oracle postgres "
                "presto salesforce samba sftp ssh trino",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": "Always CLI",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Only CLI tests and Kubernetes tests should run if cli/chart files changed in non-main branch",
        ),
        pytest.param(
            (
                "airflow/file.py",
                "providers/tests/google/file.py",
            ),
            {
                "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": "API Always BranchExternalPython BranchPythonVenv "
                "CLI Core ExternalPython Operators Other PlainAsserts PythonVenv Serialization WWW",
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
    "files, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            {
                "affected-providers-list-as-string": None,
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "docs-build": "false",
                "docs-list-as-string": None,
                "upgrade-to-newer-dependencies": "false",
                "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,lint-helm-chart,"
                "mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            ("tests/system/any_file.py",),
            {
                "affected-providers-list-as-string": None,
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": "Always",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Only Always and docs build should run if only system tests changed",
        ),
        pytest.param(
            (
                "airflow/cli/test.py",
                "chart/aaaa.txt",
                "providers/tests/google/file.py",
            ),
            {
                "affected-providers-list-as-string": "amazon apache.beam apache.cassandra "
                "cncf.kubernetes common.compat common.sql "
                "facebook google hashicorp microsoft.azure microsoft.mssql mysql "
                "openlineage oracle postgres presto salesforce samba sftp ssh trino",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow helm-chart amazon apache.beam apache.cassandra "
                "cncf.kubernetes common.compat common.sql facebook google hashicorp microsoft.azure "
                "microsoft.mssql mysql openlineage oracle postgres "
                "presto salesforce samba sftp ssh trino",
                "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "false",
                "parallel-test-types-list-as-string": "Always CLI Providers[amazon] "
                "Providers[apache.beam,apache.cassandra,cncf.kubernetes,common.compat,common.sql,facebook,"
                "hashicorp,microsoft.azure,microsoft.mssql,mysql,openlineage,oracle,postgres,presto,"
                "salesforce,samba,sftp,ssh,trino] Providers[google]",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers']",
            },
            id="CLI tests and Google-related provider tests should run if cli/chart files changed but "
            "prod image should be build too and k8s tests too",
        ),
        pytest.param(
            (
                "airflow/cli/file.py",
                "airflow/operators/file.py",
                "airflow/www/file.py",
                "airflow/api/file.py",
            ),
            {
                "affected-providers-list-as-string": "common.compat fab",
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow common.compat fab",
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "false",
                "parallel-test-types-list-as-string": "API Always CLI Operators Providers[common.compat,fab] WWW",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="No providers tests except common.compat fab should run if only CLI/API/Operators/WWW file changed",
        ),
        pytest.param(
            ("airflow/models/test.py",),
            {
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow",
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES_WITHOUT_PROVIDERS,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow']",
            },
            id="Tests for all airflow core types except providers should run if model file changed",
        ),
        pytest.param(
            ("airflow/file.py",),
            {
                "all-python-versions": "['3.9']",
                "all-python-versions-list-as-string": "3.9",
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow",
                "skip-pre-commits": "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,"
                "ts-compile-format-lint-ui,ts-compile-format-lint-www",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "skip-provider-tests": "true",
                "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES_WITHOUT_PROVIDERS,
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
    "files, pr_labels, default_branch, expected_outputs,",
    [
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "main",
            {
                "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                "upgrade-to-newer-dependencies": "true",
                "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
            },
            id="All tests run on push even if unimportant file changed",
        ),
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "v2-3-stable",
            {
                "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": "check-airflow-provider-compatibility,check-extra-packages-references,check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk,validate-operators-init",
                "docs-list-as-string": "apache-airflow docker-stack",
                "upgrade-to-newer-dependencies": "true",
                "parallel-test-types-list-as-string": "API Always BranchExternalPython BranchPythonVenv "
                "CLI Core ExternalPython Operators Other PlainAsserts PythonVenv Serialization WWW",
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
            },
            id="All tests except Providers and Helm run on push"
            " even if unimportant file changed in non-main branch",
        ),
        pytest.param(
            ("airflow/api.py",),
            (),
            "main",
            {
                "affected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "upgrade-to-newer-dependencies": "true",
                "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
            "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
            "all-python-versions-list-as-string": "3.9 3.10 3.11 3.12",
            "ci-image-build": "true",
            "prod-image-build": "true",
            "needs-helm-tests": "true",
            "run-tests": "true",
            "docs-build": "true",
            "skip-pre-commits": "identity,mypy-airflow,mypy-dev,mypy-docs,mypy-providers,mypy-task-sdk",
            "upgrade-to-newer-dependencies": (
                "true" if github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE] else "false"
            ),
            "parallel-test-types-list-as-string": ALL_CI_SELECTIVE_TEST_TYPES,
            "needs-mypy": "true",
            "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
            ("providers/src/airflow/providers/microsoft/azure/provider.yaml",),
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
            ("docs/apache-airflow-providers-google/docs.rst",),
            {
                "docs-list-as-string": "amazon apache.beam apache.cassandra "
                "cncf.kubernetes common.compat common.sql facebook google hashicorp "
                "microsoft.azure microsoft.mssql mysql openlineage oracle "
                "postgres presto salesforce samba sftp ssh trino",
            },
            id="Google provider docs changed",
        ),
        pytest.param(
            ("providers/src/airflow/providers/common/sql/common_sql_python.py",),
            {
                "docs-list-as-string": "amazon apache.drill apache.druid apache.hive "
                "apache.impala apache.pinot common.sql databricks elasticsearch "
                "exasol google jdbc microsoft.mssql mysql odbc openlineage "
                "oracle pgvector postgres presto slack snowflake sqlite teradata trino vertica ydb",
            },
            id="Common SQL provider package python files changed",
        ),
        pytest.param(
            ("docs/apache-airflow-providers-airbyte/docs.rst",),
            {
                "docs-list-as-string": "airbyte",
            },
            id="Airbyte provider docs changed",
        ),
        pytest.param(
            ("docs/apache-airflow-providers-airbyte/docs.rst", "docs/apache-airflow/docs.rst"),
            {
                "docs-list-as-string": "apache-airflow airbyte",
            },
            id="Airbyte provider and airflow core docs changed",
        ),
        pytest.param(
            (
                "docs/apache-airflow-providers-airbyte/docs.rst",
                "docs/apache-airflow/docs.rst",
                "docs/apache-airflow-providers/docs.rst",
            ),
            {
                "docs-list-as-string": "apache-airflow apache-airflow-providers airbyte",
            },
            id="Airbyte provider and airflow core and common provider docs changed",
        ),
        pytest.param(
            ("docs/apache-airflow/docs.rst",),
            {
                "docs-list-as-string": "apache-airflow",
            },
            id="Only Airflow docs changed",
        ),
        pytest.param(
            ("providers/src/airflow/providers/celery/file.py",),
            {"docs-list-as-string": "celery cncf.kubernetes"},
            id="Celery python files changed",
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
            # TODO: revert it when we fix self-hosted runners
            '["ubuntu-22.04"]',
            '["self-hosted", "asf-runner"]',
            # '["self-hosted", "Linux", "X64"]',
            # TODO: revert it when we fix self-hosted runners
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
                "providers-compatibility-checks": json.dumps(
                    [
                        check
                        for check in BASE_PROVIDERS_COMPATIBILITY_CHECKS
                        if check["python-version"] == DEFAULT_PYTHON_MAJOR_MINOR_VERSION
                    ]
                ),
            },
            id="Regular tests",
        ),
        pytest.param(
            ("all versions",),
            {"providers-compatibility-checks": json.dumps(BASE_PROVIDERS_COMPATIBILITY_CHECKS)},
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
            ("providers/src/airflow/providers/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            "main",
            (),
            id="Airflow mypy checks on provider files",
        ),
        pytest.param(
            ("task_sdk/src/airflow/sdk/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-task-sdk']",
            },
            "main",
            (),
            id="Airflow mypy checks on Task SDK files",
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
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
            },
            "main",
            (),
            id="All mypy checks on def files changed (full tests needed are implicit)",
        ),
        pytest.param(
            ("readme.md",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow', 'mypy-providers', 'mypy-docs', 'mypy-dev', 'mypy-task-sdk']",
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
