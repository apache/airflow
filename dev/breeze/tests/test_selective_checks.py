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

import itertools
import json
import re
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from airflow_breeze.global_constants import (
    ALLOWED_KUBERNETES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_KUBERNETES_VERSION,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    NUMBER_OF_LOW_DEP_SLICES,
    PROVIDERS_COMPATIBILITY_TESTS_MATRIX,
    GithubEvents,
)
from airflow_breeze.utils.functools_cache import clearable_cache
from airflow_breeze.utils.packages import get_available_distributions
from airflow_breeze.utils.selective_checks import (
    ALL_CI_SELECTIVE_TEST_TYPES,
    SelectiveChecks,
    _get_test_list_as_json,
    _split_list,
)

ANSI_COLORS_MATCHER = re.compile(r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]")

ALL_DOCS_SELECTED_FOR_BUILD = ""
ALL_PROVIDERS_AFFECTED = ""

ALL_KUBERNETES_VERSIONS_AS_STRING = " ".join(ALLOWED_KUBERNETES_VERSIONS)
ALL_KUBERNETES_VERSIONS_AS_LIST = "[" + ", ".join([f"'{v}'" for v in ALLOWED_KUBERNETES_VERSIONS]) + "]"
ALL_PYTHON_VERSIONS_AS_STRING = " ".join(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS)
ALL_PYTHON_VERSIONS_AS_LIST = "[" + ", ".join([f"'{v}'" for v in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS]) + "]"

ZIP_PYTHON_AND_KUBERNETES_VERSIONS_AS_STRING = " ".join(
    [
        f"{t[0]}-{t[1]}"
        for t in itertools.zip_longest(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS, ALLOWED_KUBERNETES_VERSIONS)
    ]
)
ZIP_PYTHON_AND_KUBERNETES_VERSIONS_AS_LIST = (
    "["
    + ", ".join(
        [
            f"'{t[0]}-{t[1]}'"
            for t in itertools.zip_longest(ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS, ALLOWED_KUBERNETES_VERSIONS)
        ]
    )
    + "]"
)


ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON = json.dumps(
    [
        {"description": "API...Serialization", "test_types": ALL_CI_SELECTIVE_TEST_TYPES},
    ]
)

ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON = json.dumps(
    [
        {
            "description": "-amazon,googl...standard",
            "test_types": "Providers[-amazon,google,standard] "
            "Providers[amazon] Providers[google] Providers[standard]",
        }
    ]
)

LIST_OF_ALL_PROVIDER_TESTS = [
    f"Providers[{provider}]" for provider in get_available_distributions(include_not_ready=True)
]

LIST_OF_ALL_PROVIDER_TESTS_AS_JSON = json.dumps(
    _get_test_list_as_json(_split_list(sorted(LIST_OF_ALL_PROVIDER_TESTS), 5))
)

ALL_MYPY_CHECKS_ARRAY = [
    "mypy-airflow-core",
    "mypy-providers",
    "mypy-dev",
    "mypy-task-sdk",
    "mypy-devel-common",
    "mypy-airflow-ctl",
]

ALL_MYPY_CHECKS = str(ALL_MYPY_CHECKS_ARRAY)

ALL_MYPY_CHECKS_EXCEPT_PROVIDERS = str(
    [check for check in ALL_MYPY_CHECKS_ARRAY if check != "mypy-providers"]
)

ALL_SKIPPED_COMMITS_ON_NO_CI_IMAGE = (
    "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,"
    "mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)

ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED = (
    "identity,mypy-airflow-core,mypy-airflow-ctl,mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk"
)

ALL_SKIPPED_COMMITS_IF_NO_UI = (
    "identity,mypy-airflow-core,mypy-airflow-ctl,mypy-dev,mypy-devel-common,"
    "mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)
ALL_SKIPPED_COMMITS_IF_NO_HELM_TESTS = (
    "identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,mypy-dev,mypy-devel-common,"
    "mypy-providers,mypy-task-sdk"
)

ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS = (
    "identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,mypy-dev,mypy-devel-common,"
    "mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)

ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS_AND_UI = (
    "check-provider-yaml-valid,identity,mypy-airflow-core,mypy-airflow-ctl,"
    "mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)

ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS = (
    "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,"
    "mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)


ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS_UI_AND_HELM_TESTS = (
    "check-provider-yaml-valid,identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,"
    "mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)

ALL_SKIPPED_COMMITS_IF_NO_CODE_PROVIDERS_AND_HELM_TESTS = (
    "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,"
    "mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk"
)

ALL_SKIPPED_COMMITS_IF_NOT_IMPORTANT_FILES_CHANGED = (
    "check-provider-yaml-valid,flynt,identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,"
    "mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui"
)


All_SKIPPED_COMMITS_IF_NON_MAIN_BRANCH = (
    "check-airflow-provider-compatibility,check-extra-packages-references,check-provider-yaml-valid,"
    "identity,lint-helm-chart,mypy-airflow-core,mypy-airflow-ctl,mypy-dev,"
    "mypy-devel-common,mypy-providers,mypy-task-sdk,validate-operators-init"
)


# commit that is neutral - allows to keep pyproject.toml-changing PRS neutral for unit tests
NEUTRAL_COMMIT = "938f0c1f3cc4cbe867123ee8aa9f290f9f18100a"

# Use me if you are adding test for the changed files that includes caplog
LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL = "log exception"


def escape_ansi_colors(line):
    return ANSI_COLORS_MATCHER.sub("", line)


@clearable_cache
def get_rich_console() -> Console:
    return Console(color_system="truecolor", force_terminal=True)


def print_in_color(s: Any = ""):
    get_rich_console().print(s)


def get_outputs_from_stderr(stderr: str) -> dict[str, str]:
    escaped_stderr = escape_ansi_colors(stderr)
    return dict(line.split("=", 1) for line in escaped_stderr.splitlines() if "=" in line)


def assert_outputs_are_printed(expected_outputs: dict[str, str], stderr: str):
    received_output_as_dict = get_outputs_from_stderr(stderr)
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
                    print(
                        received_value,
                    )
                    print_in_color("Expected value:\n")
                    print(expected_value)
                    print_in_color("\nOutput received:")
                    print_in_color(received_output_as_dict)
                    print_in_color()
                    assert received_value == expected_value, f"Correct value for {expected_key!r}"
                else:
                    print_in_color(
                        f"\n[red]ERROR: The key '{expected_key}' missing but it is expected. Expected value:"
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
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_ON_NO_CI_IMAGE,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": None,
                    "providers-test-types-list-as-strings-in-json": None,
                    "individual-providers-test-types-list-as-strings-in-json": None,
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
                ("airflow-core/src/airflow/api/file.py",),
                {
                    "selected-providers-list-as-string": "",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "individual-providers-test-types-list-as-strings-in-json": LIST_OF_ALL_PROVIDER_TESTS_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="All tests should be run when API file changed",
            )
        ),
        (
            pytest.param(
                ("airflow-core/src/airflow/api_fastapi/file.py",),
                {
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "individual-providers-test-types-list-as-strings-in-json": LIST_OF_ALL_PROVIDER_TESTS_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="All tests should be run when fastapi files change",
            )
        ),
        (
            pytest.param(
                ("airflow-core/tests/unit/api/file.py",),
                {
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "individual-providers-test-types-list-as-strings-in-json": LIST_OF_ALL_PROVIDER_TESTS_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="All tests should run when API test files change",
            )
        ),
        (
            pytest.param(
                ("airflow-core/src/airflow/serialization/python.py",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS_UI_AND_HELM_TESTS,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always...Serialization", "test_types": "Always Serialization"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": None,
                    "individual-providers-test-types-list-as-strings-in-json": None,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-airflow-core']",
                    "skip-providers-tests": "true",
                },
                id="Only Serialization tests",
            )
        ),
        (
            pytest.param(
                (
                    "airflow-core/src/airflow/api/file.py",
                    "providers/postgres/tests/unit/postgres/file.py",
                ),
                {
                    "selected-providers-list-as-string": "",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "individual-providers-test-types-list-as-strings-in-json": LIST_OF_ALL_PROVIDER_TESTS_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="All tests and docs should run on API change",
            )
        ),
        (
            pytest.param(
                ("providers/apache/beam/tests/unit/apache/beam/file.py",),
                {
                    "selected-providers-list-as-string": "apache.beam common.compat google",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam,c...google",
                                "test_types": "Providers[apache.beam,common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "individual-providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam...google",
                                "test_types": "Providers[apache.beam] "
                                "Providers[common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                },
                id="Selected Providers and docs should run",
            )
        ),
        (
            pytest.param(
                ("providers/apache/beam/tests/unit/apache/beam/file.py",),
                {
                    "selected-providers-list-as-string": "apache.beam common.compat google",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam,c...google",
                                "test_types": "Providers[apache.beam,common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "individual-providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam...google",
                                "test_types": "Providers[apache.beam] "
                                "Providers[common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                    "skip-providers-tests": "false",
                },
                id="Selected Providers and docs should run when system tests are modified",
            )
        ),
        (
            pytest.param(
                (
                    "providers/apache/beam/tests/system/apache/beam/file.py",
                    "providers/apache/beam/tests/unit/apache/beam/file.py",
                ),
                {
                    "selected-providers-list-as-string": "apache.beam common.compat google",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam,c...google",
                                "test_types": "Providers[apache.beam,common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "individual-providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam...google",
                                "test_types": "Providers[apache.beam] "
                                "Providers[common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                    "skip-providers-tests": "false",
                },
                id="Selected Providers and docs should run when both system tests and tests are modified",
            )
        ),
        (
            pytest.param(
                (
                    "providers/apache/beam/tests/system/apache/beam/file.py",
                    "providers/apache/beam/tests/unit/apache/beam/file.py",
                ),
                {
                    "selected-providers-list-as-string": "apache.beam common.compat google",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam,c...google",
                                "test_types": "Providers[apache.beam,common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "individual-providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "apache.beam...google",
                                "test_types": "Providers[apache.beam] "
                                "Providers[common.compat] Providers[google]",
                            }
                        ]
                    ),
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers']",
                    "skip-providers-tests": "false",
                },
                id="Selected Providers and docs should run when both system "
                "tests and tests are modified for more than one provider",
            )
        ),
        (
            pytest.param(
                ("docs/file.rst",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_ON_NO_CI_IMAGE,
                    "run-kubernetes-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": None,
                    "providers-test-types-list-as-strings-in-json": None,
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                },
                id="Only docs builds should run - no tests needed",
            )
        ),
        (
            pytest.param(
                ("task-sdk/src/airflow/sdk/random.py",),
                {
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "false",
                    "needs-api-tests": "false",
                    "needs-helm-tests": "false",
                    "run-kubernetes-tests": "false",
                    "run-tests": "true",
                    "run-task-sdk-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS_UI_AND_HELM_TESTS,
                    "skip-providers-tests": "false",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": "['mypy-providers', 'mypy-task-sdk']",
                },
                id="Task SDK source file changed - Task SDK, Core and provider tests should run",
            )
        ),
        (
            pytest.param(
                (
                    "chart/aaaa.txt",
                    "providers/postgres/tests/unit/postgres/file.py",
                ),
                {
                    "selected-providers-list-as-string": "amazon common.sql google "
                    "openlineage pgvector postgres",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI,
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "amazon...google",
                                "test_types": "Providers[amazon] "
                                "Providers[common.sql,openlineage,pgvector,postgres] "
                                "Providers[google]",
                            }
                        ]
                    ),
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
                    "providers/http/tests/file.py",
                ),
                {
                    "selected-providers-list-as-string": "amazon apache.livy dbt.cloud dingding discord http",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI,
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "amazon...apache.livy,d",
                                "test_types": "Providers[amazon] Providers[apache.livy,dbt.cloud,dingding,discord,http]",
                            }
                        ]
                    ),
                    "individual-providers-test-types-list-as-strings-in-json": json.dumps(
                        [
                            {
                                "description": "amazon...apache.livy",
                                "test_types": "Providers[amazon] Providers[apache.livy]",
                            },
                            {"description": "dbt.cloud", "test_types": "Providers[dbt.cloud]"},
                            {"description": "dingding", "test_types": "Providers[dingding]"},
                            {"description": "discord", "test_types": "Providers[discord]"},
                            {"description": "http", "test_types": "Providers[http]"},
                        ]
                    ),
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
                    "providers/airbyte/tests/file.py",
                ),
                {
                    "selected-providers-list-as-string": "airbyte",
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "false",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI,
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "airbyte", "test_types": "Providers[airbyte]"}]
                    ),
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
                    "selected-providers-list-as-string": None,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "docs-build": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS_AND_UI,
                    "run-amazon-tests": "false",
                    "run-kubernetes-tests": "true",
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": json.dumps(
                        [{"description": "Always", "test_types": "Always"}]
                    ),
                    "providers-test-types-list-as-strings-in-json": None,
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
                    "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                    "python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "true",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
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
                    "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                    "python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "true",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="Everything should run and upgrading to newer requirements as dependencies change",
            )
        ),
        pytest.param(
            ("providers/amazon/src/airflow/providers/amazon/__init__.py",),
            {
                "selected-providers-list-as-string": "amazon apache.hive cncf.kubernetes "
                "common.compat common.messaging common.sql exasol ftp google http imap microsoft.azure "
                "mongo mysql openlineage postgres salesforce ssh teradata",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "run-amazon-tests": "true",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "amazon...google",
                            "test_types": "Providers[amazon] Providers[apache.hive,cncf.kubernetes,"
                            "common.compat,common.messaging,common.sql,exasol,ftp,http,imap,"
                            "microsoft.azure,mongo,mysql,openlineage,postgres,salesforce,ssh,teradata] "
                            "Providers[google]",
                        }
                    ]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run including amazon tests if amazon provider files changed",
        ),
        pytest.param(
            ("providers/amazon/src/airflow/providers/amazon/pyproject.toml",),
            {
                "selected-providers-list-as-string": "amazon apache.hive cncf.kubernetes "
                "common.compat common.messaging common.sql exasol ftp google http imap microsoft.azure "
                "mongo mysql openlineage postgres salesforce ssh teradata",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                # no python files changed so flynt should not run
                "skip-pre-commits": "flynt," + ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "run-amazon-tests": "true",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "amazon...google",
                            "test_types": "Providers[amazon] Providers[apache.hive,cncf.kubernetes,"
                            "common.compat,common.messaging,common.sql,exasol,ftp,http,imap,"
                            "microsoft.azure,mongo,mysql,openlineage,postgres,salesforce,ssh,teradata] "
                            "Providers[google]",
                        }
                    ]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run including amazon tests if only amazon pyproject.toml files changed",
        ),
        pytest.param(
            ("providers/amazon/src/airflow/providers/amazon/provider.yaml",),
            {
                "selected-providers-list-as-string": "amazon apache.hive cncf.kubernetes "
                "common.compat common.messaging common.sql exasol ftp google http imap microsoft.azure "
                "mongo mysql openlineage postgres salesforce ssh teradata",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                # no python files changed so flynt should not run
                "skip-pre-commits": "flynt," + ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "run-amazon-tests": "true",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "amazon...google",
                            "test_types": "Providers[amazon] Providers[apache.hive,cncf.kubernetes,"
                            "common.compat,common.messaging,common.sql,exasol,ftp,http,imap,"
                            "microsoft.azure,mongo,mysql,openlineage,postgres,salesforce,ssh,teradata] "
                            "Providers[google]",
                        }
                    ]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run including amazon tests if only amazon provider.yaml files changed",
        ),
        pytest.param(
            ("providers/airbyte/tests/airbyte/__init__.py",),
            {
                "selected-providers-list-as-string": "airbyte",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "false",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "airbyte", "test_types": "Providers[airbyte]"}]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run without amazon tests if no amazon file changed",
        ),
        pytest.param(
            ("providers/amazon/src/airflow/providers/amazon/file.py",),
            {
                "selected-providers-list-as-string": "amazon apache.hive cncf.kubernetes "
                "common.compat common.messaging common.sql exasol ftp google http imap microsoft.azure "
                "mongo mysql openlineage postgres salesforce ssh teradata",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "amazon...google",
                            "test_types": "Providers[amazon] Providers[apache.hive,cncf.kubernetes,"
                            "common.compat,common.messaging,common.sql,exasol,ftp,http,imap,"
                            "microsoft.azure,mongo,mysql,openlineage,postgres,salesforce,ssh,teradata] "
                            "Providers[google]",
                        }
                    ]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers']",
            },
            id="Providers tests run including amazon tests if amazon provider files changed",
        ),
        pytest.param(
            (
                "airflow-core/tests/unit/always/test_project_structure.py",
                "providers/common/io/tests/operators/__init__.py",
                "providers/common/io/tests/operators/test_file_transfer.py",
            ),
            {
                "selected-providers-list-as-string": "common.compat common.io openlineage",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "false",
                "run-kubernetes-tests": "false",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI_AND_HELM_TESTS,
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "common.compat,common.io,openl",
                            "test_types": "Providers[common.compat,common.io,openlineage]",
                        }
                    ]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core', 'mypy-providers']",
            },
            id="Only Always and common providers tests should run when only common.io and tests/always changed",
        ),
        pytest.param(
            ("providers/standard/src/airflow/providers/standard/operators/bash.py",),
            {
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "run-amazon-tests": "true",
                "docs-build": "true",
                "run-kubernetes-tests": "true",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS,
            },
            id="All tests to run when standard operator changed",
        ),
        (
            pytest.param(
                ("airflow-core/tests/unit/utils/test_cli_util.py",),
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="All tests should be run when tests/utils/ change",
            )
        ),
        (
            pytest.param(
                ("devel-common/src/tests_common/test_utils/__init__.py",),
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "needs-helm-tests": "true",
                    "run-tests": "true",
                    "run-amazon-tests": "true",
                    "docs-build": "true",
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "testable-core-integrations": "['kerberos']",
                    "testable-providers-integrations": "['celery', 'cassandra', 'drill', 'gremlin', 'kafka', "
                    "'mongo', 'pinot', 'qdrant', 'redis', 'trino', 'ydb']",
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="All tests should be run when devel-common/ change",
            )
        ),
        (
            pytest.param(
                ("airflow-core/src/airflow/ui/src/index.tsx",),
                {
                    "selected-providers-list-as-string": None,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "ci-image-build": "false",
                    "prod-image-build": "false",
                    "needs-helm-tests": "false",
                    "run-tests": "false",
                    "run-amazon-tests": "false",
                    "docs-build": "false",
                    "full-tests-needed": "false",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_CODE_PROVIDERS_AND_HELM_TESTS,
                    "upgrade-to-newer-dependencies": "false",
                    "needs-mypy": "false",
                    "mypy-checks": "[]",
                    "run-ui-tests": "true",
                    "only-new-ui-files": "true",
                },
                id="Run only ui tests for PR with new UI only changes.",
            )
        ),
        pytest.param(
            ("RELEASE_NOTES.rst",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "run-amazon-tests": "false",
                "docs-build": "true",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_ON_NO_CI_IMAGE,
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": None,
                "providers-test-types-list-as-strings-in-json": None,
                "individual-providers-test-types-list-as-strings-in-json": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Run docs-build for RELEASE_NOTES.rst",
        ),
        pytest.param(
            ("chart/RELEASE_NOTES.rst",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "run-amazon-tests": "false",
                "docs-build": "true",
                "skip-pre-commits": "check-provider-yaml-valid,flynt,identity,mypy-airflow-core,mypy-airflow-ctl,mypy-dev,mypy-devel-common,mypy-providers,mypy-task-sdk,ts-compile-format-lint-ui",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": None,
                "providers-test-types-list-as-strings-in-json": None,
                "individual-providers-test-types-list-as-strings-in-json": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Run docs-build for chart/RELEASE_NOTES.rst",
        ),
        pytest.param(
            (".github/SECURITY.md",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "run-amazon-tests": "false",
                "docs-build": "true",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_ON_NO_CI_IMAGE,
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": None,
                "providers-test-types-list-as-strings-in-json": None,
                "individual-providers-test-types-list-as-strings-in-json": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Run docs-build for SECURITY.md",
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
        pr_labels=(LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,),
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


def test_list_splitting():
    stderr = SelectiveChecks(
        pr_labels=("full tests needed",),
        default_branch="main",
    )
    output_dict = get_outputs_from_stderr(str(stderr))
    individual_providers_test_types_list_as_string = json.loads(
        output_dict["individual-providers-test-types-list-as-strings-in-json"]
    )
    all_providers_in_sub_lists = [
        list_of_types["test_types"].split(" ")
        for list_of_types in individual_providers_test_types_list_as_string
    ]
    assert len(all_providers_in_sub_lists) == NUMBER_OF_LOW_DEP_SLICES
    assert sum([len(list_of_types) for list_of_types in all_providers_in_sub_lists]) == len(
        LIST_OF_ALL_PROVIDER_TESTS
    )


def test_excluded_providers():
    stderr = SelectiveChecks(
        files=(),
        github_event=GithubEvents.PULL_REQUEST,
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "excluded-providers-as-string": json.dumps({DEFAULT_PYTHON_MAJOR_MINOR_VERSION: ["cloudant"]}),
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
                ("providers/git/src/airflow/providers/git/test_file.py",),
                (),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['13']",
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "kubernetes-versions": f"['{DEFAULT_KUBERNETES_VERSION}']",
                    "kubernetes-versions-list-as-string": DEFAULT_KUBERNETES_VERSION,
                    "kubernetes-combos-list-as-string": f"{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-{DEFAULT_KUBERNETES_VERSION}",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="Everything should run including all providers when git provider is changed"
                "(special case for now)",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md",),
                ("full tests needed", "all versions"),
                "main",
                {
                    "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                    "all-versions": "true",
                    "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                    "mysql-versions": "['8.0', '8.4']",
                    "postgres-versions": "['13', '14', '15', '16', '17']",
                    "python-versions": "['3.9', '3.10', '3.11', '3.12']",
                    "python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                    "kubernetes-versions": ALL_KUBERNETES_VERSIONS_AS_LIST,
                    "kubernetes-versions-list-as-string": ALL_KUBERNETES_VERSIONS_AS_STRING,
                    "kubernetes-combos-list-as-string": ZIP_PYTHON_AND_KUBERNETES_VERSIONS_AS_STRING,
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
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
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['13']",
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "kubernetes-versions": f"['{DEFAULT_KUBERNETES_VERSION}']",
                    "kubernetes-versions-list-as-string": DEFAULT_KUBERNETES_VERSION,
                    "kubernetes-combos-list-as-string": f"{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-{DEFAULT_KUBERNETES_VERSION}",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
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
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "all-versions": "false",
                    "mysql-versions": "['8.0']",
                    "postgres-versions": "['13']",
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "kubernetes-versions": f"['{DEFAULT_KUBERNETES_VERSION}']",
                    "kubernetes-versions-list-as-string": DEFAULT_KUBERNETES_VERSION,
                    "kubernetes-combos-list-as-string": f"{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-{DEFAULT_KUBERNETES_VERSION}",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
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
                    "all-python-versions": f"['{ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[-1]}']",
                    "all-python-versions-list-as-string": f"{ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[-1]}",
                    "all-versions": "false",
                    "default-python-version": f"{ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[-1]}",
                    "mysql-versions": "['8.4']",
                    "postgres-versions": "['17']",
                    "python-versions": f"['{ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[-1]}']",
                    "python-versions-list-as-string": f"{ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[-1]}",
                    "kubernetes-versions": f"['{ALLOWED_KUBERNETES_VERSIONS[-1]}']",
                    "kubernetes-versions-list-as-string": f"{ALLOWED_KUBERNETES_VERSIONS[-1]}",
                    "kubernetes-combos-list-as-string": f"{ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[-1]}-{ALLOWED_KUBERNETES_VERSIONS[-1]}",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
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
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "all-versions": "false",
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "kubernetes-versions": f"['{DEFAULT_KUBERNETES_VERSION}']",
                    "kubernetes-versions-list-as-string": DEFAULT_KUBERNETES_VERSION,
                    "kubernetes-combos-list-as-string": f"{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-{DEFAULT_KUBERNETES_VERSION}",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
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
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "all-versions": "false",
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "kubernetes-versions": f"['{DEFAULT_KUBERNETES_VERSION}']",
                    "kubernetes-versions-list-as-string": DEFAULT_KUBERNETES_VERSION,
                    "kubernetes-combos-list-as-string": f"{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-{DEFAULT_KUBERNETES_VERSION}",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "false",
                    "docs-build": "true",
                    "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                    "full-tests-needed": "true",
                    "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "providers-test-types-list-as-strings-in-json": ALL_PROVIDERS_SELECTIVE_TEST_TYPES_AS_JSON,
                    "individual-providers-test-types-list-as-strings-in-json": LIST_OF_ALL_PROVIDER_TESTS_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS,
                },
                id="Everything should run including full providers when "
                "full tests are needed even if no files are changed",
            )
        ),
        (
            pytest.param(
                ("INTHEWILD.md", "providers/asana/tests/asana.py"),
                (
                    "full tests needed",
                    LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,
                ),
                "v2-7-stable",
                {
                    "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                    "python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                    "all-versions": "false",
                    "ci-image-build": "true",
                    "prod-image-build": "true",
                    "run-tests": "true",
                    "skip-providers-tests": "true",
                    "docs-build": "true",
                    "docs-list-as-string": "apache-airflow docker-stack",
                    "full-tests-needed": "true",
                    "skip-pre-commits": All_SKIPPED_COMMITS_IF_NON_MAIN_BRANCH,
                    "upgrade-to-newer-dependencies": "false",
                    "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                    "needs-mypy": "true",
                    "mypy-checks": ALL_MYPY_CHECKS_EXCEPT_PROVIDERS,
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
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "skip-providers-tests": "true",
                "docs-build": "false",
                "docs-list-as-string": None,
                "full-tests-needed": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            (
                "chart/aaaa.txt",
                "providers/google/tests/unit/google/file.py",
            ),
            {
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "needs-helm-tests": "false",
                "ci-image-build": "true",
                "prod-image-build": "true",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="No Helm tests, No providers no lint charts, should run if "
            "only chart/providers changed in non-main but PROD image should be built",
        ),
        pytest.param(
            (
                "airflow-core/src/airflow/cli/test.py",
                "chart/aaaa.txt",
                "providers/google/tests/unit/google/file.py",
            ),
            {
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always...CLI", "test_types": "Always CLI"}]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core']",
            },
            id="Only CLI tests and Kubernetes tests should run if cli/chart files changed in non-main branch",
        ),
        pytest.param(
            (
                "airflow-core/src/airflow/file.py",
                "providers/google/tests/unit/google/file.py",
            ),
            {
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow docker-stack",
                "full-tests-needed": "false",
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core']",
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
        pr_labels=(LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,),
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
                "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                "upgrade-to-newer-dependencies": "true",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS,
            },
            id="All tests run on push even if unimportant file changed",
        ),
        pytest.param(
            ("INTHEWILD.md",),
            (),
            "v2-3-stable",
            {
                "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": All_SKIPPED_COMMITS_IF_NON_MAIN_BRANCH,
                "docs-list-as-string": "apache-airflow docker-stack",
                "upgrade-to-newer-dependencies": "true",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS_EXCEPT_PROVIDERS,
            },
            id="All tests except Providers and Helm run on push"
            " even if unimportant file changed in non-main branch",
        ),
        pytest.param(
            ("airflow-core/src/airflow/api.py",),
            (),
            "main",
            {
                "selected-providers-list-as-string": ALL_PROVIDERS_AFFECTED,
                "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
                "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "docs-build": "true",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "upgrade-to-newer-dependencies": "true",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS,
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
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "false",
                "skip-providers-tests": "true",
                "docs-build": "false",
                "docs-list-as-string": None,
                "upgrade-to-newer-dependencies": "false",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NOT_IMPORTANT_FILES_CHANGED,
                "core-test-types-list-as-strings-in-json": None,
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Nothing should run if only non-important files changed",
        ),
        pytest.param(
            ("airflow-core/tests/system/any_file.py",),
            {
                "selected-providers-list-as-string": None,
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": ALL_DOCS_SELECTED_FOR_BUILD,
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS,
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always", "test_types": "Always"}]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core']",
            },
            id="Only Always and docs build should run if only system tests changed",
        ),
        pytest.param(
            (
                "airflow-core/src/airflow/cli/test.py",
                "chart/aaaa.txt",
                "providers/google/tests/unit/google/file.py",
            ),
            {
                "selected-providers-list-as-string": "amazon apache.beam apache.cassandra apache.kafka "
                "cncf.kubernetes common.compat common.sql "
                "facebook google hashicorp microsoft.azure microsoft.mssql mysql "
                "openlineage oracle postgres presto salesforce samba sftp ssh trino",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "skip-providers-tests": "false",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow helm-chart amazon apache.beam apache.cassandra "
                "apache.kafka cncf.kubernetes common.compat common.sql facebook google hashicorp microsoft.azure "
                "microsoft.mssql mysql openlineage oracle postgres "
                "presto salesforce samba sftp ssh trino",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_UI,
                "run-kubernetes-tests": "true",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": json.dumps(
                    [{"description": "Always...CLI", "test_types": "Always CLI"}]
                ),
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "amazon...google",
                            "test_types": "Providers[amazon] Providers[apache.beam,apache.cassandra,"
                            "apache.kafka,cncf.kubernetes,common.compat,common.sql,facebook,"
                            "hashicorp,microsoft.azure,microsoft.mssql,mysql,"
                            "openlineage,oracle,postgres,presto,salesforce,samba,sftp,ssh,trino] "
                            "Providers[google]",
                        }
                    ]
                ),
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core', 'mypy-providers']",
            },
            id="CLI tests and Google-related provider tests should run if cli/chart files changed but "
            "prod image should be build too and k8s tests too",
        ),
        pytest.param(
            ("airflow-core/src/airflow/models/test.py",),
            {
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "true",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_IF_NO_PROVIDERS_UI_AND_HELM_TESTS,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core']",
            },
            id="Tests for all airflow core types except providers should run if model file changed",
        ),
        pytest.param(
            ("airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml",),
            {
                "selected-providers-list-as-string": "",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "needs-helm-tests": "true",
                "run-tests": "true",
                "skip-providers-tests": "false",
                "docs-build": "true",
                "docs-list-as-string": "",
                "upgrade-to-newer-dependencies": "false",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS,
            },
            id="pre commit ts-compile-format-lint should not be ignored if openapi spec changed.",
        ),
        pytest.param(
            (
                "airflow-core/src/airflow/assets/",
                "airflow-core/src/airflow/models/assets/",
                "task-sdk/src/airflow/sdk/definitions/asset/",
                "airflow-core/src/airflow/datasets/",
            ),
            {
                "selected-providers-list-as-string": "amazon common.compat common.io common.sql "
                "databricks dbt.cloud ftp google microsoft.mssql mysql "
                "openlineage postgres sftp snowflake trino",
                "all-python-versions": f"['{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}']",
                "all-python-versions-list-as-string": DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
                "run-tests": "true",
                "skip-providers-tests": "false",
                "docs-build": "true",
                "docs-list-as-string": "apache-airflow amazon common.compat common.io common.sql "
                "databricks dbt.cloud ftp google microsoft.mssql mysql "
                "openlineage postgres sftp snowflake trino",
                "skip-pre-commits": ALL_SKIPPED_COMMITS_ON_NO_CI_IMAGE,
                "run-kubernetes-tests": "false",
                "upgrade-to-newer-dependencies": "false",
                "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
                "providers-test-types-list-as-strings-in-json": json.dumps(
                    [
                        {
                            "description": "amazon...google",
                            "test_types": "Providers[amazon] Providers[common.compat,common.io,common.sql,"
                            "databricks,dbt.cloud,ftp,microsoft.mssql,mysql,openlineage,"
                            "postgres,sftp,snowflake,trino] Providers[google]",
                        }
                    ]
                ),
                "needs-mypy": "false",
                "mypy-checks": "[]",
            },
            id="Trigger openlineage and related providers tests when Assets files changed",
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
        pr_labels=(LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,),
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
            "all-python-versions": ALL_PYTHON_VERSIONS_AS_LIST,
            "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
            "ci-image-build": "true",
            "prod-image-build": "true",
            "needs-helm-tests": "true",
            "run-tests": "true",
            "docs-build": "true",
            "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
            "upgrade-to-newer-dependencies": (
                "true" if github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE] else "false"
            ),
            "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
            "needs-mypy": "true",
            "mypy-checks": ALL_MYPY_CHECKS,
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
        files=(
            "airflow-core/src/airflow/ui/src/pages/Run/Details.tsx",
            "airflow-core/src/airflow/ui/src/router.tsx",
        ),
        commit_ref="",
        github_event=github_event,
        pr_labels=(),
        default_branch="main",
    )
    assert_outputs_are_printed(
        {
            "all-python-versions": "['3.9', '3.10', '3.11', '3.12']",
            "all-python-versions-list-as-string": ALL_PYTHON_VERSIONS_AS_STRING,
            "ci-image-build": "true",
            "prod-image-build": "true",
            "needs-helm-tests": "true",
            "run-tests": "true",
            "docs-build": "true",
            "skip-pre-commits": ALL_SKIPPED_COMMITS_BY_DEFAULT_ON_ALL_TESTS_NEEDED,
            "upgrade-to-newer-dependencies": (
                "true" if github_event in [GithubEvents.PUSH, GithubEvents.SCHEDULE] else "false"
            ),
            "core-test-types-list-as-strings-in-json": ALL_CI_SELECTIVE_TEST_TYPES_AS_JSON,
            "needs-mypy": "true",
            "mypy-checks": ALL_MYPY_CHECKS,
        },
        str(stderr),
    )


@pytest.mark.parametrize(
    "files, expected_outputs, pr_labels, commit_ref",
    [
        pytest.param(
            ("airflow-core/src/airflow/models/dag.py",),
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
            ("providers/microsoft/azure/src/airflow/providers/microsoft/azure/provider.yaml",),
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
            ("airflow-core/src/airflow/models/dag.py",),
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
            ("providers/google/docs/some_file.rst",),
            {
                "docs-list-as-string": "amazon apache.beam apache.cassandra apache.kafka "
                "cncf.kubernetes common.compat common.sql facebook google hashicorp "
                "microsoft.azure microsoft.mssql mysql openlineage oracle "
                "postgres presto salesforce samba sftp ssh trino",
            },
            id="Google provider docs changed",
        ),
        pytest.param(
            ("providers/common/sql/src/airflow/providers/common/sql/common_sql_python.py",),
            {
                "docs-list-as-string": "amazon apache.drill apache.druid apache.hive "
                "apache.impala apache.pinot common.sql databricks elasticsearch "
                "exasol google jdbc microsoft.mssql mysql odbc openlineage "
                "oracle pgvector postgres presto slack snowflake sqlite teradata trino vertica ydb",
            },
            id="Common SQL provider package python files changed",
        ),
        pytest.param(
            ("providers/airbyte/docs/some_file.rst",),
            {
                "docs-list-as-string": "airbyte",
            },
            id="Airbyte provider docs changed",
        ),
        pytest.param(
            ("providers/airbyte/docs/some_file.rst", "airflow-core/docs/docs.rst"),
            {
                "docs-list-as-string": "apache-airflow airbyte",
            },
            id="Airbyte provider and airflow core docs changed",
        ),
        pytest.param(
            (
                "providers/airbyte/docs/some_file.rst",
                "airflow-core/docs/docs.rst",
                "providers-summary-docs/docs.rst",
            ),
            {
                "docs-list-as-string": "apache-airflow apache-airflow-providers airbyte",
            },
            id="Airbyte provider and airflow core and common provider docs changed",
        ),
        pytest.param(
            ("airflow-core/docs/docs.rst",),
            {
                "docs-list-as-string": "apache-airflow",
            },
            id="Only Airflow docs changed",
        ),
        pytest.param(
            ("providers/celery/src/airflow/providers/celery/file.py",),
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
            ("airflow-core/src/airflow/test.py",),
            {
                "docs-list-as-string": "apache-airflow",
            },
            id="Core files changed. Apache-Airflow docs should also be built",
        ),
        pytest.param(
            ("docker-stack-docs/test.rst",),
            {"docs-list-as-string": "docker-stack"},
            id="Docker stack files changed. No provider docs to build",
        ),
        pytest.param(
            ("airflow-core/src/airflow/test.py", "chart/airflow/values.yaml"),
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
            ("chart/docs/airflow/values.yaml",),
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
            ("helm-tests/tests/helm_tests/random_helm_test.py",),
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
    "files, expected_outputs,",
    [
        pytest.param(
            ("providers/amazon/provider.yaml",),
            {
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
            },
            id="Amazon provider.yaml",
        ),
        pytest.param(
            ("providers/amazon/pyproject.toml",),
            {
                "ci-image-build": "true",
                "prod-image-build": "false",
                "needs-helm-tests": "false",
            },
            id="Amazon pyproject.toml",
        ),
        pytest.param(
            ("providers/cncf/kubernetes/provider.yaml",),
            {
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
            },
            id="CNCF Kubernetes provider.yaml",
        ),
        pytest.param(
            ("providers/cncf/kubernetes/pyproject.toml",),
            {
                "ci-image-build": "true",
                "prod-image-build": "true",
                "needs-helm-tests": "false",
            },
            id="CNCF Kubernetes pyproject.toml",
        ),
    ],
)
def test_provider_yaml_or_pyproject_toml_changes_trigger_ci_build(
    files: tuple[str, ...], expected_outputs: dict[str, str]
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
    "files, has_migrations",
    [
        pytest.param(
            ("airflow-core/src/airflow/test.py",),
            False,
            id="No migrations",
        ),
        pytest.param(
            ("airflow-core/src/airflow/migrations/test_sql", "airflow-core/src/airflow/test.py"),
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
            ("airflow-core/src/airflow/cli/file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core']",
            },
            "main",
            (),
            id="Airflow mypy checks on airflow regular files",
        ),
        pytest.param(
            ("airflow-core/src/airflow/models/file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-airflow-core']",
            },
            "main",
            (),
            id="Airflow mypy checks on airflow files with model changes.",
        ),
        pytest.param(
            ("task-sdk/src/airflow/sdk/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": "['mypy-providers', 'mypy-task-sdk']",
            },
            "main",
            (),
            id="Airflow mypy checks on Task SDK files (implies providers)",
        ),
        pytest.param(
            ("dev/a_package/a_file.py",),
            {
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS,
            },
            "main",
            (),
            id="All mypy checks on def files changed (full tests needed are implicit)",
        ),
        pytest.param(
            ("readme.md",),
            {
                "needs-mypy": "true",
                "mypy-checks": ALL_MYPY_CHECKS,
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
    "files, pr_labels, github_event",
    [
        pytest.param(
            ("airflow-core/tests/unit/test.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the the git diff Tests",
        ),
        pytest.param(
            ("providers/common/sql/tests/unit/common/sql/operators/test_sql.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff Providers",
        ),
        pytest.param(
            ("task-sdk/tests/definitions/test_dag.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff TaskSDK",
        ),
    ],
)
# Patch run_command
@patch("airflow_breeze.utils.selective_checks.run_command")
def test_is_log_mocked_in_the_tests_fail(
    mock_run_command,
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    github_event: GithubEvents,
):
    mock_run_command_result = MagicMock()
    mock_run_command_result.stdout = """
        + #Test Change
        + def test_selective_checks_caplop(self, caplog)
        +   caplog.set_level(logging.INFO)
        +   "test log" in caplog.text
    """
    mock_run_command.return_value = mock_run_command_result
    with pytest.raises(SystemExit):
        assert (
            "[error]please ask maintainer to include as an exception using "
            f"'{LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL}' label."
            in escape_ansi_colors(
                str(
                    SelectiveChecks(
                        files=files,
                        commit_ref=NEUTRAL_COMMIT,
                        pr_labels=pr_labels,
                        github_event=GithubEvents.PULL_REQUEST,
                        default_branch="main",
                    )
                )
            )
        )


@pytest.mark.parametrize(
    "files, pr_labels, github_event",
    [
        pytest.param(
            ("airflow-core/tests/unit/test.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the the git diff Tests",
        ),
        pytest.param(
            ("providers/common/sql/tests/unit/common/sql/operators/test_sql.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff Providers",
        ),
        pytest.param(
            ("task-sdk/tests/definitions/test_dag.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff TaskSDK",
        ),
    ],
)
# Patch run_command
@patch("airflow_breeze.utils.selective_checks.run_command")
def test_is_log_mocked_in_the_tests_fail_formatted(
    mock_run_command,
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    github_event: GithubEvents,
):
    mock_run_command_result = MagicMock()
    mock_run_command_result.stdout = """
        + #Test Change
        + def test_selective_checks(
        +     self,
        +     caplog
        + )
        +   caplog.set_level(logging.INFO)
        +   "test log" in caplog.text
    """
    mock_run_command.return_value = mock_run_command_result
    with pytest.raises(SystemExit):
        assert (
            "[error]please ask maintainer to include as an exception using "
            f"'{LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL}' label."
            in escape_ansi_colors(
                str(
                    SelectiveChecks(
                        files=files,
                        commit_ref=NEUTRAL_COMMIT,
                        pr_labels=pr_labels,
                        github_event=GithubEvents.PULL_REQUEST,
                        default_branch="main",
                    )
                )
            )
        )


@pytest.mark.parametrize(
    "files, pr_labels, github_event",
    [
        pytest.param(
            ("airflow-core/tests/unit/test.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the the git diff Tests",
        ),
        pytest.param(
            ("providers/common/sql/tests/unit/common/sql/operators/test_sql.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff Providers",
        ),
        pytest.param(
            ("task-sdk/tests/definitions/test_dag.py",),
            (),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff TaskSDK",
        ),
    ],
)
# Patch run_command
@patch("airflow_breeze.utils.selective_checks.run_command")
def test_is_log_mocked_in_the_tests_not_fail(
    mock_run_command,
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    github_event: GithubEvents,
):
    mock_run_command_result = MagicMock()
    mock_run_command_result.stdout = """
         + #Test Change
         + def test_selective_checks(self)
         +   assert "I am just a test" == "I am just a test"
     """
    mock_run_command.return_value = mock_run_command_result
    selective_checks = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        pr_labels=pr_labels,
        github_event=GithubEvents.PULL_REQUEST,
        default_branch="main",
    )
    assert selective_checks.is_log_mocked_in_the_tests


@pytest.mark.parametrize(
    "files, pr_labels, github_event",
    [
        pytest.param(
            ("airflow-core/tests/unit/test.py",),
            (LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the the git diff Tests",
        ),
        pytest.param(
            ("providers/common/sql/tests/unit/common/sql/operators/test_sql.py",),
            (LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff Providers",
        ),
        pytest.param(
            ("task-sdk/tests/definitions/test_dag.py",),
            (LOG_WITHOUT_MOCK_IN_TESTS_EXCEPTION_LABEL,),
            GithubEvents.PULL_REQUEST,
            id="Caplog is in the git diff TaskSDK",
        ),
    ],
)
# Patch run_command
@patch("airflow_breeze.utils.selective_checks.run_command")
def test_is_log_mocked_in_the_tests_not_fail_with_label(
    mock_run_command,
    files: tuple[str, ...],
    pr_labels: tuple[str, ...],
    github_event: GithubEvents,
):
    mock_run_command_result = MagicMock()
    mock_run_command_result.stdout = """
        + #Test Change
        + def test_selective_checks_caplop(self, caplog)
        +   caplog.set_level(logging.INFO)
        +   "test log" in caplog.text
    """
    mock_run_command.return_value = mock_run_command_result
    selective_checks = SelectiveChecks(
        files=files,
        commit_ref=NEUTRAL_COMMIT,
        pr_labels=pr_labels,
        github_event=GithubEvents.PULL_REQUEST,
        default_branch="main",
    )
    assert selective_checks.is_log_mocked_in_the_tests
