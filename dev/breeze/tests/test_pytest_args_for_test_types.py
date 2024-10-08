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

import pytest

from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION
from airflow_breeze.utils.run_tests import convert_parallel_types_to_folders, convert_test_type_to_pytest_args


@pytest.mark.parametrize(
    "test_type, pytest_args, skip_provider_tests",
    [
        # Those list needs to be updated every time we add a new directory to tests/ folder
        (
            "Core",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
            ],
            False,
        ),
        (
            "Integration",
            ["tests/integration"],
            False,
        ),
        (
            "Integration",
            [
                "tests/integration/cli",
                "tests/integration/executors",
                "tests/integration/security",
            ],
            True,
        ),
        (
            "API",
            ["tests/api", "tests/api_connexion", "tests/api_internal", "tests/api_fastapi"],
            False,
        ),
        (
            "Serialization",
            ["tests/serialization"],
            False,
        ),
        (
            "System",
            ["tests/system"],
            False,
        ),
        (
            "Operators",
            ["tests/operators", "--exclude-virtualenv-operator", "--exclude-external-python-operator"],
            False,
        ),
        (
            "Providers",
            ["tests/providers"],
            False,
        ),
        (
            "Providers",
            [],
            True,
        ),
        (
            "Providers[amazon]",
            ["tests/providers/amazon"],
            False,
        ),
        (
            "Providers[common.io]",
            ["tests/providers/common/io"],
            False,
        ),
        (
            "Providers[amazon,google,apache.hive]",
            ["tests/providers/amazon", "tests/providers/google", "tests/providers/apache/hive"],
            False,
        ),
        (
            "Providers[-amazon,google,microsoft.azure]",
            [
                "tests/providers",
                "--ignore=tests/providers/amazon",
                "--ignore=tests/providers/google",
                "--ignore=tests/providers/microsoft/azure",
            ],
            False,
        ),
        (
            "PlainAsserts",
            [
                "tests/operators/test_python.py::TestPythonVirtualenvOperator::test_airflow_context",
                "--assert=plain",
            ],
            False,
        ),
        (
            "All-Quarantined",
            ["tests", "-m", "quarantined", "--include-quarantined"],
            False,
        ),
        (
            "PythonVenv",
            [
                "tests/operators/test_python.py::TestPythonVirtualenvOperator",
            ],
            False,
        ),
        (
            "BranchPythonVenv",
            [
                "tests/operators/test_python.py::TestBranchPythonVirtualenvOperator",
            ],
            False,
        ),
        (
            "ExternalPython",
            [
                "tests/operators/test_python.py::TestExternalPythonOperator",
            ],
            False,
        ),
        (
            "BranchExternalPython",
            [
                "tests/operators/test_python.py::TestBranchExternalPythonOperator",
            ],
            False,
        ),
        (
            "Other",
            [
                "tests/assets",
                "tests/auth",
                "tests/callbacks",
                "tests/charts",
                "tests/cluster_policies",
                "tests/config_templates",
                "tests/dag_processing",
                "tests/decorators",
                "tests/hooks",
                "tests/io",
                "tests/lineage",
                "tests/listeners",
                "tests/macros",
                "tests/notifications",
                "tests/plugins",
                "tests/secrets",
                "tests/security",
                "tests/sensors",
                "tests/task",
                "tests/template",
                "tests/testconfig",
                "tests/timetables",
                "tests/triggers",
            ],
            False,
        ),
    ],
)
def test_pytest_args_for_regular_test_types(
    test_type: str,
    pytest_args: list[str],
    skip_provider_tests: bool,
):
    assert (
        convert_test_type_to_pytest_args(
            test_type=test_type,
            skip_provider_tests=skip_provider_tests,
        )
        == pytest_args
    )


def test_pytest_args_for_missing_provider():
    with pytest.raises(SystemExit):
        convert_test_type_to_pytest_args(
            test_type="Providers[missing.provider]",
            skip_provider_tests=False,
        )


@pytest.mark.parametrize(
    "helm_test_package, pytest_args",
    [
        (
            None,
            ["helm_tests"],
        ),
        (
            "airflow_aux",
            ["helm_tests/airflow_aux"],
        ),
        (
            "all",
            ["helm_tests"],
        ),
    ],
)
def test_pytest_args_for_helm_test_types(helm_test_package: str, pytest_args: list[str]):
    assert (
        convert_test_type_to_pytest_args(
            test_type="Helm",
            skip_provider_tests=False,
            helm_test_package=helm_test_package,
        )
        == pytest_args
    )


@pytest.mark.parametrize(
    "parallel_test_types, folders, skip_provider_tests",
    [
        (
            "API",
            ["tests/api", "tests/api_connexion", "tests/api_internal", "tests/api_fastapi"],
            False,
        ),
        (
            "CLI",
            [
                "tests/cli",
            ],
            False,
        ),
        (
            "API CLI",
            [
                "tests/api",
                "tests/api_connexion",
                "tests/api_internal",
                "tests/api_fastapi",
                "tests/cli",
            ],
            False,
        ),
        (
            "Core",
            ["tests/core", "tests/executors", "tests/jobs", "tests/models", "tests/ti_deps", "tests/utils"],
            False,
        ),
        (
            "Core Providers",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
                "tests/providers",
            ],
            False,
        ),
        (
            "Core Providers[amazon]",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
                "tests/providers/amazon",
            ],
            False,
        ),
        (
            "Core Providers[amazon] Providers[google]",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
                "tests/providers/amazon",
                "tests/providers/google",
            ],
            False,
        ),
        (
            "Core Providers[-amazon,google]",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
                "tests/providers",
            ],
            False,
        ),
        (
            "Core Providers[amazon] Providers[google]",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
            ],
            True,
        ),
        (
            "Core Providers[-amazon,google] Providers[amazon] Providers[google]",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
                "tests/providers",
            ],
            False,
        ),
    ],
)
def test_folders_for_parallel_test_types(
    parallel_test_types: str, folders: list[str], skip_provider_tests: bool
):
    assert (
        convert_parallel_types_to_folders(
            parallel_test_types_list=parallel_test_types.split(" "),
            skip_provider_tests=skip_provider_tests,
            python_version=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        )
        == folders
    )
