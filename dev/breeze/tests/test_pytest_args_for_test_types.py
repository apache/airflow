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

from airflow_breeze.global_constants import GroupOfTests
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.run_tests import convert_parallel_types_to_folders, convert_test_type_to_pytest_args


# TODO(potiuk): rename to all_providers when we move all providers to the new structure
def _all_new_providers() -> list[str]:
    all_new_providers: list[str] = []
    providers_root = AIRFLOW_SOURCES_ROOT / "providers"
    for file in providers_root.rglob("provider.yaml"):
        # TODO: remove this check when all providers are moved to the new structure
        if file.is_relative_to(providers_root / "src"):
            continue
        provider_path = file.parent.relative_to(providers_root)
        all_new_providers.append(provider_path.as_posix())
    return sorted(all_new_providers)


@pytest.mark.parametrize(
    "test_group, test_type, pytest_args",
    [
        # Those list needs to be updated every time we add a new directory to tests/ folder
        (
            GroupOfTests.CORE,
            "Core",
            [
                "tests/core",
                "tests/executors",
                "tests/jobs",
                "tests/models",
                "tests/ti_deps",
                "tests/utils",
            ],
        ),
        (
            GroupOfTests.INTEGRATION_PROVIDERS,
            "All",
            ["providers/tests/integration"],
        ),
        (
            GroupOfTests.INTEGRATION_CORE,
            "All",
            ["tests/integration"],
        ),
        (
            GroupOfTests.CORE,
            "API",
            ["tests/api", "tests/api_connexion", "tests/api_fastapi"],
        ),
        (
            GroupOfTests.CORE,
            "Serialization",
            ["tests/serialization"],
        ),
        (
            GroupOfTests.CORE,
            "Operators",
            ["tests/operators"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers",
            [*[f"providers/{provider}/tests" for provider in _all_new_providers()], "providers/tests"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[amazon]",
            ["providers/tests/amazon"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[common.io]",
            ["providers/common/io/tests"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[amazon,google,apache.hive]",
            ["providers/tests/amazon", "providers/tests/google", "providers/tests/apache/hive"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[-amazon,google,microsoft.azure]",
            [
                *[f"providers/{provider}/tests" for provider in _all_new_providers()],
                "providers/tests",
                "--ignore=providers/tests/amazon",
                "--ignore=providers/tests/google",
                "--ignore=providers/tests/microsoft/azure",
            ],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[-edge]",
            [
                *[f"providers/{provider}/tests" for provider in _all_new_providers() if provider != "edge"],
                "providers/tests",
            ],
        ),
        (
            GroupOfTests.CORE,
            "All-Quarantined",
            ["tests", "-m", "quarantined", "--include-quarantined"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "All-Quarantined",
            [
                *[f"providers/{provider}/tests" for provider in _all_new_providers()],
                "providers/tests",
                "-m",
                "quarantined",
                "--include-quarantined",
            ],
        ),
        (
            GroupOfTests.CORE,
            "Other",
            [
                "tests/assets",
                "tests/auth",
                "tests/callbacks",
                "tests/charts",
                "tests/cluster_policies",
                "tests/config_templates",
                "tests/dag_processing",
                "tests/datasets",
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
                "tests/testconfig",
                "tests/timetables",
            ],
        ),
        (
            GroupOfTests.HELM,
            "All",
            ["helm_tests"],
        ),
        (
            GroupOfTests.HELM,
            "airflow_aux",
            ["helm_tests/airflow_aux"],
        ),
    ],
)
def test_pytest_args_for_regular_test_types(
    test_group: GroupOfTests,
    test_type: str,
    pytest_args: list[str],
):
    assert (
        convert_test_type_to_pytest_args(
            test_group=test_group,
            test_type=test_type,
        )
        == pytest_args
    )


def test_pytest_args_for_missing_provider():
    with pytest.raises(SystemExit):
        convert_test_type_to_pytest_args(
            test_group=GroupOfTests.PROVIDERS,
            test_type="Providers[missing.provider]",
        )


@pytest.mark.parametrize(
    "test_group, parallel_test_types, folders",
    [
        (
            GroupOfTests.CORE,
            "API",
            ["tests/api", "tests/api_connexion", "tests/api_fastapi"],
        ),
        (
            GroupOfTests.CORE,
            "CLI",
            [
                "tests/cli",
            ],
        ),
        (
            GroupOfTests.CORE,
            "API CLI",
            [
                "tests/api",
                "tests/api_connexion",
                "tests/api_fastapi",
                "tests/cli",
            ],
        ),
        (
            GroupOfTests.CORE,
            "Core",
            ["tests/core", "tests/executors", "tests/jobs", "tests/models", "tests/ti_deps", "tests/utils"],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers",
            [
                *[f"providers/{provider}/tests" for provider in _all_new_providers()],
                "providers/tests",
            ],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[amazon]",
            [
                "providers/tests/amazon",
            ],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[amazon] Providers[google]",
            [
                "providers/tests/amazon",
                "providers/tests/google",
            ],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[-amazon,google]",
            [
                *[f"providers/{provider}/tests" for provider in _all_new_providers()],
                "providers/tests",
            ],
        ),
        (
            GroupOfTests.PROVIDERS,
            "Providers[-amazon,google] Providers[amazon] Providers[google]",
            [
                *[f"providers/{provider}/tests" for provider in _all_new_providers()],
                "providers/tests",
            ],
        ),
        (
            GroupOfTests.INTEGRATION_PROVIDERS,
            "All",
            [
                "providers/tests/integration",
            ],
        ),
        (
            GroupOfTests.HELM,
            "All",
            [
                "helm_tests",
            ],
        ),
        (
            GroupOfTests.TASK_SDK,
            "All",
            [
                "task_sdk/tests",
            ],
        ),
        (
            GroupOfTests.INTEGRATION_CORE,
            "All",
            [
                "tests/integration",
            ],
        ),
        (
            GroupOfTests.SYSTEM,
            "None",
            [],
        ),
    ],
)
def test_folders_for_parallel_test_types(
    test_group: GroupOfTests, parallel_test_types: str, folders: list[str]
):
    assert (
        convert_parallel_types_to_folders(
            test_group=test_group,
            parallel_test_types_list=parallel_test_types.split(" "),
        )
        == folders
    )


@pytest.mark.parametrize(
    "test_group, parallel_test_types",
    [
        (
            GroupOfTests.CORE,
            "Providers",
        ),
        (
            GroupOfTests.CORE,
            "Helm",
        ),
        (
            GroupOfTests.PROVIDERS,
            "API CLI",
        ),
        (
            GroupOfTests.PROVIDERS,
            "API CLI Providers",
        ),
        (
            GroupOfTests.HELM,
            "API",
        ),
        (
            GroupOfTests.HELM,
            "Providers",
        ),
        (
            GroupOfTests.INTEGRATION_PROVIDERS,
            "API",
        ),
        (
            GroupOfTests.INTEGRATION_CORE,
            "WWW",
        ),
        (
            GroupOfTests.SYSTEM,
            "CLI",
        ),
    ],
)
def xtest_wrong_types_for_parallel_test_types(test_group: GroupOfTests, parallel_test_types: str):
    with pytest.raises(SystemExit):
        convert_parallel_types_to_folders(
            test_group=test_group,
            parallel_test_types_list=parallel_test_types.split(" "),
        )
