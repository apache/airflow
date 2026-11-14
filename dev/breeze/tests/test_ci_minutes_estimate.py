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

from unittest import mock

import pytest

from airflow_breeze.utils.ci_minutes_estimate import (
    BASE_ESTIMATE,
    BASIC_CHECKS_ONLY_ESTIMATE,
    CORE_ALL_TEST_TYPES_MINUTES,
    MYPY_PROVIDERS_ESTIMATE,
    PROVIDERS_ALL_LONGEST_JOB_MINUTES,
    PROVIDERS_ALL_MINUTES,
    RUN_OVERHEAD_MINUTES,
    TS_SDK_DOCS_ESTIMATE,
    estimate_ci_minutes,
    estimate_core_job_minutes,
    estimate_providers_group,
    find_tested_provider_ids,
)
from airflow_breeze.utils.selective_checks import SelectiveChecks

OPTIONAL_FLAGS = (
    "docs_build",
    "run_unit_tests",
    "full_tests_needed",
    "prod_image_build",
    "run_kubernetes_tests",
    "run_helm_tests",
    "run_kustomize_overlays_tests",
    "run_task_sdk_tests",
    "run_agent_framework_tests",
    "run_java_sdk_tests",
    "run_go_sdk_tests",
    "run_ts_sdk_tests",
    "run_ts_sdk_docs",
    "run_airflow_ctl_tests",
    "run_mypy_providers",
)


def _selective_checks(
    *, core_test_types=(), provider_test_types=(), basic_checks_only=False, **flags
) -> mock.MagicMock:
    selective_checks = mock.MagicMock(spec=SelectiveChecks)
    selective_checks.basic_checks_only = basic_checks_only
    selective_checks.skip_providers_tests = not provider_test_types
    for flag in OPTIONAL_FLAGS:
        setattr(selective_checks, flag, flags.get(flag, False))
    selective_checks._get_core_test_types_to_run.return_value = list(core_test_types)
    selective_checks._get_providers_test_types_to_run.return_value = list(provider_test_types)
    return selective_checks


@pytest.mark.parametrize(
    ("core_test_types", "expected"),
    [(0, 0), (1, 38), (2, 99), (3, 160), (6, CORE_ALL_TEST_TYPES_MINUTES)],
)
def test_estimate_core_job_minutes(core_test_types, expected):
    assert estimate_core_job_minutes(core_test_types) == expected


@pytest.mark.parametrize(
    ("provider_test_types", "expected"),
    [
        (["Providers[amazon]", "Providers[google]"], ["amazon", "google"]),
        (["Providers[common.compat,fab]"], ["common.compat", "fab"]),
        (["Providers[-amazon,google]", "Providers[amazon]"], None),
        (["Providers"], None),
    ],
)
def test_find_tested_provider_ids(provider_test_types, expected):
    assert find_tested_provider_ids(provider_test_types) == expected


@mock.patch("airflow_breeze.utils.ci_minutes_estimate.count_provider_test_files", autospec=True)
def test_estimate_providers_group_weights_providers_by_test_files(mock_count):
    mock_count.side_effect = lambda provider_id: {"fab": 50, "amazon": 500}[provider_id]

    small = estimate_providers_group(["Providers[fab]"])
    large = estimate_providers_group(["Providers[amazon]"])

    assert small.name == "Provider tests (1 providers, 50 test files)"
    assert small.job_minutes < large.job_minutes
    assert small.critical_path_minutes < large.critical_path_minutes


def test_estimate_providers_group_for_all_providers():
    group = estimate_providers_group(["Providers"])

    assert (group.job_minutes, group.critical_path_minutes) == (
        PROVIDERS_ALL_MINUTES,
        PROVIDERS_ALL_LONGEST_JOB_MINUTES,
    )


def test_estimate_providers_group_without_providers():
    assert estimate_providers_group([]) is None


def test_estimate_basic_checks_only():
    estimate = estimate_ci_minutes(_selective_checks(basic_checks_only=True))

    assert [group.name for group in estimate.job_groups] == ["Basic checks only"]
    assert estimate.job_minutes == BASIC_CHECKS_ONLY_ESTIMATE[0]
    assert estimate.wall_clock_minutes == BASIC_CHECKS_ONLY_ESTIMATE[1]


@pytest.mark.parametrize(
    ("flags", "expected_groups"),
    [
        pytest.param(
            {"run_ts_sdk_docs": True}, ["Basic checks only", "TypeScript SDK docs"], id="ts-sdk docs"
        ),
        pytest.param(
            {"run_java_sdk_tests": True, "run_go_sdk_tests": True, "run_airflow_ctl_tests": True},
            ["Basic checks only", "Java SDK tests", "Go SDK tests", "Airflow CTL tests"],
            id="sdk tests",
        ),
    ],
)
def test_estimate_basic_checks_only_counts_jobs_that_need_no_ci_image(flags, expected_groups):
    estimate = estimate_ci_minutes(_selective_checks(basic_checks_only=True, **flags))

    assert [group.name for group in estimate.job_groups] == expected_groups


def test_estimate_counts_ts_sdk_docs_with_the_ci_image():
    estimate = estimate_ci_minutes(_selective_checks(run_ts_sdk_docs=True))

    assert estimate.job_groups[-1].name == "TypeScript SDK docs"
    assert estimate.job_minutes == BASE_ESTIMATE[0] + TS_SDK_DOCS_ESTIMATE[0]


@mock.patch("airflow_breeze.utils.ci_minutes_estimate.count_provider_test_files", return_value=100)
def test_estimate_lists_the_job_groups_that_run(mock_count):
    estimate = estimate_ci_minutes(
        _selective_checks(
            core_test_types=["Always"],
            provider_test_types=["Providers[http]"],
            docs_build=True,
            run_unit_tests=True,
            prod_image_build=True,
            run_helm_tests=True,
        )
    )

    assert [group.name for group in estimate.job_groups] == [
        "CI image, basic tests and static checks",
        "Documentation build",
        "Core tests (1 test types)",
        "Integration and system tests",
        "Provider tests (1 providers, 100 test files)",
        "PROD image build and tests",
        "Helm tests",
    ]
    assert estimate.job_minutes == sum(group.job_minutes for group in estimate.job_groups)
    assert estimate.wall_clock_minutes == RUN_OVERHEAD_MINUTES + max(
        group.critical_path_minutes for group in estimate.job_groups
    )


def test_estimate_full_tests_adds_special_tests_and_all_core_types():
    estimate = estimate_ci_minutes(
        _selective_checks(
            core_test_types=["API", "Always", "CLI", "Core", "Other", "Serialization"],
            provider_test_types=["Providers"],
            run_unit_tests=True,
            full_tests_needed=True,
        )
    )

    names = [group.name for group in estimate.job_groups]
    assert "Core tests (all test types)" in names
    assert "Provider tests (all providers)" in names
    assert "Special tests" in names


def test_estimate_does_not_count_companion_core_test_types():
    estimate = estimate_ci_minutes(
        _selective_checks(core_test_types=["API", "Always", "PublicAPI"], run_unit_tests=True)
    )

    core_group = next(group for group in estimate.job_groups if group.name.startswith("Core tests"))
    assert core_group.name == "Core tests (2 test types)"
    assert core_group.job_minutes == estimate_core_job_minutes(2)


def test_estimate_without_unit_tests_has_only_base_group():
    estimate = estimate_ci_minutes(_selective_checks())

    assert estimate.job_minutes == BASE_ESTIMATE[0]


@pytest.mark.parametrize(
    ("run_unit_tests", "expected_groups"),
    [
        pytest.param(
            False, ["CI image, basic tests and static checks", "Provider mypy checks"], id="metadata only"
        ),
        pytest.param(
            True,
            [
                "CI image, basic tests and static checks",
                "Integration and system tests",
                "Provider tests (1 providers, 100 test files)",
                "Provider mypy checks",
            ],
            id="with unit tests",
        ),
    ],
)
@mock.patch("airflow_breeze.utils.ci_minutes_estimate.count_provider_test_files", return_value=100)
def test_estimate_charges_provider_tests_only_with_unit_tests(mock_count, run_unit_tests, expected_groups):
    estimate = estimate_ci_minutes(
        _selective_checks(
            provider_test_types=["Providers[google]"], run_unit_tests=run_unit_tests, run_mypy_providers=True
        )
    )

    assert [group.name for group in estimate.job_groups] == expected_groups
    if not run_unit_tests:
        assert estimate.job_minutes == BASE_ESTIMATE[0] + MYPY_PROVIDERS_ESTIMATE[0]
