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
"""
Estimate the GitHub Actions minutes a ``Tests (AMD)`` PR run takes from its selective-check outputs.

The numbers are medians measured on recent PR runs on GitHub-hosted (public) runners, which is where
fork PRs run. They are estimates for deciding whether a change's CI run is cheap enough to approve
without a maintainer, not a bill: queueing, retries and cache misses are not modelled.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from airflow_breeze.global_constants import CORE_COMPANION_TEST_TYPES, all_selective_core_test_types
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

# TODO: refresh these estimates periodically with an agent, and always after changing the CI
#  workflows (see .github/AGENTS.md). Run ``dev/calibrate_ci_minutes_estimates.py`` and copy the
#  medians it prints into the constants below.
#  Last calibrated: 2026-10-23, from 310 successful ``Tests (AMD)`` PR runs of 2026-10-13..2026-10-23,
#  224 of which tested a partial set of providers.
# TODO: add a second set of estimates for self-hosted (RunsOn) runners once enough runs on them exist
#  to measure. Their machine sizes differ, so the public-runner numbers do not carry over.

# Each entry: (job-minutes, minutes the group adds to the run's critical path after the CI image).
BASIC_CHECKS_ONLY_ESTIMATE = (6, 3)
BASE_ESTIMATE = (47, 17)
DOCS_ESTIMATE = (15, 10)
# The PROD image waits for constraints, so its tests sit about 26 minutes after the CI image.
PROD_IMAGE_ESTIMATE = (51, 26)
KUBERNETES_ESTIMATE = (84, 29)
HELM_ESTIMATE = (55, 10)
KUSTOMIZE_ESTIMATE = (6, 6)
INTEGRATION_ESTIMATE = (5, 5)
SPECIAL_TESTS_ESTIMATE = (317, 32)
TASK_SDK_ESTIMATE = (6, 6)
AGENT_FRAMEWORK_ESTIMATE = (11, 5)
JAVA_SDK_ESTIMATE = (7, 4)
GO_SDK_ESTIMATE = (1, 1)
TS_SDK_ESTIMATE = (1, 1)
TS_SDK_DOCS_ESTIMATE = (1, 1)
# The provider mypy job runs whenever provider code changes, also without provider unit tests.
MYPY_PROVIDERS_ESTIMATE = (8, 8)
AIRFLOW_CTL_ESTIMATE = (1, 1)

# Core tests: about 38 job-minutes for the first test type and 61 for each further one, up to
# the cost of all core test types.
CORE_FIRST_TEST_TYPE_MINUTES = 38
CORE_EXTRA_TEST_TYPE_MINUTES = 61
CORE_ALL_TEST_TYPES_MINUTES = 222
CORE_LONGEST_JOB_MINUTES = {1: 11, 2: 29}
CORE_LONGEST_JOB_MINUTES_DEFAULT = 32

# Provider tests (DB/non-DB, lowest dependencies, compatibility, distributions) scale with the size of
# the tested providers' test suites: amazon or google cost far more than a small provider. Fitted on
# 224 runs testing 2 to 22 providers, together with the provider mypy job: job-minutes ~= 105 + 0.161
# per provider test file, and the longest provider job ~= 16 + 0.015 per test file, both capped at the
# cost of testing all providers. The mypy job's median is taken off the base and the all-providers cost.
PROVIDERS_BASE_MINUTES = 105 - MYPY_PROVIDERS_ESTIMATE[0]
PROVIDERS_MINUTES_PER_TEST_FILE = 0.161
PROVIDERS_ALL_MINUTES = 315 - MYPY_PROVIDERS_ESTIMATE[0]
PROVIDERS_LONGEST_JOB_BASE_MINUTES = 16
PROVIDERS_LONGEST_JOB_MINUTES_PER_TEST_FILE = 0.015
PROVIDERS_ALL_LONGEST_JOB_MINUTES = 36

# Build info, the CI image build, queueing and the final jobs before and after the parallel part.
RUN_OVERHEAD_MINUTES = 18


@dataclass
class EstimatedJobGroup:
    name: str
    job_minutes: int
    critical_path_minutes: int


@dataclass
class CiMinutesEstimate:
    job_groups: list[EstimatedJobGroup]

    @property
    def job_minutes(self) -> int:
        return sum(group.job_minutes for group in self.job_groups)

    @property
    def wall_clock_minutes(self) -> int:
        if not self.job_groups:
            return 0
        longest = max(group.critical_path_minutes for group in self.job_groups)
        if self.job_groups[0].name == "Basic checks only":
            return longest
        return RUN_OVERHEAD_MINUTES + longest


def estimate_core_job_minutes(core_test_types: int) -> int:
    if core_test_types == 0:
        return 0
    return min(
        CORE_FIRST_TEST_TYPE_MINUTES + CORE_EXTRA_TEST_TYPE_MINUTES * (core_test_types - 1),
        CORE_ALL_TEST_TYPES_MINUTES,
    )


def count_provider_test_files(provider_id: str) -> int:
    tests_path = AIRFLOW_ROOT_PATH / "providers" / Path(*provider_id.split(".")) / "tests"
    return sum(1 for _ in tests_path.rglob("test_*.py"))


def find_tested_provider_ids(provider_test_types: list[str]) -> list[str] | None:
    """Return the ids of individually tested providers, or None when all providers are tested."""
    provider_ids = []
    for test_type in provider_test_types:
        if not test_type.startswith("Providers[") or test_type.startswith("Providers[-"):
            return None
        provider_ids.extend(test_type[len("Providers[") : -1].split(","))
    return provider_ids


def estimate_providers_group(provider_test_types: list[str]) -> EstimatedJobGroup | None:
    if not provider_test_types:
        return None
    provider_ids = find_tested_provider_ids(provider_test_types)
    if provider_ids is None:
        return EstimatedJobGroup(
            "Provider tests (all providers)", PROVIDERS_ALL_MINUTES, PROVIDERS_ALL_LONGEST_JOB_MINUTES
        )
    test_files = sum(count_provider_test_files(provider_id) for provider_id in provider_ids)
    return EstimatedJobGroup(
        f"Provider tests ({len(provider_ids)} providers, {test_files} test files)",
        min(
            round(PROVIDERS_BASE_MINUTES + PROVIDERS_MINUTES_PER_TEST_FILE * test_files),
            PROVIDERS_ALL_MINUTES,
        ),
        min(
            round(
                PROVIDERS_LONGEST_JOB_BASE_MINUTES + PROVIDERS_LONGEST_JOB_MINUTES_PER_TEST_FILE * test_files
            ),
            PROVIDERS_ALL_LONGEST_JOB_MINUTES,
        ),
    )


# Jobs that need only build-info, so they also run when no CI image is built. The Java SDK group
# includes its docs job.
IMAGE_INDEPENDENT_GROUPS = (
    ("run_java_sdk_tests", "Java SDK tests", JAVA_SDK_ESTIMATE),
    ("run_go_sdk_tests", "Go SDK tests", GO_SDK_ESTIMATE),
    ("run_ts_sdk_tests", "TypeScript SDK tests", TS_SDK_ESTIMATE),
    ("run_ts_sdk_docs", "TypeScript SDK docs", TS_SDK_DOCS_ESTIMATE),
    ("run_airflow_ctl_tests", "Airflow CTL tests", AIRFLOW_CTL_ESTIMATE),
)


def estimate_image_independent_groups(selective_checks: Any) -> list[EstimatedJobGroup]:
    return [
        EstimatedJobGroup(name, *estimate)
        for flag, name, estimate in IMAGE_INDEPENDENT_GROUPS
        if getattr(selective_checks, flag)
    ]


def estimate_ci_minutes(selective_checks: Any) -> CiMinutesEstimate:
    """Estimate the job groups, job-minutes and wall-clock time of a ``Tests (AMD)`` PR run."""
    if selective_checks.basic_checks_only:
        return CiMinutesEstimate(
            [
                EstimatedJobGroup("Basic checks only", *BASIC_CHECKS_ONLY_ESTIMATE),
                *estimate_image_independent_groups(selective_checks),
            ]
        )
    groups = [EstimatedJobGroup("CI image, basic tests and static checks", *BASE_ESTIMATE)]
    if selective_checks.docs_build:
        groups.append(EstimatedJobGroup("Documentation build", *DOCS_ESTIMATE))
    if selective_checks.run_unit_tests:
        core_test_types = len(set(selective_checks._get_core_test_types_to_run()) - CORE_COMPANION_TEST_TYPES)
        if core_test_types:
            all_types = core_test_types >= len(
                set(all_selective_core_test_types()) - CORE_COMPANION_TEST_TYPES
            )
            groups.append(
                EstimatedJobGroup(
                    f"Core tests ({'all' if all_types else core_test_types} test types)",
                    estimate_core_job_minutes(core_test_types),
                    CORE_LONGEST_JOB_MINUTES.get(core_test_types, CORE_LONGEST_JOB_MINUTES_DEFAULT),
                )
            )
        groups.append(EstimatedJobGroup("Integration and system tests", *INTEGRATION_ESTIMATE))
    # Like the provider test jobs of ci-amd.yml, which all need run-unit-tests.
    if selective_checks.run_unit_tests and not selective_checks.skip_providers_tests:
        providers_group = estimate_providers_group(
            selective_checks._get_providers_test_types_to_run(split_to_individual_providers=True)
        )
        if providers_group:
            groups.append(providers_group)
    if selective_checks.run_mypy_providers:
        groups.append(EstimatedJobGroup("Provider mypy checks", *MYPY_PROVIDERS_ESTIMATE))
    if selective_checks.full_tests_needed:
        groups.append(EstimatedJobGroup("Special tests", *SPECIAL_TESTS_ESTIMATE))
    optional_groups = (
        ("prod_image_build", "PROD image build and tests", PROD_IMAGE_ESTIMATE),
        ("run_kubernetes_tests", "Kubernetes tests", KUBERNETES_ESTIMATE),
        ("run_helm_tests", "Helm tests", HELM_ESTIMATE),
        ("run_kustomize_overlays_tests", "Kustomize overlays tests", KUSTOMIZE_ESTIMATE),
        ("run_task_sdk_tests", "Task SDK tests", TASK_SDK_ESTIMATE),
        ("run_agent_framework_tests", "Agent framework tests", AGENT_FRAMEWORK_ESTIMATE),
    )
    for flag, name, estimate in optional_groups:
        if getattr(selective_checks, flag):
            groups.append(EstimatedJobGroup(name, *estimate))
    groups.extend(estimate_image_independent_groups(selective_checks))
    return CiMinutesEstimate(groups)
