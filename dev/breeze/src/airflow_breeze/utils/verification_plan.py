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
import shlex
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow_breeze.utils.selective_checks import SelectiveChecks


@dataclass(frozen=True)
class VerificationItem:
    kind: str
    command: str
    runs_in: str


# SelectiveChecks flag -> (kind, command, runs_in) of the CI job(s) that flag gates.
# Mirrors .github/workflows/ci-amd.yml and basic-tests.yml; "breeze" means the command needs
# Docker and the CI image, "host" means only host tooling.
FLAG_COMMANDS: dict[str, tuple[tuple[str, str, str], ...]] = {
    "run_mypy_providers": (("mypy", "prek run --stage manual mypy-providers --all-files", "breeze"),),
    "has_migrations": (("schema", "prek run --stage manual migration-round-trip --all-files", "breeze"),),
    "run_task_sdk_tests": (("unit", "breeze testing task-sdk-tests", "breeze"),),
    "run_airflow_ctl_tests": (("unit", "breeze testing airflow-ctl-tests", "breeze"),),
    "run_scripts_tests": (("unit", "uv run --project scripts pytest scripts/tests/", "host"),),
    "run_ui_tests": (
        ("ui", "cd airflow-core/src/airflow/ui && pnpm install --frozen-lockfile && pnpm test", "host"),
        (
            "ui",
            "cd airflow-core/src/airflow/api_fastapi/auth/managers/simple/ui && pnpm install --frozen-lockfile && pnpm test",
            "host",
        ),
    ),
    "run_api_codegen": (("schema", "breeze testing python-api-client-tests", "breeze"),),
    "run_go_sdk_tests": (("unit", "cd go-sdk && go test ./...", "host"),),
    "run_java_sdk_tests": (
        ("unit", "cd java-sdk && ./gradlew test", "host"),
        ("docs", "breeze build-docs --sdk-docs-only --sdk=java", "breeze"),
    ),
    "run_ts_sdk_docs": (("docs", "breeze build-docs --sdk-docs-only --sdk=typescript", "breeze"),),
    "run_breeze_integration_tests": (
        ("unit", "cd dev/breeze && uv run --locked pytest", "host"),
        ("unit", "cd dev/breeze && uv run --locked pytest -m integration_tests", "host"),
    ),
}

# Gated CI jobs that need a cluster, a prod image or an e2e stack; classified only, never printed.
NOT_RUNNABLE_LOCALLY: frozenset[str] = frozenset(
    {
        "run_kubernetes_tests",
        "run_helm_tests",
        "run_kustomize_overlays_tests",
        "run_ui_e2e_tests",
        "run_task_sdk_integration_tests",
        "run_airflow_ctl_integration_tests",
        "run_system_tests",
        "run_remote_logging_s3_e2e_tests",
        "run_remote_logging_elasticsearch_e2e_tests",
        "run_remote_logging_opensearch_e2e_tests",
        "run_event_driven_e2e_tests",
        "run_java_sdk_e2e_tests",
        "run_go_sdk_e2e_tests",
        "run_openlineage_e2e_tests",
        "run_openlineage_e2e_compat_tests",
        "run_ts_sdk_e2e_tests",
    }
)


def build_prek_item(sc: SelectiveChecks, base_ref: str) -> VerificationItem:
    if sc.basic_checks_only:
        command = f"SKIP_BREEZE_PREK_HOOKS=true SKIP={sc.skip_prek_hooks} prek run --from-ref {shlex.quote(base_ref)} --to-ref HEAD"
        return VerificationItem("prek", command, "host")
    return VerificationItem("prek", f"SKIP={sc.skip_prek_hooks} prek run --all-files", "breeze")


def build_unit_test_items(group: str, test_types_json: str | None) -> list[VerificationItem]:
    test_types = " ".join(chunk["test_types"] for chunk in json.loads(test_types_json or "null") or [])
    if not test_types:
        return []
    prefix = f"breeze testing {group}-tests"
    return [
        VerificationItem(
            "unit",
            f'{prefix} --run-in-parallel --run-db-tests-only --parallel-test-types "{test_types}"',
            "breeze",
        ),
        VerificationItem(
            "unit",
            f'{prefix} --use-xdist --skip-db-tests --no-db-cleanup --backend none --parallel-test-types "{test_types}"',
            "breeze",
        ),
    ]


def build_local_verification_plan(
    sc: SelectiveChecks, changed_files: tuple[str, ...], base_ref: str
) -> dict[str, Any]:
    items = [build_prek_item(sc, base_ref)]
    if sc.run_unit_tests:
        items += build_unit_test_items("core", sc.core_test_types_list_as_strings_in_json)
    if not sc.skip_providers_tests:
        items += build_unit_test_items("providers", sc.providers_test_types_list_as_strings_in_json)
    for flag, commands in FLAG_COMMANDS.items():
        if getattr(sc, flag):
            items += [VerificationItem(*command) for command in commands]
    if sc.docs_build:
        items.append(
            VerificationItem("docs", f"breeze build-docs {sc.docs_list_as_string}".rstrip(), "breeze")
        )
    return {
        "base_ref": base_ref,
        "default_python_version": sc.default_python_version,
        "full_tests_needed": sc.full_tests_needed,
        "changed_files": list(changed_files),
        "items": [asdict(item) for item in items],
    }
