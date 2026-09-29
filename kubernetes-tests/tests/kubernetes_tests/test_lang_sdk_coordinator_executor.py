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
End-to-end test of the lang-SDK coordinators on KubernetesExecutor.

Triggers the ``lang_sdk_combined`` Dag (Python + Go + Java tasks in one graph)
and asserts every task instance and the Dag run reach ``success``. This exercises
the full path the worktree-1 feature enables: the ``golang``/``java`` queues are
routed to their coordinators, each coordinator's ``pod_template_file`` launches a
worker pod whose init-container stages the artifact from localstack S3 via the
DagBundle interface, and the coordinator then runs the Go binary / Java jar.

Prerequisites are provisioned by ``breeze k8s setup-lang-sdk-test``.
"""

from __future__ import annotations

import os

import pytest

from kubernetes_tests.test_base import EXECUTOR, BaseK8STest

_RUN_LANG_SDK = os.environ.get("RUN_LANG_SDK_K8S_TESTS", "").lower() in ("true", "1")

DAG_ID = "lang_sdk_combined"
# Declared entirely in TypeScript: the Dag processor asks the bundle to parse
# itself, so no Python file in the bundles folder mentions it.
NATIVE_TS_DAG_ID = "typescript_native_example"
NATIVE_TS_TASK_IDS = [
    "extract.north",
    "extract.south",
    "summarize",
    "has_rows",
    "load_rows",
    "pick_cadence",
    "publish_weekly",
    "cleanup",
    "trigger_downstream",
]
TASK_IDS = [
    "python_task_1",
    "go_extract",
    "go_transform",
    "java_extract",
    "java_transform",
    "python_task_2",
]
# Each task is a fresh pod (KubernetesExecutor) and the lang tasks also pull an
# artifact + start a coordinator subprocess, so allow generous headroom.
_TIMEOUT = 600

# The native Dag additionally needs a Dag processor that dispatches a parse
# request to the Node coordinator; until that lands the Dag never appears.
_RUN_NATIVE_TS = os.environ.get("RUN_TS_SDK_NATIVE_DAG_K8S_TESTS", "").lower() in ("true", "1")


@pytest.mark.skipif(
    EXECUTOR != "KubernetesExecutor" or not _RUN_LANG_SDK,
    reason="Runs only on KubernetesExecutor with the lang-SDK env provisioned (RUN_LANG_SDK_K8S_TESTS)",
)
class TestLangSdkCoordinatorExecutor(BaseK8STest):
    def _ensure_variable(self, key: str, value: str) -> None:
        """Create the Airflow Variable the Go/Java transform tasks read (idempotent)."""
        resp = self.session.post(f"http://{self.host}/variables", json={"key": key, "value": value})
        # 409 == already exists from a previous run; both are acceptable.
        assert resp.status_code in (200, 201, 409), f"Could not create variable {key}: {resp.text}"

    @pytest.mark.execution_timeout(900)
    def test_lang_sdk_combined_dag_succeeds(self):
        self._ensure_variable("my_variable", "value_from_test")

        dag_run_id, logical_date = self.start_job_in_kubernetes(DAG_ID, self.host)
        print(f"Triggered {DAG_ID} run {dag_run_id} (logical_date={logical_date})")

        for task_id in TASK_IDS:
            self.monitor_task(
                host=self.host,
                dag_run_id=dag_run_id,
                dag_id=DAG_ID,
                task_id=task_id,
                expected_final_state="success",
                timeout=_TIMEOUT,
            )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=DAG_ID,
            expected_final_state="success",
            timeout=_TIMEOUT,
        )


@pytest.mark.skipif(
    EXECUTOR != "KubernetesExecutor" or not _RUN_NATIVE_TS,
    reason="Runs only on KubernetesExecutor with a Dag processor that parses Lang SDK bundles "
    "(RUN_TS_SDK_NATIVE_DAG_K8S_TESTS)",
)
class TestNativeTypeScriptDagOnKubernetes(BaseK8STest):
    """The same Dag the SDK and Airflow e2e suites run, under the Node coordinator.

    Covers the path a Dag with no Python file takes: the Dag processor parses the
    ``airflow-ts-pack`` bundle through the coordinator, the scheduler reads the serialized Dag,
    and each task runs in its own pod. The graph is a real one -- a task group, a named fan-in,
    order-only edges, a conditional and a multi-way branch -- so pod-per-task scheduling is
    exercised against branches that skip rather than a linear chain.
    """

    def _ensure_variable(self, key: str, value: str) -> None:
        resp = self.session.post(f"http://{self.host}/variables", json={"key": key, "value": value})
        assert resp.status_code in (200, 201, 409), f"Could not create variable {key}: {resp.text}"

    @pytest.mark.execution_timeout(1200)
    def test_native_typescript_dag_succeeds(self):
        # Both regions non-empty, so the conditional takes its `then` branch and
        # `report_empty` is the side that skips. The cadence names the case the
        # multi-way branch chooses, so `publish_daily` is the side that skips.
        self._ensure_variable("typescript_native_north_rows", "3")
        self._ensure_variable("typescript_native_south_rows", "2")
        self._ensure_variable("typescript_native_cadence", "weekly")

        dag_run_id, logical_date = self.start_job_in_kubernetes(NATIVE_TS_DAG_ID, self.host)
        print(f"Triggered {NATIVE_TS_DAG_ID} run {dag_run_id} (logical_date={logical_date})")

        for task_id in NATIVE_TS_TASK_IDS:
            self.monitor_task(
                host=self.host,
                dag_run_id=dag_run_id,
                dag_id=NATIVE_TS_DAG_ID,
                task_id=task_id,
                expected_final_state="success",
                timeout=_TIMEOUT,
            )

        # The branch that was not taken is skipped, not failed, which is what
        # proves the skip reached the supervisor rather than the task erroring.
        for skipped in ("report_empty", "publish_daily"):
            self.monitor_task(
                host=self.host,
                dag_run_id=dag_run_id,
                dag_id=NATIVE_TS_DAG_ID,
                task_id=skipped,
                expected_final_state="skipped",
                timeout=_TIMEOUT,
            )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=NATIVE_TS_DAG_ID,
            expected_final_state="success",
            timeout=_TIMEOUT,
        )
