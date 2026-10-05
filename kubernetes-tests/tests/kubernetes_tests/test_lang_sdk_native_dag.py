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
End-to-end tests of Dags declared entirely in a language SDK (no Python file at all) on
KubernetesExecutor.

The Dag processor parses each native Dag through the coordinator its language routes to,
from a Dag bundle holding only that language's artifact (``lang-sdk-native-java`` /
``lang-sdk-native-ts``). The same coordinators also serve the mixed-language Dag's stub
tasks (see ``test_lang_sdk_mixed_language.py``); a native task ignores
``task_handler_bundle_name`` and reads its own Dag bundle instead, so one coordinator per
language is enough -- no ``[sdk] dag_bundle_to_coordinator`` is configured.

Prerequisites are provisioned by ``breeze k8s setup-lang-sdk-test``.
"""

from __future__ import annotations

import os

import pytest

from kubernetes_tests.test_base import EXECUTOR, BaseK8STest

_RUN_LANG_SDK = os.environ.get("RUN_LANG_SDK_K8S_TESTS", "").lower() in ("true", "1")
_TIMEOUT = 600

_skip_unless_lang_sdk = pytest.mark.skipif(
    EXECUTOR != "KubernetesExecutor" or not _RUN_LANG_SDK,
    reason="Runs only on KubernetesExecutor with the lang-SDK env provisioned (RUN_LANG_SDK_K8S_TESTS)",
)


@pytest.mark.skip(
    reason="Native Go Dag parsing is not supported yet: there is no Go Dag importer, and "
    "ExecutableCoordinator has no parse-Dag command. Unskip once a Go bundle can answer a "
    "Dag-parse request."
)
class TestNativeGoDagOnKubernetes(BaseK8STest):
    def test_native_go_dag_succeeds(self):
        pass


@_skip_unless_lang_sdk
class TestNativeJavaDagOnKubernetes(BaseK8STest):
    """Declared entirely in Java; the interface API half of ``airflow-e2e-tests/java-native-bundle``.

    Covers the path a Dag with no Python file takes: the Dag processor parses the
    ``airflow-e2e-java-native-bundle`` jar through the Java coordinator, the scheduler reads
    the serialized Dag, and each task runs in its own pod on the ``java-native`` queue.
    """

    DAG_ID = "java_native_e2e"
    TASK_IDS = ["extract", "transform", "load"]

    @pytest.mark.execution_timeout(900)
    def test_native_java_dag_succeeds(self):
        dag_run_id, logical_date = self.start_job_in_kubernetes(self.DAG_ID, self.host)
        print(f"Triggered {self.DAG_ID} run {dag_run_id} (logical_date={logical_date})")

        for task_id in self.TASK_IDS:
            self.monitor_task(
                host=self.host,
                dag_run_id=dag_run_id,
                dag_id=self.DAG_ID,
                task_id=task_id,
                expected_final_state="success",
                timeout=_TIMEOUT,
            )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=self.DAG_ID,
            expected_final_state="success",
            timeout=_TIMEOUT,
        )


@_skip_unless_lang_sdk
class TestNativeTypeScriptDagOnKubernetes(BaseK8STest):
    """The same Dag the SDK and Airflow e2e suites run, under the Node coordinator.

    Covers the path a Dag with no Python file takes: the Dag processor parses the
    ``airflow-ts-pack`` bundle through the coordinator, the scheduler reads the serialized Dag,
    and each task runs in its own pod. The graph is a real one -- a task group, a named fan-in,
    order-only edges, a conditional and a multi-way branch -- so pod-per-task scheduling is
    exercised against branches that skip rather than a linear chain. ``trigger_downstream`` defers
    to ``DagStateTrigger``, which the Python triggerer runs, and resumes in a pod of its own.
    """

    DAG_ID = "typescript_native_example"
    # The Dag its trigger_downstream task starts; see ts-sdk/example/src/native.ts.
    DOWNSTREAM_DAG_ID = "typescript_example"
    TASK_IDS = [
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

    def _ensure_variable(self, key: str, value: str) -> None:
        resp = self.session.post(f"http://{self.host}/variables", json={"key": key, "value": value})
        assert resp.status_code in (200, 201, 409), f"Could not create variable {key}: {resp.text}"

    def _unpause_dag(self, dag_id: str) -> None:
        resp = self.session.patch(f"http://{self.host}/dags/{dag_id}", json={"is_paused": False})
        assert resp.status_code == 200, f"Could not un-pause {dag_id}: {resp.text}"

    @pytest.mark.execution_timeout(1200)
    def test_native_typescript_dag_succeeds(self):
        # Both regions non-empty, so the conditional takes its `then` branch and
        # `report_empty` is the side that skips. The cadence names the case the
        # multi-way branch chooses, so `publish_daily` is the side that skips.
        self._ensure_variable("typescript_native_north_rows", "3")
        self._ensure_variable("typescript_native_south_rows", "2")
        self._ensure_variable("typescript_native_cadence", "weekly")
        # trigger_downstream waits for the run it starts, which stays queued while its Dag is paused.
        self._unpause_dag(self.DOWNSTREAM_DAG_ID)

        dag_run_id, logical_date = self.start_job_in_kubernetes(self.DAG_ID, self.host)
        print(f"Triggered {self.DAG_ID} run {dag_run_id} (logical_date={logical_date})")

        for task_id in self.TASK_IDS:
            self.monitor_task(
                host=self.host,
                dag_run_id=dag_run_id,
                dag_id=self.DAG_ID,
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
                dag_id=self.DAG_ID,
                task_id=skipped,
                expected_final_state="skipped",
                timeout=_TIMEOUT,
            )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=self.DAG_ID,
            expected_final_state="success",
            timeout=_TIMEOUT,
        )
