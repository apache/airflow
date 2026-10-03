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
End-to-end tests of the lang-SDK coordinators on KubernetesExecutor.

``test_lang_sdk_combined_dag_succeeds`` triggers the ``lang_sdk_combined`` Dag (Python + Go + Java
tasks in one graph) and asserts every task instance and the Dag run reach ``success``. The Dag processor
binds each stub task to the artifact that registers its handler, the scheduler sends the task with that
artifact, the queue (``golang`` or ``java``) picks the coordinator's ``pod_template_file`` for the pod, and
the worker runs the bound file. A Go pod stages the Go binary from localstack S3 with an init container,
and a Java pod downloads the Java jar as an S3 Dag bundle. Neither coordinator runs ``lang_sdk_combined.py``
itself, so the tasks succeed only if the artifact reached the pod.

``test_a_task_routed_to_a_coordinator_without_an_artifact_fails_with_its_reason`` triggers
``lang_sdk_misrouted``, a Python task on the ``golang`` queue. No artifact is bound to it, so the worker
fails the task, and the test reads the reason from its state reason.

Prerequisites are provisioned by ``breeze k8s setup-lang-sdk-test``.
"""

from __future__ import annotations

import os
import time

import pytest

from kubernetes_tests.test_base import EXECUTOR, BaseK8STest

_RUN_LANG_SDK = os.environ.get("RUN_LANG_SDK_K8S_TESTS", "").lower() in ("true", "1")

DAG_ID = "lang_sdk_combined"
# The Dag bundle the stub Dags are uploaded to (kubernetes-tests/lang_sdk/config/values.yaml).
STUB_DAG_BUNDLE = "lang-sdk-dags"
MISROUTED_DAG_ID = "lang_sdk_misrouted"
MISROUTED_TASK_ID = "python_task_on_golang_queue"
# Why the worker fails a task whose queue is routed to a coordinator and whose workload names no artifact:
# the task, its Dag file and its queue.
MISROUTED_REASON = (
    f"Task '{MISROUTED_TASK_ID}' of Dag '{MISROUTED_DAG_ID}' has no task handler artifact, and its Dag file "
    f"'{MISROUTED_DAG_ID}.py' is not an artifact that ExecutableCoordinator runs. "
    "Queue 'golang' routes it to a Lang-SDK coordinator"
)
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
# How long a test waits for the Dag processor to import its Dag file. The execution timeout of each test
# adds it, so that the wait reports the import errors before pytest times the test out.
_IMPORT_TIMEOUT = 600


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

    def _wait_until_dag_imports(self, dag_id: str, timeout: int = _IMPORT_TIMEOUT) -> None:
        """Wait until the Dag processor has imported the file of *dag_id*, or fail with its import errors."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            response = self.session.get(
                f"http://{self.host}/dags",
                params={"bundle_name": STUB_DAG_BUNDLE, "dag_id_pattern": dag_id, "exclude_stale": "false"},
            )
            response.raise_for_status()
            dags = response.json()["dags"]
            if any(d["dag_id"] == dag_id and not d["has_import_errors"] and not d["is_stale"] for d in dags):
                return
            time.sleep(10)
        errors = self.session.get(f"http://{self.host}/importErrors", params={"bundle_name": STUB_DAG_BUNDLE})
        pytest.fail(f"{dag_id} did not import on the Dag processor: {errors.json()['import_errors']}")

    @pytest.mark.execution_timeout(_IMPORT_TIMEOUT + 900)
    def test_lang_sdk_combined_dag_succeeds(self):
        self._ensure_variable("my_variable", "value_from_test")
        self._wait_until_dag_imports(DAG_ID)

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

    @pytest.mark.execution_timeout(_IMPORT_TIMEOUT + 600)
    def test_a_task_routed_to_a_coordinator_without_an_artifact_fails_with_its_reason(self):
        self._wait_until_dag_imports(MISROUTED_DAG_ID)

        dag_run_id, logical_date = self.start_job_in_kubernetes(MISROUTED_DAG_ID, self.host)
        print(f"Triggered {MISROUTED_DAG_ID} run {dag_run_id} (logical_date={logical_date})")

        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=MISROUTED_DAG_ID,
            task_id=MISROUTED_TASK_ID,
            expected_final_state="failed",
            timeout=_TIMEOUT,
        )
        task_instance = self.session.get(
            f"http://{self.host}/dags/{MISROUTED_DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{MISROUTED_TASK_ID}"
        ).json()
        # The worker failed it, not the scheduler, and recorded why.
        assert MISROUTED_REASON in (task_instance["state_reason"] or ""), task_instance

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=MISROUTED_DAG_ID,
            expected_final_state="failed",
            timeout=_TIMEOUT,
        )
