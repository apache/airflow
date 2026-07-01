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
