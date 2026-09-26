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
from subprocess import CompletedProcess
from unittest import mock

import pytest

from airflow_breeze.commands import kubernetes_kustomize_commands as overlay_commands
from airflow_breeze.commands.kubernetes_kustomize_commands import (
    ALLOWED_OVERLAY_IMAGES,
    _discover_overlay_images,
    _find_disallowed_overlay_images,
    _get_pod_health_problems,
    _smoke_test_overlay_impl,
    _wait_for_airflow_pods,
)


class TestDiscoverOverlayImages:
    def test_collects_images_across_nested_pod_specs(self):
        manifest = """
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      initContainers:
        - name: init
          image: alpine/k8s:1.31.0
      containers:
        - name: kdc
          image: gcavalcante8808/krb5-server:latest
---
apiVersion: batch/v1
kind: Job
spec:
  template:
    spec:
      containers:
        - name: bootstrap
          image: alpine/k8s:1.31.0
"""
        assert _discover_overlay_images(manifest) == [
            "alpine/k8s:1.31.0",
            "gcavalcante8808/krb5-server:latest",
        ]

    def test_returns_empty_list_when_no_images(self):
        assert _discover_overlay_images("kind: ConfigMap\ndata:\n  key: value\n") == []


class TestFindDisallowedOverlayImages:
    def test_allowed_images_yield_no_findings(self):
        assert _find_disallowed_overlay_images(sorted(ALLOWED_OVERLAY_IMAGES)) == []

    @pytest.mark.parametrize(
        ("images", "expected"),
        [
            (["evil/backdoor:latest"], ["evil/backdoor:latest"]),
            (
                ["alpine/k8s:1.31.0", "evil/backdoor:latest"],
                ["evil/backdoor:latest"],
            ),
            # A different tag of an allow-listed repo is still not allowed (pinned exact ref).
            (["alpine/k8s:9.9.9"], ["alpine/k8s:9.9.9"]),
        ],
    )
    def test_flags_images_not_on_allow_list(self, images, expected):
        assert _find_disallowed_overlay_images(images) == expected


def build_pod(name="scheduler", phase="Running", ready="True", terminating=False):
    metadata = {"name": name}
    if terminating:
        metadata["deletionTimestamp"] = "2026-01-01T00:00:00Z"
    return {
        "metadata": metadata,
        "status": {"phase": phase, "conditions": [{"type": "Ready", "status": ready}]},
    }


@pytest.mark.parametrize(
    ("pod", "expected"),
    [
        (build_pod(), []),
        (build_pod(phase="Succeeded", ready="False"), []),
        (build_pod(phase="Pending", ready="False"), ["scheduler: Pending"]),
        (build_pod(phase="Failed", ready="False"), ["scheduler: Failed"]),
        (build_pod(ready="False"), ["scheduler: not Ready"]),
        (build_pod(ready="Unknown"), ["scheduler: not Ready"]),
        (build_pod(terminating=True), ["scheduler: terminating"]),
        ({"metadata": {"name": "scheduler"}}, ["scheduler: Unknown"]),
        (
            {"metadata": {"name": "scheduler"}, "status": {"phase": "Running"}},
            ["scheduler: not Ready"],
        ),
    ],
)
def test_get_pod_health_problems(pod, expected):
    assert _get_pod_health_problems([pod]) == expected


class TestWaitForAirflowPods:
    @mock.patch.object(overlay_commands, "run_command", autospec=True)
    def test_checks_release_in_namespace_and_allows_completed_jobs(self, run_command):
        run_command.return_value = CompletedProcess(
            [], 0, stdout=json.dumps({"items": [build_pod(), build_pod("migrate", "Succeeded", "False")]})
        )
        env = {"KUBECONFIG": "/test/kubeconfig"}

        assert _wait_for_airflow_pods("custom-namespace", "custom-release", 30, env) == 0

        run_command.assert_called_once_with(
            [
                str(overlay_commands.KUBECTL_BIN_PATH),
                "get",
                "pods",
                "-n",
                "custom-namespace",
                "-l",
                "release=custom-release",
                "-o",
                "json",
                "--request-timeout=10s",
            ],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

    @pytest.mark.parametrize(
        "initial_pods",
        [[], [build_pod(phase="Pending")], [build_pod(ready="False")], [build_pod(terminating=True)]],
    )
    @mock.patch.object(overlay_commands.time, "sleep", autospec=True)
    @mock.patch.object(overlay_commands, "run_command", autospec=True)
    def test_waits_for_current_pods_to_be_ready(self, run_command, sleep, initial_pods):
        run_command.side_effect = [
            CompletedProcess([], 0, stdout=json.dumps({"items": initial_pods})),
            CompletedProcess([], 0, stdout=json.dumps({"items": [build_pod("replacement-scheduler")]})),
        ]

        assert _wait_for_airflow_pods("airflow", "airflow", 30, {}) == 0
        assert run_command.call_count == 2
        sleep.assert_called_once()

    @pytest.mark.parametrize(
        "pods",
        [
            [],
            [build_pod("migrate", "Succeeded", "False")],
            [build_pod(phase="Failed")],
            [build_pod(ready="False")],
            [build_pod(), build_pod("worker", ready="False")],
        ],
    )
    @mock.patch.object(overlay_commands.time, "monotonic", autospec=True, side_effect=[0, 0, 1, 1])
    @mock.patch.object(overlay_commands.time, "sleep", autospec=True)
    @mock.patch.object(overlay_commands, "run_command", autospec=True)
    def test_timeout_fails_and_dumps_scoped_diagnostics(self, run_command, sleep, monotonic, pods):
        run_command.return_value = CompletedProcess([], 0, stdout=json.dumps({"items": pods}))

        assert _wait_for_airflow_pods("test-ns", "test-release", 1, {}) == 1

        kubectl = str(overlay_commands.KUBECTL_BIN_PATH)
        scope = ["-n", "test-ns", "-l", "release=test-release"]
        assert run_command.call_args_list[1:] == [
            mock.call([kubectl, "get", "pods", *scope, "-o", "wide"], env={}, check=False),
            mock.call([kubectl, "describe", "pods", *scope], env={}, check=False),
        ]

    @mock.patch.object(overlay_commands, "run_command", autospec=True)
    def test_query_failure_fails_health_check(self, run_command):
        run_command.return_value = CompletedProcess([], 1, stdout="", stderr="Forbidden")

        assert _wait_for_airflow_pods("airflow", "airflow", 30, {}) == 1
        run_command.assert_called_once()


@pytest.mark.parametrize("health_result", [0, 1])
@pytest.mark.parametrize("no_pytest", [False, True])
@pytest.mark.parametrize("skip_cleanup", [False, True])
@mock.patch.object(overlay_commands, "get_k8s_env", autospec=True, return_value={})
@mock.patch.object(overlay_commands, "_render_overlay", autospec=True, return_value="manifest")
@mock.patch.object(overlay_commands, "_preload_overlay_images", autospec=True, return_value=0)
@mock.patch.object(overlay_commands, "_apply_or_delete_overlay", autospec=True, return_value=0)
@mock.patch.object(overlay_commands, "_wait_for_verify_resource", autospec=True, return_value=0)
@mock.patch.object(overlay_commands, "_wait_for_airflow_pods", autospec=True)
@mock.patch.object(overlay_commands, "_run_overlay_pytest", autospec=True, return_value=0)
def test_smoke_test_requires_healthy_airflow(
    run_pytest,
    wait_for_airflow,
    wait_for_resource,
    apply_or_delete,
    preload,
    render,
    get_env,
    skip_cleanup,
    no_pytest,
    health_result,
    tmp_path,
):
    wait_for_airflow.return_value = health_result
    calls = mock.Mock(spec=["verify", "health", "apply_or_delete", "pytest"])
    calls.attach_mock(wait_for_resource, "verify")
    calls.attach_mock(wait_for_airflow, "health")
    calls.attach_mock(apply_or_delete, "apply_or_delete")
    calls.attach_mock(run_pytest, "pytest")

    result = _smoke_test_overlay_impl(
        overlay_name="kerberos",
        overlay_dir=tmp_path,
        verify={"timeout_seconds": 30, "resources": [{"kind": "Secret", "name": "keytab"}]},
        python="3.10",
        kubernetes_version="1.35.0",
        executor="CeleryExecutor",
        release_name="test-release",
        namespace="test-ns",
        skip_cleanup=skip_cleanup,
        no_pytest=no_pytest,
    )

    assert result == health_result
    wait_for_airflow.assert_called_once_with("test-ns", "test-release", 30, {})
    expected_order = ["apply_or_delete", "verify", "health"]
    if health_result == 0 and not no_pytest:
        expected_order.append("pytest")
    if not skip_cleanup:
        expected_order.append("apply_or_delete")
        assert apply_or_delete.call_args == mock.call("delete", "manifest", "test-ns", {})
    assert [call[0] for call in calls.mock_calls] == expected_order
