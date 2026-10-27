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

from airflow_breeze.commands import kubernetes_kustomize_commands
from airflow_breeze.commands.kubernetes_kustomize_commands import (
    ALLOWED_OVERLAY_IMAGES,
    _discover_overlay_images,
    _find_disallowed_overlay_images,
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


@mock.patch.object(kubernetes_kustomize_commands, "run_command", autospec=True)
@mock.patch.object(kubernetes_kustomize_commands, "get_k8s_env", autospec=True, return_value={})
def test_run_overlay_pytest_does_not_relock(mock_get_k8s_env, mock_run_command):
    mock_run_command.return_value = mock.Mock(returncode=0)

    kubernetes_kustomize_commands._run_overlay_pytest(
        overlay_name="kerberos",
        namespace="airflow",
        release_name="airflow",
        python="3.11",
        kubernetes_version="v1.31.12",
        executor="LocalExecutor",
    )

    assert mock_run_command.call_args.args[0][:4] == ["uv", "run", "--frozen", "pytest"]
