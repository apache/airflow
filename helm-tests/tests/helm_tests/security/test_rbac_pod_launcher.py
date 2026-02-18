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

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


class TestPodLauncher:
    """Tests RBAC Pod Launcher."""

    @pytest.mark.parametrize(
        (
            "rbac_create",
            "allow_pod_launching",
            "multi_ns",
            "expected_kind",
            "expected_name",
            "expected_secret_verbs",
        ),
        [
            (True, True, False, "Role", "release-name-pod-launcher-role", {"create", "get", "delete"}),
            (
                True,
                True,
                True,
                "ClusterRole",
                "default-release-name-pod-launcher-role",
                {"create", "get", "delete"},
            ),
            (True, False, False, None, None, None),
            (False, True, False, None, None, None),
        ],
    )
    def test_pod_launcher_role(
        self, rbac_create, allow_pod_launching, multi_ns, expected_kind, expected_name, expected_secret_verbs
    ):
        docs = render_chart(
            values={
                "rbac": {"create": rbac_create},
                "allowPodLaunching": allow_pod_launching,
                "multiNamespaceMode": multi_ns,
            },
            show_only=["templates/rbac/pod-launcher-role.yaml"],
        )
        if expected_kind is None:
            assert docs == []
        else:
            assert docs[0]["kind"] == expected_kind
            assert docs[0]["metadata"]["name"] == expected_name
            rules = jmespath.search("rules", docs[0])
            secrets_rule = next((r for r in rules if "secrets" in r.get("resources", [])), None)
            assert secrets_rule is not None
            assert set(secrets_rule["verbs"]) == expected_secret_verbs

    @pytest.mark.parametrize(
        (
            "rbac_create",
            "allow_pod_launching",
            "executor",
            "triggerer_enabled",
            "multi_ns",
            "expected_subjects",
        ),
        [
            # Only scheduler and worker SAs for KubernetesExecutor, CeleryExecutor
            (
                True,
                True,
                "CeleryExecutor,KubernetesExecutor",
                False,
                False,
                ["release-name-airflow-scheduler", "release-name-airflow-worker"],
            ),
            # Add triggerer SA if enabled
            (
                True,
                True,
                "CeleryExecutor,KubernetesExecutor",
                True,
                False,
                [
                    "release-name-airflow-scheduler",
                    "release-name-airflow-worker",
                    "release-name-airflow-triggerer",
                ],
            ),
            # RoleBinding not created if allowPodLaunching is False
            (True, False, "CeleryExecutor,KubernetesExecutor", False, False, []),
            # RoleBinding not created if rbac.create is False
            (False, True, "CeleryExecutor,KubernetesExecutor", False, False, []),
        ],
    )
    def test_pod_launcher_rolebinding(
        self,
        rbac_create,
        allow_pod_launching,
        executor,
        triggerer_enabled,
        multi_ns,
        expected_subjects,
    ):
        docs = render_chart(
            values={
                "rbac": {"create": rbac_create},
                "allowPodLaunching": allow_pod_launching,
                "executor": executor,
                "triggerer": {"enabled": triggerer_enabled},
                "multiNamespaceMode": multi_ns,
            },
            show_only=["templates/rbac/pod-launcher-rolebinding.yaml"],
        )
        if not (rbac_create and allow_pod_launching):
            assert docs == []
        else:
            actual = jmespath.search("subjects[*].name", docs[0]) if docs else []
            assert sorted(actual) == sorted(expected_subjects)
