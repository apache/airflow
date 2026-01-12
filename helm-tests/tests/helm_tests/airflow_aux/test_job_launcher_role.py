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


class TestJobLauncher:
    """Tests job launcher RBAC."""

    @pytest.mark.parametrize(
        ("executor", "rbac", "allow", "expected_accounts"),
        [
            ("CeleryKubernetesExecutor", True, True, ["scheduler", "worker"]),
            ("KubernetesExecutor", True, True, ["scheduler", "worker"]),
            ("CeleryExecutor", True, True, ["worker"]),
            ("LocalExecutor", True, True, ["scheduler"]),
            ("LocalExecutor", False, False, []),
            ("CeleryExecutor,KubernetesExecutor", True, True, ["scheduler", "worker"]),
        ],
    )
    def test_job_launcher_rolebinding(self, executor, rbac, allow, expected_accounts):
        docs = render_chart(
            values={
                "rbac": {"create": rbac},
                "allowJobLaunching": allow,
                "executor": executor,
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )
        if expected_accounts:
            for idx, suffix in enumerate(expected_accounts):
                assert f"release-name-airflow-{suffix}" == jmespath.search(f"subjects[{idx}].name", docs[0])
        else:
            assert docs == []

    @pytest.mark.parametrize(
        ("multiNamespaceMode", "namespace", "expectedRole", "expectedRoleBinding"),
        [
            (
                True,
                "namespace",
                "namespace-release-name-job-launcher-role",
                "namespace-release-name-job-launcher-rolebinding",
            ),
            (
                True,
                "other-ns",
                "other-ns-release-name-job-launcher-role",
                "other-ns-release-name-job-launcher-rolebinding",
            ),
            (False, "namespace", "release-name-job-launcher-role", "release-name-job-launcher-rolebinding"),
        ],
    )
    def test_job_launcher_rolebinding_multi_namespace(
        self, multiNamespaceMode, namespace, expectedRole, expectedRoleBinding
    ):
        docs = render_chart(
            namespace=namespace,
            values={"allowJobLaunching": True, "multiNamespaceMode": multiNamespaceMode},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        actualRoleBinding = jmespath.search("metadata.name", docs[0])
        assert actualRoleBinding == expectedRoleBinding

        actualRoleRef = jmespath.search("roleRef.name", docs[0])
        assert actualRoleRef == expectedRole

        actualKind = jmespath.search("kind", docs[0])
        actualRoleRefKind = jmespath.search("roleRef.kind", docs[0])
        if multiNamespaceMode:
            assert actualKind == "ClusterRoleBinding"
            assert actualRoleRefKind == "ClusterRole"
        else:
            assert actualKind == "RoleBinding"
            assert actualRoleRefKind == "Role"

    @pytest.mark.parametrize(
        ("multiNamespaceMode", "namespace", "expectedRole"),
        [
            (True, "namespace", "namespace-release-name-job-launcher-role"),
            (True, "other-ns", "other-ns-release-name-job-launcher-role"),
            (False, "namespace", "release-name-job-launcher-role"),
        ],
    )
    def test_job_launcher_role_multi_namespace(self, multiNamespaceMode, namespace, expectedRole):
        docs = render_chart(
            namespace=namespace,
            values={"allowJobLaunching": True, "multiNamespaceMode": multiNamespaceMode},
            show_only=["templates/rbac/job-launcher-role.yaml"],
        )

        actualRole = jmespath.search("metadata.name", docs[0])
        assert actualRole == expectedRole

        actualKind = jmespath.search("kind", docs[0])
        if multiNamespaceMode:
            assert actualKind == "ClusterRole"
        else:
            assert actualKind == "Role"
