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
    @pytest.mark.parametrize(
        "executor",
        ["LocalExecutor", "CeleryExecutor", "KubernetesExecutor", "CeleryExecutor,KubernetesExecutor"],
    )
    def test_should_render(self, executor):
        docs = render_chart(
            values={"rbac": {"create": True}, "allowJobLaunching": True, "executor": executor},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert len(docs) == 1

    @pytest.mark.parametrize(("rbac", "allow"), [(True, False), (False, True), (False, False)])
    @pytest.mark.parametrize(
        "executor",
        ["LocalExecutor", "CeleryExecutor", "KubernetesExecutor", "CeleryExecutor,KubernetesExecutor"],
    )
    def test_should_not_render(self, rbac, allow, executor):
        docs = render_chart(
            values={"rbac": {"create": rbac}, "allowJobLaunching": allow, "executor": executor},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert len(docs) == 0

    def test_multi_namespace_mode_disabled(self):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={"rbac": {"create": True}, "allowJobLaunching": True, "multiNamespaceMode": False},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "RoleBinding"

        role_ref = jmespath.search("roleRef", docs[0])
        assert role_ref["kind"] == "Role"
        assert role_ref["name"] == "prod-job-launcher-role"

        metadata = jmespath.search("metadata", docs[0])
        assert metadata["namespace"] == "airflow"
        assert metadata["name"] == "prod-job-launcher-rolebinding"
        assert "namespace" not in metadata["labels"]

    def test_multi_namespace_mode_enabled(self):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={"rbac": {"create": True}, "allowJobLaunching": True, "multiNamespaceMode": True},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "ClusterRoleBinding"

        role_ref = jmespath.search("roleRef", docs[0])
        assert role_ref["kind"] == "ClusterRole"
        assert role_ref["name"] == "airflow-prod-job-launcher-role"

        metadata = jmespath.search("metadata", docs[0])
        assert "namespace" not in metadata
        assert metadata["name"] == "airflow-prod-job-launcher-rolebinding"
        assert metadata["labels"]["namespace"] == "airflow"

    @pytest.mark.parametrize(
        "executor", ["LocalExecutor", "KubernetesExecutor", "KubernetesExecutor,LocalExecutor,CeleryExecutor"]
    )
    def test_scheduler_role_binding_should_exists(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": executor,
                "scheduler": {"enabled": True},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-scheduler'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-scheduler",
            "namespace": "airflow",
        }

    @pytest.mark.parametrize(
        ("executor", "enabled"),
        [
            ("CeleryExecutor", False),
            ("CeleryExecutor", True),
            ("KubernetesExecutor", False),
            ("LocalExecutor,CeleryExecutor", False),
        ],
    )
    def test_scheduler_role_binding_should_not_exists(self, executor, enabled):
        docs = render_chart(
            name="prod",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": executor,
                "scheduler": {"enabled": enabled},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-scheduler']", docs[0]) == []

    @pytest.mark.parametrize(
        "executor", ["CeleryExecutor", "KubernetesExecutor", "LocalExecutor,CeleryExecutor"]
    )
    def test_worker_role_binding_should_exists(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={"rbac": {"create": True}, "allowJobLaunching": True, "executor": executor},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-worker",
            "namespace": "airflow",
        }

    def test_worker_role_binding_should_not_exists(self):
        docs = render_chart(
            name="prod",
            values={"rbac": {"create": True}, "allowJobLaunching": True, "executor": "LocalExecutor"},
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker']", docs[0]) == []

    @pytest.mark.parametrize(
        "executor",
        [
            "CeleryExecutor",
            "CeleryExecutor,KubernetesExecutor",
            "LocalExecutor,CeleryExecutor,KubernetesExecutor",
        ],
    )
    @pytest.mark.parametrize("create", [False, True, None])
    def test_worker_role_binding_should_exists_with_kubernetes(self, executor, create):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": executor,
                "workers": {"kubernetes": {"serviceAccount": {"create": create}}},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-worker",
            "namespace": "airflow",
        }

    @pytest.mark.parametrize(
        "executor", ["KubernetesExecutor", "LocalExecutor", "LocalExecutor,KubernetesExecutor"]
    )
    @pytest.mark.parametrize("create", [False, True])
    def test_worker_role_binding_should_not_exists_with_kubernetes(self, executor, create):
        docs = render_chart(
            name="prod",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": executor,
                "workers": {"kubernetes": {"serviceAccount": {"create": create}}},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker']", docs[0]) == []

    @pytest.mark.parametrize(
        "executor",
        [
            "KubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
            "LocalExecutor,CeleryExecutor,KubernetesExecutor",
        ],
    )
    @pytest.mark.parametrize(
        "service_account_values",
        [
            {"create": True},
            {"name": "prod-airflow-worker-kubernetes"},
            {"create": False, "name": "prod-airflow-worker-kubernetes"},
        ],
    )
    def test_worker_kubernetes_role_binding_should_exists(self, executor, service_account_values):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": executor,
                "workers": {"kubernetes": {"serviceAccount": service_account_values}},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker-kubernetes'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-worker-kubernetes",
            "namespace": "airflow",
        }

    @pytest.mark.parametrize("executor", ["LocalExecutor", "CeleryExecutor", "KubernetesExecutor"])
    @pytest.mark.parametrize(
        "service_account_values",
        [{"create": False}, {"create": False, "name": None}, {"create": None, "name": None}, {}],
    )
    def test_worker_kubernetes_role_binding_should_not_exists(self, executor, service_account_values):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": executor,
                "workers": {"kubernetes": {"serviceAccount": service_account_values}},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker-kubernetes']", docs[0]) == []

    def test_no_role_bindings(self):
        docs = render_chart(
            name="prod",
            values={
                "rbac": {"create": True},
                "allowJobLaunching": True,
                "executor": "LocalExecutor",
                "scheduler": {"enabled": False},
            },
            show_only=["templates/rbac/job-launcher-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-scheduler']", docs[0]) is None
