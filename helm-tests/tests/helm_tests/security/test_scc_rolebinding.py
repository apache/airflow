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


class TestSCCActivation:
    """Tests SCCs."""

    def test_default_user_disabled_excludes_create_user_subject_airflow_2(self):
        """webserver.defaultUser.enabled=false should exclude the create-user-job service account."""
        docs = render_chart(
            values={
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "webserver": {
                    "defaultUser": {
                        "enabled": False,
                        "role": "Admin",
                        "username": "admin",
                        "email": "admin@example.com",
                        "firstName": "admin",
                        "lastName": "user",
                        "password": "admin",
                    }
                },
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert len(docs) == 1
        assert "release-name-airflow-create-user-job" not in jmespath.search("subjects | [*].name", docs[0])

    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ],
    )
    def test_should_render(self, executor):
        docs = render_chart(
            values={"rbac": {"create": True, "createSCCRoleBinding": True}, "executor": executor},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert len(docs) == 1

    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "LocalKubernetesExecutor",
            "CeleryKubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ],
    )
    def test_should_render_airflow_2(self, executor):
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert len(docs) == 1

    @pytest.mark.parametrize(("create", "role_binding"), [(True, False), (False, True), (False, False)])
    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ],
    )
    def test_should_not_render(self, create, role_binding, executor):
        docs = render_chart(
            values={"rbac": {"create": create, "createSCCRoleBinding": role_binding}, "executor": executor},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert len(docs) == 0

    @pytest.mark.parametrize(("create", "role_binding"), [(True, False), (False, True), (False, False)])
    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "LocalKubernetesExecutor",
            "CeleryKubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ],
    )
    def test_should_not_render_airflow_2(self, create, role_binding, executor):
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": create, "createSCCRoleBinding": role_binding},
                "executor": executor,
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert len(docs) == 0

    def test_multi_namespace_mode_disabled(self):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={"rbac": {"create": True, "createSCCRoleBinding": True}, "multiNamespaceMode": False},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "RoleBinding"

        metadata = jmespath.search("metadata", docs[0])
        assert metadata["namespace"] == "airflow"
        assert metadata["name"] == "prod-scc-rolebinding"
        assert "namespace" not in metadata["labels"]

    def test_multi_namespace_mode_disabled_airflow_2(self):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "multiNamespaceMode": False,
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "RoleBinding"

        metadata = jmespath.search("metadata", docs[0])
        assert metadata["namespace"] == "airflow"
        assert metadata["name"] == "prod-scc-rolebinding"
        assert "namespace" not in metadata["labels"]

    def test_multi_namespace_mode_enabled(self):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={"rbac": {"create": True, "createSCCRoleBinding": True}, "multiNamespaceMode": True},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "ClusterRoleBinding"

        metadata = jmespath.search("metadata", docs[0])
        assert "namespace" not in metadata
        assert metadata["name"] == "airflow-prod-scc-rolebinding"
        assert metadata["labels"]["namespace"] == "airflow"

    def test_multi_namespace_mode_enabled_airflow_2(self):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "multiNamespaceMode": True,
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "ClusterRoleBinding"

        metadata = jmespath.search("metadata", docs[0])
        assert "namespace" not in metadata
        assert metadata["name"] == "airflow-prod-scc-rolebinding"
        assert metadata["labels"]["namespace"] == "airflow"

    def test_role_ref_default(self):
        docs = render_chart(
            values={"rbac": {"create": True, "createSCCRoleBinding": True}},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("roleRef", docs[0]) == {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "system:openshift:scc:anyuid",
        }

    def test_role_ref_default_airflow_2(self):
        docs = render_chart(
            values={"airflowVersion": "2.11.0", "rbac": {"create": True, "createSCCRoleBinding": True}},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("roleRef", docs[0]) == {
            "apiGroup": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "system:openshift:scc:anyuid",
        }

    def test_no_role_bindings(self):
        docs = render_chart(
            values={
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": "LocalExecutor",
                "cleanup": {"enabled": False},
                "databaseCleanup": {"enabled": False},
                "flower": {"enabled": False},
                "dagProcessor": {"enabled": False},
                "apiServer": {"enabled": False},
                "scheduler": {"enabled": False},
                "statsd": {"enabled": False},
                "triggerer": {"enabled": False},
                "redis": {"enabled": False},
                "migrateDatabaseJob": {"enabled": False},
                "createUserJob": {"enabled": False},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects", docs[0]) is None

    def test_no_role_bindings_airflow_2(self):
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": "LocalExecutor",
                "cleanup": {"enabled": False},
                "databaseCleanup": {"enabled": False},
                "flower": {"enabled": False},
                "dagProcessor": {"enabled": False},
                "webserver": {"enabled": False},
                "scheduler": {"enabled": False},
                "statsd": {"enabled": False},
                "triggerer": {"enabled": False},
                "redis": {"enabled": False},
                "migrateDatabaseJob": {"enabled": False},
                "createUserJob": {"enabled": False},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects", docs[0]) is None

    @pytest.mark.parametrize(
        "executor",
        [
            "CeleryExecutor",
            "KubernetesExecutor",
            "LocalExecutor,CeleryExecutor",
        ],
    )
    def test_worker_role_binding_should_exists(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={"rbac": {"create": True, "createSCCRoleBinding": True}, "executor": executor},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-worker",
            "namespace": "airflow",
        }

    @pytest.mark.parametrize(
        "executor",
        [
            "CeleryExecutor",
            "KubernetesExecutor",
            "LocalKubernetesExecutor",
            "CeleryKubernetesExecutor",
            "LocalExecutor,CeleryExecutor",
        ],
    )
    def test_worker_role_binding_should_exists_airflow_2(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-worker",
            "namespace": "airflow",
        }

    def test_worker_role_binding_should_not_exists(self):
        docs = render_chart(
            name="prod",
            values={"rbac": {"create": True, "createSCCRoleBinding": True}, "executor": "LocalExecutor"},
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker']", docs[0]) == []

    def test_worker_role_binding_should_not_exists_airflow_2(self):
        docs = render_chart(
            name="prod",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": "LocalExecutor",
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-worker']", docs[0]) == []

    @pytest.mark.parametrize("executor", ["CeleryExecutor", "LocalExecutor,CeleryExecutor"])
    def test_flower_role_binding_should_exists(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
                "flower": {"enabled": True},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-flower'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-flower",
            "namespace": "airflow",
        }

    @pytest.mark.parametrize(
        "executor", ["CeleryExecutor", "CeleryKubernetesExecutor", "LocalExecutor,CeleryExecutor"]
    )
    def test_flower_role_binding_should_exists_airflow_2(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
                "flower": {"enabled": True},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-flower'] | [0]", docs[0]) == {
            "kind": "ServiceAccount",
            "name": "prod-airflow-flower",
            "namespace": "airflow",
        }

    @pytest.mark.parametrize(
        ("executor", "enabled"),
        [
            ("LocalExecutor", True),
            ("LocalExecutor", False),
            ("KubernetesExecutor", True),
            ("KubernetesExecutor", False),
            ("LocalKubernetesExecutor", False),
            ("LocalKubernetesExecutor", True),
            ("CeleryKubernetesExecutor", False),
            ("CeleryExecutor", False),
        ],
    )
    def test_flower_role_binding_should_not_exists(self, executor, enabled):
        docs = render_chart(
            name="prod",
            values={
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
                "flower": {"enabled": enabled},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-flower']", docs[0]) == []

    @pytest.mark.parametrize(
        ("executor", "enabled"),
        [
            ("LocalExecutor", True),
            ("LocalExecutor", False),
            ("KubernetesExecutor", True),
            ("KubernetesExecutor", False),
            ("LocalKubernetesExecutor", False),
            ("LocalKubernetesExecutor", True),
            ("CeleryKubernetesExecutor", False),
            ("CeleryExecutor", False),
        ],
    )
    def test_flower_role_binding_should_not_exists_airflow_2(self, executor, enabled):
        docs = render_chart(
            name="prod",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
                "flower": {"enabled": enabled},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        assert jmespath.search("subjects[?name=='prod-airflow-flower']", docs[0]) == []

    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "KubernetesExecutor",
            "CeleryExecutor",
            "KubernetesExecutor,LocalExecutor,CeleryExecutor",
        ],
    )
    def test_only_enable_components_role_binding_should_exists(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
                "scheduler": {"enabled": True},
                "apiServer": {"enabled": True},
                "statsd": {"enabled": True},
                "redis": {"enabled": True},
                "triggerer": {"enabled": True},
                "migrateDatabaseJob": {"enabled": True},
                "createUserJob": {"enabled": True},
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        subjects = jmespath.search("subjects", docs[0])
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-scheduler",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-api-server",
            "namespace": "airflow",
        } in subjects
        assert {"kind": "ServiceAccount", "name": "prod-airflow-statsd", "namespace": "airflow"} in subjects
        assert {"kind": "ServiceAccount", "name": "prod-airflow-redis", "namespace": "airflow"} in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-triggerer",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-migrate-database-job",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-create-user-job",
            "namespace": "airflow",
        } in subjects
        assert {"kind": "ServiceAccount", "name": "prod-airflow-cleanup", "namespace": "airflow"} in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-database-cleanup",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-dag-processor",
            "namespace": "airflow",
        } in subjects

    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "KubernetesExecutor",
            "CeleryExecutor",
            "LocalKubernetesExecutor",
            "CeleryKubernetesExecutor",
            "KubernetesExecutor,LocalExecutor,CeleryExecutor",
        ],
    )
    def test_only_enable_components_role_binding_should_exists_airflow_2(self, executor):
        docs = render_chart(
            name="prod",
            namespace="airflow",
            values={
                "airflowVersion": "2.11.0",
                "rbac": {"create": True, "createSCCRoleBinding": True},
                "executor": executor,
                "scheduler": {"enabled": True},
                "webserver": {"enabled": True},
                "statsd": {"enabled": True},
                "redis": {"enabled": True},
                "triggerer": {"enabled": True},
                "migrateDatabaseJob": {"enabled": True},
                "createUserJob": {"enabled": True},
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/rbac/security-context-constraint-rolebinding.yaml"],
        )

        subjects = jmespath.search("subjects", docs[0])
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-scheduler",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-webserver",
            "namespace": "airflow",
        } in subjects
        assert {"kind": "ServiceAccount", "name": "prod-airflow-statsd", "namespace": "airflow"} in subjects
        assert {"kind": "ServiceAccount", "name": "prod-airflow-redis", "namespace": "airflow"} in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-triggerer",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-migrate-database-job",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-create-user-job",
            "namespace": "airflow",
        } in subjects
        assert {"kind": "ServiceAccount", "name": "prod-airflow-cleanup", "namespace": "airflow"} in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-database-cleanup",
            "namespace": "airflow",
        } in subjects
        assert {
            "kind": "ServiceAccount",
            "name": "prod-airflow-dag-processor",
            "namespace": "airflow",
        } in subjects
