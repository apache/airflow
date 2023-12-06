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

from tests.charts.helm_template_generator import render_chart


class TestKeda:
    """Tests keda."""

    def test_keda_disabled_by_default(self):
        """Disabled by default."""
        docs = render_chart(
            values={},
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert docs == []

    @pytest.mark.parametrize(
        "executor, is_created",
        [
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
        ],
    )
    def test_keda_enabled(self, executor, is_created):
        """ScaledObject should only be created when enabled and executor is Celery or CeleryKubernetes."""
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
                "executor": executor,
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        if is_created:
            assert jmespath.search("metadata.name", docs[0]) == "release-name-worker"
        else:
            assert docs == []

    @pytest.mark.parametrize("executor", ["CeleryExecutor", "CeleryKubernetesExecutor"])
    def test_keda_advanced(self, executor):
        """Verify keda advanced config."""
        expected_advanced = {
            "horizontalPodAutoscalerConfig": {
                "behavior": {
                    "scaleDown": {
                        "stabilizationWindowSeconds": 300,
                        "policies": [{"type": "Percent", "value": 100, "periodSeconds": 15}],
                    }
                }
            }
        }
        docs = render_chart(
            values={
                "workers": {
                    "keda": {
                        "enabled": True,
                        "advanced": expected_advanced,
                    },
                },
                "executor": executor,
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert jmespath.search("spec.advanced", docs[0]) == expected_advanced

    @staticmethod
    def build_query(executor, concurrency=16, queue=None):
        """Build the query used by KEDA autoscaler to determine how many workers there should be."""
        query = (
            f"SELECT ceil(COUNT(*)::decimal / {concurrency}) "
            "FROM task_instance WHERE (state='running' OR state='queued')"
        )
        if executor == "CeleryKubernetesExecutor":
            query += f" AND queue != '{queue or 'kubernetes'}'"
        return query

    @pytest.mark.parametrize(
        "executor,concurrency",
        [
            ("CeleryExecutor", 8),
            ("CeleryExecutor", 16),
            ("CeleryKubernetesExecutor", 8),
            ("CeleryKubernetesExecutor", 16),
        ],
    )
    def test_keda_concurrency(self, executor, concurrency):
        """Verify keda sql query uses configured concurrency."""
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
                "executor": executor,
                "config": {"celery": {"worker_concurrency": concurrency}},
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        expected_query = self.build_query(executor=executor, concurrency=concurrency)
        assert jmespath.search("spec.triggers[0].metadata.query", docs[0]) == expected_query

    @pytest.mark.parametrize(
        "executor,queue,should_filter",
        [
            ("CeleryExecutor", None, False),
            ("CeleryExecutor", "my_queue", False),
            ("CeleryKubernetesExecutor", None, True),
            ("CeleryKubernetesExecutor", "my_queue", True),
        ],
    )
    def test_keda_query_kubernetes_queue(self, executor, queue, should_filter):
        """
        Verify keda sql query ignores kubernetes queue when CKE is used.

        Sometimes a user might want to use a different queue name for k8s executor tasks,
        and we also verify here that we use the configured queue name in that case.
        """
        values = {
            "workers": {"keda": {"enabled": True}, "persistence": {"enabled": False}},
            "executor": executor,
        }
        if queue:
            values.update({"config": {"celery_kubernetes_executor": {"kubernetes_queue": queue}}})
        docs = render_chart(
            values=values,
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        expected_query = self.build_query(executor=executor, queue=queue)
        assert jmespath.search("spec.triggers[0].metadata.query", docs[0]) == expected_query

    @pytest.mark.parametrize(
        "enabled, kind",
        [
            ("enabled", "StatefulSet"),
            ("not_enabled", "Deployment"),
        ],
    )
    def test_persistence(self, enabled, kind):
        """If worker persistence is enabled, scaleTargetRef should be StatefulSet else Deployment."""
        is_enabled = enabled == "enabled"
        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}, "persistence": {"enabled": is_enabled}},
                "executor": "CeleryExecutor",
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert jmespath.search("spec.scaleTargetRef.kind", docs[0]) == kind

    def test_default_keda_db_connection(self):
        """Verify default keda db connection."""
        import base64

        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}},
                "executor": "CeleryExecutor",
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/workers/worker-kedaautoscaler.yaml",
                "templates/secrets/metadata-connection-secret.yaml",
            ],
        )
        worker_deployment = docs[0]
        keda_autoscaler = docs[1]
        metadata_connection_secret = docs[2]

        worker_container_env_vars = jmespath.search(
            "spec.template.spec.containers[?name=='worker'].env[].name", worker_deployment
        )
        assert "AIRFLOW_CONN_AIRFLOW_DB" in worker_container_env_vars
        assert "KEDA_DB_CONN" not in worker_container_env_vars

        secret_data = jmespath.search("data", metadata_connection_secret)
        assert "connection" in secret_data.keys()
        assert "@release-name-postgresql" in base64.b64decode(secret_data["connection"]).decode()
        assert "kedaConnection" not in secret_data.keys()

        autoscaler_connection_env_var = jmespath.search(
            "spec.triggers[0].metadata.connectionFromEnv", keda_autoscaler
        )
        assert autoscaler_connection_env_var == "AIRFLOW_CONN_AIRFLOW_DB"

    def test_default_keda_db_connection_pgbouncer_enabled(self):
        """Verify keda db connection when pgbouncer is enabled."""
        import base64

        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True}},
                "executor": "CeleryExecutor",
                "pgbouncer": {"enabled": True},
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/workers/worker-kedaautoscaler.yaml",
                "templates/secrets/metadata-connection-secret.yaml",
            ],
        )
        worker_deployment = docs[0]
        keda_autoscaler = docs[1]
        metadata_connection_secret = docs[2]

        worker_container_env_vars = jmespath.search(
            "spec.template.spec.containers[?name=='worker'].env[].name", worker_deployment
        )
        assert "AIRFLOW_CONN_AIRFLOW_DB" in worker_container_env_vars
        assert "KEDA_DB_CONN" not in worker_container_env_vars

        secret_data = jmespath.search("data", metadata_connection_secret)
        assert "connection" in secret_data.keys()
        assert "@release-name-pgbouncer" in base64.b64decode(secret_data["connection"]).decode()
        assert "kedaConnection" not in secret_data.keys()

        autoscaler_connection_env_var = jmespath.search(
            "spec.triggers[0].metadata.connectionFromEnv", keda_autoscaler
        )
        assert autoscaler_connection_env_var == "AIRFLOW_CONN_AIRFLOW_DB"

    def test_default_keda_db_connection_pgbouncer_enabled_usePgbouncer_false(self):
        """Verify keda db connection when pgbouncer is enabled and usePgbouncer is false."""
        import base64

        docs = render_chart(
            values={
                "workers": {"keda": {"enabled": True, "usePgbouncer": False}},
                "executor": "CeleryExecutor",
                "pgbouncer": {"enabled": True},
            },
            show_only=[
                "templates/workers/worker-deployment.yaml",
                "templates/workers/worker-kedaautoscaler.yaml",
                "templates/secrets/metadata-connection-secret.yaml",
            ],
        )
        worker_deployment = docs[0]
        keda_autoscaler = docs[1]
        metadata_connection_secret = docs[2]

        worker_container_env_vars = jmespath.search(
            "spec.template.spec.containers[?name=='worker'].env[].name", worker_deployment
        )
        assert "AIRFLOW_CONN_AIRFLOW_DB" in worker_container_env_vars
        assert "KEDA_DB_CONN" in worker_container_env_vars

        secret_data = jmespath.search("data", metadata_connection_secret)
        connection_secret = base64.b64decode(secret_data["connection"]).decode()
        keda_connection_secret = base64.b64decode(secret_data["kedaConnection"]).decode()
        assert "connection" in secret_data.keys()
        assert "@release-name-pgbouncer" in connection_secret
        assert ":6543" in connection_secret
        assert "/release-name-metadata" in connection_secret
        assert "kedaConnection" in secret_data.keys()
        assert "@release-name-postgresql" in keda_connection_secret
        assert ":5432" in keda_connection_secret
        assert "/postgres" in keda_connection_secret

        autoscaler_connection_env_var = jmespath.search(
            "spec.triggers[0].metadata.connectionFromEnv", keda_autoscaler
        )
        assert autoscaler_connection_env_var == "KEDA_DB_CONN"
