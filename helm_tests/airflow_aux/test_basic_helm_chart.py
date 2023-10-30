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

import base64
import warnings
from subprocess import CalledProcessError
from typing import Any
from unittest import mock

import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart

OBJECT_COUNT_IN_BASIC_DEPLOYMENT = 35


class TestBaseChartTest:
    """Tests basic helm chart tests."""

    def _get_values_with_version(self, values, version):
        if version != "default":
            values["airflowVersion"] = version
        return values

    def _get_object_count(self, version):
        if version == "2.3.2" or version == "default":
            return OBJECT_COUNT_IN_BASIC_DEPLOYMENT + 1
        return OBJECT_COUNT_IN_BASIC_DEPLOYMENT

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_basic_deployments(self, version):
        expected_object_count_in_basic_deployment = self._get_object_count(version)
        k8s_objects = render_chart(
            "test-basic",
            self._get_values_with_version(
                values={
                    "chart": {
                        "metadata": "AA",
                    },
                    "labels": {"test-label": "TEST-VALUE"},
                    "fullnameOverride": "test-basic",
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = {
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        }
        expected = {
            ("ServiceAccount", "test-basic-create-user-job"),
            ("ServiceAccount", "test-basic-migrate-database-job"),
            ("ServiceAccount", "test-basic-redis"),
            ("ServiceAccount", "test-basic-scheduler"),
            ("ServiceAccount", "test-basic-statsd"),
            ("ServiceAccount", "test-basic-triggerer"),
            ("ServiceAccount", "test-basic-webserver"),
            ("ServiceAccount", "test-basic-worker"),
            ("Secret", "test-basic-metadata"),
            ("Secret", "test-basic-broker-url"),
            ("Secret", "test-basic-fernet-key"),
            ("Secret", "test-basic-webserver-secret-key"),
            ("Secret", "test-basic-postgresql"),
            ("Secret", "test-basic-redis-password"),
            ("ConfigMap", "test-basic-config"),
            ("ConfigMap", "test-basic-statsd"),
            ("Role", "test-basic-pod-launcher-role"),
            ("Role", "test-basic-pod-log-reader-role"),
            ("RoleBinding", "test-basic-pod-launcher-rolebinding"),
            ("RoleBinding", "test-basic-pod-log-reader-rolebinding"),
            ("Service", "test-basic-postgresql-hl"),
            ("Service", "test-basic-postgresql"),
            ("Service", "test-basic-redis"),
            ("Service", "test-basic-statsd"),
            ("Service", "test-basic-webserver"),
            ("Service", "test-basic-worker"),
            ("Deployment", "test-basic-scheduler"),
            ("Deployment", "test-basic-statsd"),
            (self.default_trigger_obj(version), "test-basic-triggerer"),
            ("Deployment", "test-basic-webserver"),
            ("StatefulSet", "test-basic-postgresql"),
            ("StatefulSet", "test-basic-redis"),
            ("StatefulSet", "test-basic-worker"),
            ("Job", "test-basic-create-user"),
            ("Job", "test-basic-run-airflow-migrations"),
        }
        if version == "2.3.2":
            expected.add(("Secret", "test-basic-result-backend"))
        if version == "default":
            expected.add(("Service", "test-basic-triggerer"))
        assert list_of_kind_names_tuples == expected
        assert expected_object_count_in_basic_deployment == len(k8s_objects)
        for k8s_object in k8s_objects:
            labels = jmespath.search("metadata.labels", k8s_object) or {}
            if "helm.sh/chart" in labels:
                chart_name = labels.get("helm.sh/chart")
            else:
                chart_name = labels.get("chart")
            if chart_name and "postgresql" in chart_name:
                continue
            k8s_name = k8s_object["kind"] + ":" + k8s_object["metadata"]["name"]
            assert "TEST-VALUE" == labels.get(
                "test-label"
            ), f"Missing label test-label on {k8s_name}. Current labels: {labels}"

    def test_basic_deployments_with_standard_naming(self):
        k8s_objects = render_chart(
            "test-basic",
            {"useStandardNaming": True},
        )
        list_of_kind_names_tuples = {
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        }
        expected = {
            ("ServiceAccount", "test-basic-airflow-create-user-job"),
            ("ServiceAccount", "test-basic-airflow-migrate-database-job"),
            ("ServiceAccount", "test-basic-airflow-redis"),
            ("ServiceAccount", "test-basic-airflow-scheduler"),
            ("ServiceAccount", "test-basic-airflow-statsd"),
            ("ServiceAccount", "test-basic-airflow-triggerer"),
            ("ServiceAccount", "test-basic-airflow-webserver"),
            ("ServiceAccount", "test-basic-airflow-worker"),
            ("Secret", "test-basic-airflow-metadata"),
            ("Secret", "test-basic-broker-url"),
            ("Secret", "test-basic-fernet-key"),
            ("Secret", "test-basic-airflow-webserver-secret-key"),
            ("Secret", "test-basic-redis-password"),
            ("Secret", "test-basic-postgresql"),
            ("ConfigMap", "test-basic-airflow-config"),
            ("ConfigMap", "test-basic-airflow-statsd"),
            ("Role", "test-basic-airflow-pod-launcher-role"),
            ("Role", "test-basic-airflow-pod-log-reader-role"),
            ("RoleBinding", "test-basic-airflow-pod-launcher-rolebinding"),
            ("RoleBinding", "test-basic-airflow-pod-log-reader-rolebinding"),
            ("Service", "test-basic-airflow-redis"),
            ("Service", "test-basic-airflow-statsd"),
            ("Service", "test-basic-airflow-triggerer"),
            ("Service", "test-basic-airflow-webserver"),
            ("Service", "test-basic-airflow-worker"),
            ("Service", "test-basic-postgresql"),
            ("Service", "test-basic-postgresql-hl"),
            ("Deployment", "test-basic-airflow-scheduler"),
            ("Deployment", "test-basic-airflow-statsd"),
            ("Deployment", "test-basic-airflow-webserver"),
            ("StatefulSet", "test-basic-airflow-redis"),
            ("StatefulSet", "test-basic-airflow-worker"),
            ("StatefulSet", "test-basic-airflow-triggerer"),
            ("StatefulSet", "test-basic-postgresql"),
            ("Job", "test-basic-airflow-create-user"),
            ("Job", "test-basic-airflow-run-airflow-migrations"),
        }
        assert list_of_kind_names_tuples == expected

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_basic_deployment_with_standalone_dag_processor(self, version):
        # Dag Processor creates two extra objects compared to the basic deployment
        object_count_in_basic_deployment = self._get_object_count(version)
        expected_object_count_with_standalone_scheduler = object_count_in_basic_deployment + 2
        k8s_objects = render_chart(
            "test-basic",
            self._get_values_with_version(
                values={
                    "chart": {
                        "metadata": "AA",
                    },
                    "labels": {"test-label": "TEST-VALUE"},
                    "fullnameOverride": "test-basic",
                    "dagProcessor": {"enabled": True},
                },
                version=version,
            ),
        )
        list_of_kind_names_tuples = {
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        }
        expected = {
            ("ServiceAccount", "test-basic-create-user-job"),
            ("ServiceAccount", "test-basic-migrate-database-job"),
            ("ServiceAccount", "test-basic-redis"),
            ("ServiceAccount", "test-basic-scheduler"),
            ("ServiceAccount", "test-basic-statsd"),
            ("ServiceAccount", "test-basic-triggerer"),
            ("ServiceAccount", "test-basic-dag-processor"),
            ("ServiceAccount", "test-basic-webserver"),
            ("ServiceAccount", "test-basic-worker"),
            ("Secret", "test-basic-metadata"),
            ("Secret", "test-basic-broker-url"),
            ("Secret", "test-basic-fernet-key"),
            ("Secret", "test-basic-webserver-secret-key"),
            ("Secret", "test-basic-postgresql"),
            ("Secret", "test-basic-redis-password"),
            ("ConfigMap", "test-basic-config"),
            ("ConfigMap", "test-basic-statsd"),
            ("Role", "test-basic-pod-launcher-role"),
            ("Role", "test-basic-pod-log-reader-role"),
            ("RoleBinding", "test-basic-pod-launcher-rolebinding"),
            ("RoleBinding", "test-basic-pod-log-reader-rolebinding"),
            ("Service", "test-basic-postgresql-hl"),
            ("Service", "test-basic-postgresql"),
            ("Service", "test-basic-redis"),
            ("Service", "test-basic-statsd"),
            ("Service", "test-basic-webserver"),
            ("Service", "test-basic-worker"),
            ("Deployment", "test-basic-scheduler"),
            ("Deployment", "test-basic-statsd"),
            (self.default_trigger_obj(version), "test-basic-triggerer"),
            ("Deployment", "test-basic-dag-processor"),
            ("Deployment", "test-basic-webserver"),
            ("StatefulSet", "test-basic-postgresql"),
            ("StatefulSet", "test-basic-redis"),
            ("StatefulSet", "test-basic-worker"),
            ("Job", "test-basic-create-user"),
            ("Job", "test-basic-run-airflow-migrations"),
        }
        if version == "2.3.2":
            expected.add(("Secret", "test-basic-result-backend"))
        if version == "default":
            expected.add(("Service", "test-basic-triggerer"))
        assert list_of_kind_names_tuples == expected
        assert expected_object_count_with_standalone_scheduler == len(k8s_objects)
        for k8s_object in k8s_objects:
            labels = jmespath.search("metadata.labels", k8s_object) or {}
            if "helm.sh/chart" in labels:
                chart_name = labels.get("helm.sh/chart")
            else:
                chart_name = labels.get("chart")
            if chart_name and "postgresql" in chart_name:
                continue
            k8s_name = k8s_object["kind"] + ":" + k8s_object["metadata"]["name"]
            assert "TEST-VALUE" == labels.get(
                "test-label"
            ), f"Missing label test-label on {k8s_name}. Current labels: {labels}"

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_basic_deployment_without_default_users(self, version):
        expected_object_count_in_basic_deployment = self._get_object_count(version)
        k8s_objects = render_chart(
            "test-basic",
            values=self._get_values_with_version(
                values={"webserver": {"defaultUser": {"enabled": False}}}, version=version
            ),
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        assert ("Job", "test-basic-create-user") not in list_of_kind_names_tuples
        assert expected_object_count_in_basic_deployment - 2 == len(k8s_objects)

    @pytest.mark.parametrize("version", ["2.3.2", "2.4.0", "default"])
    def test_basic_deployment_without_statsd(self, version):
        expected_object_count_in_basic_deployment = self._get_object_count(version)
        k8s_objects = render_chart(
            "test-basic",
            values=self._get_values_with_version(values={"statsd": {"enabled": False}}, version=version),
        )
        list_of_kind_names_tuples = [
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        ]
        assert ("ServiceAccount", "test-basic-statsd") not in list_of_kind_names_tuples
        assert ("ConfigMap", "test-basic-statsd") not in list_of_kind_names_tuples
        assert ("Service", "test-basic-statsd") not in list_of_kind_names_tuples
        assert ("Deployment", "test-basic-statsd") not in list_of_kind_names_tuples

        assert expected_object_count_in_basic_deployment - 4 == len(k8s_objects)

    def test_network_policies_are_valid(self):
        k8s_objects = render_chart(
            "test-basic",
            {
                "networkPolicies": {"enabled": True},
                "executor": "CeleryExecutor",
                "flower": {"enabled": True},
                "pgbouncer": {"enabled": True},
            },
        )
        kind_names_tuples = {
            (k8s_object["kind"], k8s_object["metadata"]["name"]) for k8s_object in k8s_objects
        }

        expected_kind_names = [
            ("NetworkPolicy", "test-basic-redis-policy"),
            ("NetworkPolicy", "test-basic-flower-policy"),
            ("NetworkPolicy", "test-basic-pgbouncer-policy"),
            ("NetworkPolicy", "test-basic-scheduler-policy"),
            ("NetworkPolicy", "test-basic-statsd-policy"),
            ("NetworkPolicy", "test-basic-webserver-policy"),
            ("NetworkPolicy", "test-basic-worker-policy"),
        ]
        for kind_name in expected_kind_names:
            assert kind_name in kind_names_tuples

    def test_labels_are_valid(self):
        """Test labels are correctly applied on all objects created by this chart."""
        release_name = "test-basic"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "labels": {"label1": "value1", "label2": "value2"},
                "executor": "CeleryExecutor",
                "data": {
                    "resultBackendConnection": {
                        "user": "someuser",
                        "pass": "somepass",
                        "host": "somehost",
                        "protocol": "postgresql",
                        "port": 7777,
                        "db": "somedb",
                        "sslmode": "allow",
                    }
                },
                "pgbouncer": {"enabled": True},
                "redis": {"enabled": True},
                "ingress": {"enabled": True},
                "networkPolicies": {"enabled": True},
                "cleanup": {"enabled": True},
                "flower": {"enabled": True},
                "dagProcessor": {"enabled": True},
                "logs": {"persistence": {"enabled": True}},
                "dags": {"persistence": {"enabled": True}},
                "postgresql": {"enabled": False},  # We won't check the objects created by the postgres chart
            },
        )
        kind_k8s_obj_labels_tuples = {
            (k8s_object["metadata"]["name"], k8s_object["kind"]): k8s_object["metadata"]["labels"]
            for k8s_object in k8s_objects
        }

        kind_names_tuples = [
            (f"{release_name}-airflow-cleanup", "ServiceAccount", None),
            (f"{release_name}-config", "ConfigMap", "config"),
            (f"{release_name}-airflow-create-user-job", "ServiceAccount", "create-user-job"),
            (f"{release_name}-airflow-flower", "ServiceAccount", "flower"),
            (f"{release_name}-metadata", "Secret", None),
            (f"{release_name}-airflow-migrate-database-job", "ServiceAccount", "run-airflow-migrations"),
            (f"{release_name}-airflow-pgbouncer", "ServiceAccount", "pgbouncer"),
            (f"{release_name}-result-backend", "Secret", None),
            (f"{release_name}-airflow-redis", "ServiceAccount", "redis"),
            (f"{release_name}-airflow-scheduler", "ServiceAccount", "scheduler"),
            (f"{release_name}-airflow-statsd", "ServiceAccount", "statsd"),
            (f"{release_name}-airflow-webserver", "ServiceAccount", "webserver"),
            (f"{release_name}-airflow-worker", "ServiceAccount", "worker"),
            (f"{release_name}-airflow-triggerer", "ServiceAccount", "triggerer"),
            (f"{release_name}-airflow-dag-processor", "ServiceAccount", "dag-processor"),
            (f"{release_name}-broker-url", "Secret", "redis"),
            (f"{release_name}-cleanup", "CronJob", "airflow-cleanup-pods"),
            (f"{release_name}-cleanup-role", "Role", None),
            (f"{release_name}-cleanup-rolebinding", "RoleBinding", None),
            (f"{release_name}-create-user", "Job", "create-user-job"),
            (f"{release_name}-fernet-key", "Secret", None),
            (f"{release_name}-flower", "Deployment", "flower"),
            (f"{release_name}-flower", "Service", "flower"),
            (f"{release_name}-flower-policy", "NetworkPolicy", "airflow-flower-policy"),
            (f"{release_name}-flower-ingress", "Ingress", "flower-ingress"),
            (f"{release_name}-pgbouncer", "Deployment", "pgbouncer"),
            (f"{release_name}-pgbouncer", "Service", "pgbouncer"),
            (f"{release_name}-pgbouncer-config", "Secret", "pgbouncer"),
            (f"{release_name}-pgbouncer-policy", "NetworkPolicy", "airflow-pgbouncer-policy"),
            (f"{release_name}-pgbouncer-stats", "Secret", "pgbouncer"),
            (f"{release_name}-pod-launcher-role", "Role", None),
            (f"{release_name}-pod-launcher-rolebinding", "RoleBinding", None),
            (f"{release_name}-pod-log-reader-role", "Role", None),
            (f"{release_name}-pod-log-reader-rolebinding", "RoleBinding", None),
            (f"{release_name}-redis", "Service", "redis"),
            (f"{release_name}-redis", "StatefulSet", "redis"),
            (f"{release_name}-redis-policy", "NetworkPolicy", "redis-policy"),
            (f"{release_name}-redis-password", "Secret", "redis"),
            (f"{release_name}-run-airflow-migrations", "Job", "run-airflow-migrations"),
            (f"{release_name}-scheduler", "Deployment", "scheduler"),
            (f"{release_name}-scheduler-policy", "NetworkPolicy", "airflow-scheduler-policy"),
            (f"{release_name}-statsd", "Deployment", "statsd"),
            (f"{release_name}-statsd", "Service", "statsd"),
            (f"{release_name}-statsd-policy", "NetworkPolicy", "statsd-policy"),
            (f"{release_name}-webserver", "Deployment", "webserver"),
            (f"{release_name}-webserver-secret-key", "Secret", "webserver"),
            (f"{release_name}-webserver", "Service", "webserver"),
            (f"{release_name}-webserver-policy", "NetworkPolicy", "airflow-webserver-policy"),
            (f"{release_name}-ingress", "Ingress", "airflow-ingress"),
            (f"{release_name}-worker", "Service", "worker"),
            (f"{release_name}-worker", "StatefulSet", "worker"),
            (f"{release_name}-worker-policy", "NetworkPolicy", "airflow-worker-policy"),
            (f"{release_name}-triggerer", "StatefulSet", "triggerer"),
            (f"{release_name}-dag-processor", "Deployment", "dag-processor"),
            (f"{release_name}-logs", "PersistentVolumeClaim", "logs-pvc"),
            (f"{release_name}-dags", "PersistentVolumeClaim", "dags-pvc"),
        ]
        for k8s_object_name, kind, component in kind_names_tuples:
            expected_labels = {
                "label1": "value1",
                "label2": "value2",
                "tier": "airflow",
                "release": release_name,
                "heritage": "Helm",
                "chart": mock.ANY,
            }
            if component:
                expected_labels["component"] = component
            if k8s_object_name == f"{release_name}-scheduler":
                expected_labels["executor"] = "CeleryExecutor"
            actual_labels = kind_k8s_obj_labels_tuples.pop((k8s_object_name, kind))
            assert actual_labels == expected_labels

        if kind_k8s_obj_labels_tuples:
            warnings.warn(f"Unchecked objects: {kind_k8s_obj_labels_tuples.keys()}")

    def test_labels_are_valid_on_job_templates(self):
        """Test labels are correctly applied on all job templates created by this chart."""
        release_name = "test-basic"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "labels": {"label1": "value1", "label2": "value2"},
                "executor": "CeleryExecutor",
                "dagProcessor": {"enabled": True},
                "pgbouncer": {"enabled": True},
                "redis": {"enabled": True},
                "networkPolicies": {"enabled": True},
                "cleanup": {"enabled": True},
                "flower": {"enabled": True},
                "postgresql": {"enabled": False},  # We won't check the objects created by the postgres chart
            },
        )
        dict_of_labels_in_job_templates = {
            k8s_object["metadata"]["name"]: k8s_object["spec"]["template"]["metadata"]["labels"]
            for k8s_object in k8s_objects
            if k8s_object["kind"] == "Job"
        }

        kind_names_tuples = [
            (f"{release_name}-create-user", "create-user-job"),
            (f"{release_name}-run-airflow-migrations", "run-airflow-migrations"),
        ]
        for k8s_object_name, component in kind_names_tuples:
            expected_labels = {
                "label1": "value1",
                "label2": "value2",
                "tier": "airflow",
                "release": release_name,
                "component": component,
            }
            assert dict_of_labels_in_job_templates.get(k8s_object_name) == expected_labels

    def test_annotations_on_airflow_pods_in_deployment(self):
        """
        Test Annotations are correctly applied.

        Verifies all pods created Scheduler, Webserver & Worker deployments.
        """
        release_name = "test-basic"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "airflowPodAnnotations": {"test-annotation/safe-to-evict": "true"},
                "flower": {"enabled": True},
                "dagProcessor": {"enabled": True},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
            ],
        )
        # pod_template_file is tested separately as it has extra setup steps

        assert 8 == len(k8s_objects)

        for k8s_object in k8s_objects:
            annotations = k8s_object["spec"]["template"]["metadata"]["annotations"]
            assert "test-annotation/safe-to-evict" in annotations
            assert "true" in annotations["test-annotation/safe-to-evict"]

    def test_chart_is_consistent_with_official_airflow_image(self):
        def get_k8s_objs_with_image(obj: list[Any] | dict[str, Any]) -> list[dict[str, Any]]:
            """Retrieve all the k8s objects that have an "image" key inside k8s obj or list of k8s obj."""
            out = []
            if isinstance(obj, list):
                for item in obj:
                    out += get_k8s_objs_with_image(item)
            if isinstance(obj, dict):
                if "image" in obj:
                    out += [obj]
                # include sub objs, just in case
                for val in obj.values():
                    out += get_k8s_objs_with_image(val)
            return out

        image_repo = "test-airflow-repo/airflow"
        k8s_objects = render_chart("test-basic", {"defaultAirflowRepository": image_repo})

        objs_with_image = get_k8s_objs_with_image(k8s_objects)
        for obj in objs_with_image:
            image: str = obj["image"]
            if image.startswith(image_repo):
                # Make sure that a command is not specified
                assert "command" not in obj

    def test_unsupported_executor(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                "test-basic",
                {
                    "executor": "SequentialExecutor",
                },
            )
        assert (
            'executor must be one of the following: "LocalExecutor", '
            '"LocalKubernetesExecutor", "CeleryExecutor", '
            '"KubernetesExecutor", "CeleryKubernetesExecutor"' in ex_ctx.value.stderr.decode()
        )

    @pytest.mark.parametrize(
        "image",
        ["airflow", "pod_template", "flower", "statsd", "redis", "pgbouncer", "pgbouncerExporter", "gitSync"],
    )
    def test_invalid_pull_policy(self, image):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                "test-basic",
                {
                    "images": {image: {"pullPolicy": "InvalidPolicy"}},
                },
            )
        assert (
            'pullPolicy must be one of the following: "Always", "Never", "IfNotPresent"'
            in ex_ctx.value.stderr.decode()
        )

    def test_invalid_dags_access_mode(self):
        with pytest.raises(CalledProcessError) as ex_ctx:
            render_chart(
                "test-basic",
                {
                    "dags": {"persistence": {"accessMode": "InvalidMode"}},
                },
            )
        assert (
            'accessMode must be one of the following: "ReadWriteOnce", "ReadOnlyMany", "ReadWriteMany"'
            in ex_ctx.value.stderr.decode()
        )

    @pytest.mark.parametrize("namespace", ["abc", "123", "123abc", "123-abc"])
    def test_namespace_names(self, namespace):
        """Test various namespace names to make sure they render correctly in templates."""
        render_chart(namespace=namespace)

    def test_postgres_connection_url_no_override(self):
        # no nameoverride provided
        doc = render_chart(
            "my-release",
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
        )[0]
        assert (
            "postgresql://postgres:postgres@my-release-postgresql.default:5432/postgres?sslmode=disable"
            == base64.b64decode(doc["data"]["connection"]).decode("utf-8")
        )

    def test_postgres_connection_url_pgbouncer(self):
        # no nameoverride, pgbouncer
        doc = render_chart(
            "my-release",
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
            values={"pgbouncer": {"enabled": True}},
        )[0]
        assert (
            "postgresql://postgres:postgres@my-release-pgbouncer.default:6543/"
            "my-release-metadata?sslmode=disable"
            == base64.b64decode(doc["data"]["connection"]).decode("utf-8")
        )

    def test_postgres_connection_url_pgbouncer_use_standard_naming(self):
        # no nameoverride, pgbouncer and useStandardNaming
        doc = render_chart(
            "my-release",
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
            values={"useStandardNaming": True, "pgbouncer": {"enabled": True}},
        )[0]
        assert (
            "postgresql://postgres:postgres@my-release-airflow-pgbouncer.default:6543/"
            "my-release-metadata?sslmode=disable"
            == base64.b64decode(doc["data"]["connection"]).decode("utf-8")
        )

    def test_postgres_connection_url_name_override(self):
        # nameoverride provided
        doc = render_chart(
            "my-release",
            show_only=["templates/secrets/metadata-connection-secret.yaml"],
            values={"postgresql": {"nameOverride": "overrideName"}},
        )[0]

        assert (
            "postgresql://postgres:postgres@overrideName:5432/postgres?sslmode=disable"
            == base64.b64decode(doc["data"]["connection"]).decode("utf-8")
        )

    def test_priority_classes(self):
        pc = [
            {"name": "class1", "preemptionPolicy": "PreemptLowerPriority", "value": 1000},
            {"name": "class2", "preemptionPolicy": "Never", "value": 10000},
        ]
        objs = render_chart(
            "my-release",
            show_only=["templates/priorityclasses/priority-classes.yaml"],
            values={"priorityClasses": pc},
        )

        assert len(objs) == 2

        for i in range(len(objs)):
            assert objs[i]["kind"] == "PriorityClass"
            assert objs[i]["apiVersion"] == "scheduling.k8s.io/v1"
            assert objs[i]["metadata"]["name"] == ("my-release" + "-" + pc[i]["name"])
            assert objs[i]["preemptionPolicy"] == pc[i]["preemptionPolicy"]
            assert objs[i]["value"] == pc[i]["value"]

    def test_priority_classes_default_preemption(self):
        obj = render_chart(
            "my-release",
            show_only=["templates/priorityclasses/priority-classes.yaml"],
            values={
                "priorityClasses": [
                    {"name": "class1", "value": 10000},
                ]
            },
        )[0]

        assert obj["preemptionPolicy"] == "PreemptLowerPriority"

    def test_redis_broker_connection_url(self):
        # no nameoverride, redis
        doc = render_chart(
            "my-release",
            show_only=["templates/secrets/redis-secrets.yaml"],
            values={"redis": {"enabled": True, "password": "test1234"}},
        )[1]
        assert "redis://:test1234@my-release-redis:6379/0" == base64.b64decode(
            doc["data"]["connection"]
        ).decode("utf-8")

    def test_redis_broker_connection_url_use_standard_naming(self):
        # no nameoverride, redis and useStandardNaming
        doc = render_chart(
            "my-release",
            show_only=["templates/secrets/redis-secrets.yaml"],
            values={"useStandardNaming": True, "redis": {"enabled": True, "password": "test1234"}},
        )[1]
        assert "redis://:test1234@my-release-airflow-redis:6379/0" == base64.b64decode(
            doc["data"]["connection"]
        ).decode("utf-8")

    @staticmethod
    def default_trigger_obj(version):
        if version == "default":
            return "StatefulSet"
        return "Deployment"
