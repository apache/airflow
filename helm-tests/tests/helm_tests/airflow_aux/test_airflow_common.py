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


class TestAirflowCommon:
    """
    Tests that apply to more than 1 Airflow component so we don't have to repeat tests everywhere.

    The one general exception will be the KubernetesExecutor PodTemplateFile, as it requires extra test setup.
    """

    @pytest.mark.parametrize(
        ("logs_values", "expected_mount"),
        [
            (
                {"persistence": {"enabled": True, "subPath": "test/logs"}},
                {"subPath": "test/logs", "mountPath": "/opt/airflow/logs", "name": "logs"},
            ),
        ],
    )
    def test_logs_mount(self, logs_values, expected_mount):
        docs = render_chart(
            values={
                "logs": logs_values,
                "airflowVersion": "3.0.0",
            },  # airflowVersion is present so webserver gets the mount
            show_only=[
                "templates/api-server/api-server-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        assert len(docs) == 5
        for doc in docs:
            assert expected_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", doc)

        # check for components deployed when airflow version is < 3.0.0
        docs = render_chart(
            values={
                "logs": logs_values,
                "airflowVersion": "1.10.15",
            },  # airflowVersion is present so webserver gets the mount
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
            ],
        )

        assert len(docs) == 3
        for doc in docs:
            assert expected_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", doc)

    @pytest.mark.parametrize(
        ("dag_values", "expected_mount"),
        [
            (
                {
                    "gitSync": {
                        "enabled": True,
                        "components": {
                            "dagProcessor": True,
                            "scheduler": True,
                            "triggerer": True,
                            "workers": True,
                            "webserver": True,
                        }
                    }
                },
                {
                    "mountPath": "/opt/airflow/dags",
                    "name": "dags",
                    "readOnly": True,
                },
            ),
            (
                {"persistence": {"enabled": True}},
                {
                    "mountPath": "/opt/airflow/dags",
                    "name": "dags",
                    "readOnly": False,
                },
            ),
            (
                {
                    "gitSync": {
                        "enabled": True,
                        "components": {
                            "dagProcessor": True,
                            "scheduler": True,
                            "triggerer": True,
                            "workers": True,
                            "webserver": True,
                        }
                    },
                    "persistence": {"enabled": True},
                },
                {
                    "mountPath": "/opt/airflow/dags",
                    "name": "dags",
                    "readOnly": True,
                },
            ),
            (
                {"persistence": {"enabled": True, "subPath": "test/dags"}},
                {
                    "subPath": "test/dags",
                    "mountPath": "/opt/airflow/dags",
                    "name": "dags",
                    "readOnly": False,
                },
            ),
            (
                {
                    "mountPath": "/opt/airflow/dags/custom",
                    "gitSync": {
                        "enabled": True,
                        "components": {
                            "dagProcessor": True,
                            "scheduler": True,
                            "triggerer": True,
                            "workers": True,
                            "webserver": True,
                        }
                    }
                },
                {
                    "mountPath": "/opt/airflow/dags/custom",
                    "name": "dags",
                    "readOnly": True,
                },
            ),
            (
                {"mountPath": "/opt/airflow/dags/custom", "persistence": {"enabled": True}},
                {
                    "mountPath": "/opt/airflow/dags/custom",
                    "name": "dags",
                    "readOnly": False,
                },
            ),
        ],
    )
    def test_dags_mount(self, dag_values, expected_mount):
        docs = render_chart(
            values={
                "dags": dag_values,
                "dagProcessor": {"enabled": True},
            },
            show_only=[
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        assert len(docs) == 5
        for doc in docs:
            assert expected_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", doc)

    def test_webserver_config_configmap_name_volume_mounts(self):
        configmap_name = "my-configmap"
        docs = render_chart(
            values={
                "webserver": {
                    "webserverConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}",
                    "webserverConfigConfigMapName": configmap_name,
                },
                "workers": {"kerberosSidecar": {"enabled": True}},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )
        for doc in docs:
            assert "webserver-config" in [
                c["name"]
                for r in jmespath.search(
                    "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations'].volumeMounts",
                    doc,
                )
                for c in r
            ]
            for container in jmespath.search("spec.template.spec.containers", doc):
                assert "webserver-config" in [c["name"] for c in jmespath.search("volumeMounts", container)]
            assert "webserver-config" in [
                c["name"] for c in jmespath.search("spec.template.spec.volumes", doc)
            ]
            assert configmap_name == jmespath.search(
                "spec.template.spec.volumes[?name=='webserver-config'].configMap.name | [0]", doc
            )

    def test_annotations(self):
        """
        Test Annotations are correctly applied.

        Verifies all pods created Scheduler, Webserver & Worker deployments.
        """
        release_name = "test-basic"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "airflowPodAnnotations": {"test-annotation/safe-to-evict": "true"},
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "flower": {"enabled": True},
                "dagProcessor": {"enabled": True},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/database-cleanup/database-cleanup-cronjob.yaml",
            ],
        )

        # Objects in show_only are 9 but only one of Webserver or API server is created so we have 8 objects
        assert len(k8s_objects) == 8

        for k8s_object in k8s_objects:
            if k8s_object["kind"] == "CronJob":
                annotations = k8s_object["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["annotations"]
            else:
                annotations = k8s_object["spec"]["template"]["metadata"]["annotations"]

            assert "test-annotation/safe-to-evict" in annotations
            assert "true" in annotations["test-annotation/safe-to-evict"]

    def test_global_affinity_tolerations_topology_spread_constraints_and_node_selector(self):
        """Test affinity, tolerations, etc are correctly applied on all pods created."""
        k8s_objects = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
                "flower": {"enabled": True},
                "pgbouncer": {"enabled": True},
                "dagProcessor": {"enabled": True},
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {"key": "foo", "operator": "In", "values": ["true"]},
                                    ]
                                }
                            ]
                        }
                    }
                },
                "tolerations": [
                    {"key": "static-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "foo",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"tier": "airflow"}},
                    }
                ],
                "nodeSelector": {"type": "user-node"},
            },
            show_only=[
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/database-cleanup/database-cleanup-cronjob.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/jobs/create-user-job.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/redis/redis-statefulset.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
            ],
        )

        # Objects in show_only are 14 but only one of Webserver or API server is created so we have 13 objects
        assert len(k8s_objects) == 13

        for k8s_object in k8s_objects:
            if k8s_object["kind"] == "CronJob":
                podSpec = jmespath.search("spec.jobTemplate.spec.template.spec", k8s_object)
            else:
                podSpec = jmespath.search("spec.template.spec", k8s_object)

            assert (
                jmespath.search(
                    "affinity.nodeAffinity."
                    "requiredDuringSchedulingIgnoredDuringExecution."
                    "nodeSelectorTerms[0]."
                    "matchExpressions[0]."
                    "key",
                    podSpec,
                )
                == "foo"
            )
            assert jmespath.search("nodeSelector.type", podSpec) == "user-node"
            assert jmespath.search("tolerations[0].key", podSpec) == "static-pods"
            assert jmespath.search("topologySpreadConstraints[0].topologyKey", podSpec) == "foo"

    @pytest.mark.parametrize(
        ("expected_image", "tag", "digest"),
        [
            ("apache/airflow:user-tag", "user-tag", None),
            ("apache/airflow@user-digest", None, "user-digest"),
            ("apache/airflow@user-digest", "user-tag", "user-digest"),
        ],
    )
    def test_should_use_correct_image(self, expected_image, tag, digest):
        docs = render_chart(
            values={
                "images": {
                    "airflow": {
                        "repository": "apache/airflow",
                        "tag": tag,
                        "digest": digest,
                    },
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert expected_image == jmespath.search("spec.template.spec.initContainers[0].image", doc)

    @pytest.mark.parametrize(
        ("expected_image", "tag", "digest"),
        [
            ("apache/airflow:user-tag", "user-tag", None),
            ("apache/airflow@user-digest", None, "user-digest"),
            ("apache/airflow@user-digest", "user-tag", "user-digest"),
        ],
    )
    def test_should_use_correct_default_image(self, expected_image, tag, digest):
        docs = render_chart(
            values={
                "defaultAirflowRepository": "apache/airflow",
                "defaultAirflowTag": tag,
                "defaultAirflowDigest": digest,
                "images": {"useDefaultImageForMigration": True},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )

        for doc in docs:
            assert expected_image == jmespath.search("spec.template.spec.initContainers[0].image", doc)

    def test_should_set_correct_helm_hooks_weight(self):
        docs = render_chart(
            show_only=["templates/secrets/fernetkey-secret.yaml"],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["helm.sh/hook-weight"] == "0"

    def test_should_disable_some_variables(self):
        docs = render_chart(
            values={
                "enableBuiltInSecretEnvVars": {
                    "AIRFLOW__CORE__SQL_ALCHEMY_CONN": False,
                    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": False,
                    "AIRFLOW__API__SECRET_KEY": False,
                    "AIRFLOW__API_AUTH__JWT_SECRET": False,
                    "AIRFLOW__WEBSERVER__SECRET_KEY": False,
                    # the following vars only appear if remote logging is set, so disabling them in this test is kind of a no-op
                    "AIRFLOW__ELASTICSEARCH__HOST": False,
                    "AIRFLOW__ELASTICSEARCH__ELASTICSEARCH_HOST": False,
                    "AIRFLOW__OPENSEARCH__HOST": False,
                },
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )
        expected_vars = [
            "AIRFLOW__CORE__FERNET_KEY",
            "AIRFLOW_HOME",
            "AIRFLOW_CONN_AIRFLOW_DB",
            "AIRFLOW__CELERY__BROKER_URL",
        ]
        expected_vars_in_worker = ["DUMB_INIT_SETSID"] + expected_vars
        for doc in docs:
            component = doc["metadata"]["labels"]["component"]
            variables = expected_vars_in_worker if component == "worker" else expected_vars
            assert variables == jmespath.search("spec.template.spec.containers[0].env[*].name", doc), (
                f"Wrong vars in {component}"
            )

    def test_have_all_variables(self):
        docs = render_chart(
            values={},
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )
        expected_vars = [
            "AIRFLOW__CORE__FERNET_KEY",
            "AIRFLOW_HOME",
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "AIRFLOW_CONN_AIRFLOW_DB",
            "AIRFLOW__API__SECRET_KEY",
            "AIRFLOW__API_AUTH__JWT_SECRET",
            "AIRFLOW__CELERY__BROKER_URL",
        ]
        expected_vars_in_worker = ["DUMB_INIT_SETSID"] + expected_vars
        for doc in docs:
            component = doc["metadata"]["labels"]["component"]
            variables = expected_vars_in_worker if component == "worker" else expected_vars
            assert variables == jmespath.search("spec.template.spec.containers[0].env[*].name", doc), (
                f"Wrong vars in {component}"
            )

    def test_have_all_config_mounts_on_init_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/api-server/api-server-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )
        assert len(docs) == 5
        expected_mount = {
            "subPath": "airflow.cfg",
            "name": "config",
            "readOnly": True,
            "mountPath": "/opt/airflow/airflow.cfg",
        }
        for doc in docs:
            assert expected_mount in jmespath.search("spec.template.spec.initContainers[0].volumeMounts", doc)

    def test_priority_class_name(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor,KubernetesExecutor",
                "flower": {"enabled": True, "priorityClassName": "low-priority-flower"},
                "pgbouncer": {"enabled": True, "priorityClassName": "low-priority-pgbouncer"},
                "scheduler": {"priorityClassName": "low-priority-scheduler"},
                "statsd": {"priorityClassName": "low-priority-statsd"},
                "triggerer": {"priorityClassName": "low-priority-triggerer"},
                "dagProcessor": {"priorityClassName": "low-priority-dag-processor"},
                "webserver": {"priorityClassName": "low-priority-webserver"},
                "workers": {"priorityClassName": "low-priority-worker"},
                "cleanup": {"enabled": True, "priorityClassName": "low-priority-airflow-cleanup-pods"},
                "databaseCleanup": {"enabled": True, "priorityClassName": "low-priority-database-cleanup"},
                "migrateDatabaseJob": {"priorityClassName": "low-priority-run-airflow-migrations"},
                "createUserJob": {"priorityClassName": "low-priority-create-user-job"},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/database-cleanup/database-cleanup-cronjob.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/jobs/create-user-job.yaml",
            ],
        )

        assert len(docs) == 11
        for doc in docs:
            component = doc["metadata"]["labels"]["component"]
            if component in ["airflow-cleanup-pods", "database-cleanup"]:
                priority = doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]["priorityClassName"]
            else:
                priority = doc["spec"]["template"]["spec"]["priorityClassName"]

            assert priority == f"low-priority-{component}"

    @pytest.mark.parametrize(
        ("image_pull_secrets", "registry_secret_name", "registry_connection", "expected_image_pull_secrets"),
        [
            ([], None, {}, []),
            (
                [],
                None,
                {"host": "example.com", "user": "user", "pass": "pass", "email": "user@example.com"},
                ["test-basic-registry"],
            ),
            ([], "regcred", {}, ["regcred"]),
            (["regcred2"], "regcred", {}, ["regcred2"]),
            (
                ["regcred2"],
                None,
                {"host": "example.com", "user": "user", "pass": "pass", "email": "user@example.com"},
                ["regcred2"],
            ),
            (["regcred", {"name": "regcred2"}, ""], None, {}, ["regcred", "regcred2"]),
        ],
    )
    def test_image_pull_secrets(
        self, image_pull_secrets, registry_secret_name, registry_connection, expected_image_pull_secrets
    ):
        release_name = "test-basic"
        docs = render_chart(
            name=release_name,
            values={
                "imagePullSecrets": image_pull_secrets,
                "registry": {"secretName": registry_secret_name, "connection": registry_connection},
                "flower": {"enabled": True},
                "pgbouncer": {"enabled": True},
                "cleanup": {"enabled": True},
                "databaseCleanup": {"enabled": True},
            },
            show_only=[
                "templates/flower/flower-deployment.yaml",
                "templates/pgbouncer/pgbouncer-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/statsd/statsd-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/cleanup/cleanup-cronjob.yaml",
                "templates/database-cleanup/database-cleanup-cronjob.yaml",
                "templates/jobs/migrate-database-job.yaml",
                "templates/jobs/create-user-job.yaml",
            ],
        )

        expected_image_pull_secrets = [{"name": name} for name in expected_image_pull_secrets]

        for doc in docs:
            got_image_pull_secrets = (
                doc["spec"]["jobTemplate"]["spec"]["template"]["spec"]["imagePullSecrets"]
                if doc["kind"] == "CronJob"
                else doc["spec"]["template"]["spec"]["imagePullSecrets"]
            )
            assert got_image_pull_secrets == expected_image_pull_secrets
