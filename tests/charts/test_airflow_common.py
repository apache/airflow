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
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class TestAirflowCommon:
    """
    This class holds tests that apply to more than 1 Airflow component so
    we don't have to repeat tests everywhere

    The one general exception will be the KubernetesExecutor PodTemplateFile,
    as it requires extra test setup.
    """

    @parameterized.expand(
        [
            (
                {"gitSync": {"enabled": True}},
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
                    "gitSync": {"enabled": True},
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
        ]
    )
    def test_dags_mount(self, dag_values, expected_mount):
        docs = render_chart(
            values={
                "dags": dag_values,
                "airflowVersion": "1.10.15",
            },  # airflowVersion is present so webserver gets the mount
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
            ],
        )

        assert 3 == len(docs)
        for doc in docs:
            assert expected_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", doc)

    def test_annotations(self):
        """
        Test Annotations are correctly applied on all pods created Scheduler, Webserver & Worker
        deployments.
        """
        release_name = "test-basic"
        k8s_objects = render_chart(
            name=release_name,
            values={
                "airflowPodAnnotations": {"test-annotation/safe-to-evict": "true"},
                "cleanup": {"enabled": True},
                "flower": {"enabled": True},
                "dagProcessor": {"enabled": True},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/flower/flower-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/cleanup/cleanup-cronjob.yaml",
            ],
        )

        assert 7 == len(k8s_objects)

        for k8s_object in k8s_objects:
            if k8s_object['kind'] == 'CronJob':
                annotations = k8s_object["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["annotations"]
            else:
                annotations = k8s_object["spec"]["template"]["metadata"]["annotations"]

            assert "test-annotation/safe-to-evict" in annotations
            assert "true" in annotations["test-annotation/safe-to-evict"]

    def test_global_affinity_tolerations_topology_spread_constraints_and_node_selector(self):
        """
        Test affinity, tolerations, etc are correctly applied on all pods created
        """
        k8s_objects = render_chart(
            values={
                "cleanup": {"enabled": True},
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
                "templates/workers/worker-deployment.yaml",
            ],
        )

        assert 12 == len(k8s_objects)

        for k8s_object in k8s_objects:
            if k8s_object["kind"] == "CronJob":
                podSpec = jmespath.search("spec.jobTemplate.spec.template.spec", k8s_object)
            else:
                podSpec = jmespath.search("spec.template.spec", k8s_object)

            assert "foo" == jmespath.search(
                "affinity.nodeAffinity."
                "requiredDuringSchedulingIgnoredDuringExecution."
                "nodeSelectorTerms[0]."
                "matchExpressions[0]."
                "key",
                podSpec,
            )
            assert "user-node" == jmespath.search("nodeSelector.type", podSpec)
            assert "static-pods" == jmespath.search("tolerations[0].key", podSpec)
            assert "foo" == jmespath.search("topologySpreadConstraints[0].topologyKey", podSpec)

    @pytest.mark.parametrize(
        "use_default_image,expected_image",
        [
            (True, "apache/airflow:2.1.0"),
            (False, "apache/airflow:user-image"),
        ],
    )
    def test_should_use_correct_image(self, use_default_image, expected_image):
        docs = render_chart(
            values={
                "defaultAirflowRepository": "apache/airflow",
                "defaultAirflowTag": "2.1.0",
                "images": {
                    "airflow": {
                        "repository": "apache/airflow",
                        "tag": "user-image",
                    },
                    "useDefaultImageForMigration": use_default_image,
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
                    "AIRFLOW__WEBSERVER__SECRET_KEY": False,
                    "AIRFLOW__ELASTICSEARCH__HOST": False,
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
            'AIRFLOW__CORE__FERNET_KEY',
            'AIRFLOW_CONN_AIRFLOW_DB',
            'AIRFLOW__CELERY__BROKER_URL',
        ]
        expected_vars_in_worker = ['DUMB_INIT_SETSID'] + expected_vars
        for doc in docs:
            component = doc['metadata']['labels']['component']
            variables = expected_vars_in_worker if component == 'worker' else expected_vars
            assert variables == jmespath.search(
                "spec.template.spec.containers[0].env[*].name", doc
            ), f"Wrong vars in {component}"

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
            'AIRFLOW__CORE__FERNET_KEY',
            'AIRFLOW__CORE__SQL_ALCHEMY_CONN',
            'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN',
            'AIRFLOW_CONN_AIRFLOW_DB',
            'AIRFLOW__WEBSERVER__SECRET_KEY',
            'AIRFLOW__CELERY__BROKER_URL',
        ]
        expected_vars_in_worker = ['DUMB_INIT_SETSID'] + expected_vars
        for doc in docs:
            component = doc['metadata']['labels']['component']
            variables = expected_vars_in_worker if component == 'worker' else expected_vars
            assert variables == jmespath.search(
                "spec.template.spec.containers[0].env[*].name", doc
            ), f"Wrong vars in {component}"

    def test_have_all_config_mounts_on_init_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True},
            },
            show_only=[
                "templates/scheduler/scheduler-deployment.yaml",
                "templates/workers/worker-deployment.yaml",
                "templates/webserver/webserver-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/dag-processor/dag-processor-deployment.yaml",
            ],
        )
        assert 5 == len(docs)
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
                "flower": {"enabled": True, "priorityClassName": "low-priority-flower"},
                "pgbouncer": {"enabled": True, "priorityClassName": "low-priority-pgbouncer"},
                "scheduler": {"priorityClassName": "low-priority-scheduler"},
                "statsd": {"priorityClassName": "low-priority-statsd"},
                "triggerer": {"priorityClassName": "low-priority-triggerer"},
                "dagProcessor": {"priorityClassName": "low-priority-dag-processor"},
                "webserver": {"priorityClassName": "low-priority-webserver"},
                "workers": {"priorityClassName": "low-priority-worker"},
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
            ],
        )

        assert 7 == len(docs)
        for doc in docs:
            component = doc['metadata']['labels']['component']
            priority = doc['spec']['template']['spec']['priorityClassName']

            assert priority == f"low-priority-{component}"
