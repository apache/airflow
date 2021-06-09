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

import jmespath
import pytest

from tests.helm_template_generator import render_chart


class TestFlowerDeployment:
    @pytest.mark.parametrize(
        "executor,flower_enabled,created",
        [
            ("CeleryExecutor", False, False),
            ("CeleryKubernetesExecutor", False, False),
            ("KubernetesExecutor", False, False),
            ("CeleryExecutor", True, True),
            ("CeleryKubernetesExecutor", True, True),
            ("KubernetesExecutor", True, False),
        ],
    )
    def test_create_flower(self, executor, flower_enabled, created):
        docs = render_chart(
            values={"executor": executor, "flower": {"enabled": flower_enabled}},
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert bool(docs) is created
        if created:
            assert "RELEASE-NAME-flower" == jmespath.search("metadata.name", docs[0])
            assert "flower" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

    @pytest.mark.parametrize(
        "airflow_version, expected_arg",
        [
            ("2.0.2", "airflow celery flower"),
            ("1.10.14", "airflow flower"),
            ("1.9.0", "airflow flower"),
            ("2.1.0", "airflow celery flower"),
        ],
    )
    def test_args_with_airflow_version(self, airflow_version, expected_arg):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "flower": {"enabled": True},
                "airflowVersion": airflow_version,
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            f"exec \\\n{expected_arg}",
        ]

    @pytest.mark.parametrize(
        "command, args",
        [
            (None, None),
            (None, ["custom", "args"]),
            (["custom", "command"], None),
            (["custom", "command"], ["custom", "args"]),
        ],
    )
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"flower": {"command": command, "args": args}},
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={"flower": {"command": ["{{ .Release.Name }}"], "args": ["{{ .Release.Service }}"]}},
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert ["RELEASE-NAME"] == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert ["Helm"] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_should_create_flower_deployment_with_authorization(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "flower": {"username": "flower", "password": "fl0w3r"},
                "ports": {"flowerUI": 7777},
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert "AIRFLOW__CELERY__FLOWER_BASIC_AUTH" == jmespath.search(
            "spec.template.spec.containers[0].env[0].name", docs[0]
        )
        assert ['curl', '--user', '$AIRFLOW__CELERY__FLOWER_BASIC_AUTH', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]
        )
        assert ['curl', '--user', '$AIRFLOW__CELERY__FLOWER_BASIC_AUTH', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.exec.command", docs[0]
        )

    def test_should_create_flower_deployment_without_authorization(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "ports": {"flowerUI": 7777},
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert "AIRFLOW__CORE__FERNET_KEY" == jmespath.search(
            "spec.template.spec.containers[0].env[0].name", docs[0]
        )
        assert ['curl', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]
        )
        assert ['curl', 'localhost:7777'] == jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.exec.command", docs[0]
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "flower": {
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
                        {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                    ],
                    "nodeSelector": {"diskType": "ssd"},
                },
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )

        assert "Deployment" == jmespath.search("kind", docs[0])
        assert "foo" == jmespath.search(
            "spec.template.spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.template.spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.template.spec.tolerations[0].key",
            docs[0],
        )

    def test_flower_resources_are_configurable(self):
        docs = render_chart(
            values={
                "flower": {
                    "resources": {
                        "limits": {"cpu": "200m", 'memory': "128Mi"},
                        "requests": {"cpu": "300m", 'memory': "169Mi"},
                    }
                },
            },
            show_only=["templates/flower/flower-deployment.yaml"],
        )
        assert "128Mi" == jmespath.search("spec.template.spec.containers[0].resources.limits.memory", docs[0])
        assert "169Mi" == jmespath.search(
            "spec.template.spec.containers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", docs[0])

    def test_flower_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/flower/flower-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}


class TestFlowerService:
    @pytest.mark.parametrize(
        "executor,flower_enabled,created",
        [
            ("CeleryExecutor", False, False),
            ("CeleryKubernetesExecutor", False, False),
            ("KubernetesExecutor", False, False),
            ("CeleryExecutor", True, True),
            ("CeleryKubernetesExecutor", True, True),
            ("KubernetesExecutor", True, False),
        ],
    )
    def test_create_flower(self, executor, flower_enabled, created):
        docs = render_chart(
            values={"executor": executor, "flower": {"enabled": flower_enabled}},
            show_only=["templates/flower/flower-service.yaml"],
        )

        assert bool(docs) is created
        if created:
            assert "RELEASE-NAME-flower" == jmespath.search("metadata.name", docs[0])

    def test_default_service(self):
        docs = render_chart(
            show_only=["templates/flower/flower-service.yaml"],
        )

        assert "RELEASE-NAME-flower" == jmespath.search("metadata.name", docs[0])
        assert jmespath.search("metadata.annotations", docs[0]) is None
        assert {"tier": "airflow", "component": "flower", "release": "RELEASE-NAME"} == jmespath.search(
            "spec.selector", docs[0]
        )
        assert "ClusterIP" == jmespath.search("spec.type", docs[0])
        assert {"name": "flower-ui", "protocol": "TCP", "port": 5555} in jmespath.search(
            "spec.ports", docs[0]
        )

    def test_overrides(self):
        docs = render_chart(
            values={
                "ports": {"flowerUI": 9000},
                "flower": {
                    "service": {
                        "type": "LoadBalancer",
                        "loadBalancerIP": "127.0.0.1",
                        "annotations": {"foo": "bar"},
                    }
                },
            },
            show_only=["templates/flower/flower-service.yaml"],
        )

        assert {"foo": "bar"} == jmespath.search("metadata.annotations", docs[0])
        assert "LoadBalancer" == jmespath.search("spec.type", docs[0])
        assert {"name": "flower-ui", "protocol": "TCP", "port": 9000} in jmespath.search(
            "spec.ports", docs[0]
        )
        assert "127.0.0.1" == jmespath.search("spec.loadBalancerIP", docs[0])
