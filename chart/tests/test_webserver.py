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

import unittest

import jmespath
from parameterized import parameterized

from tests.helm_template_generator import render_chart


class WebserverDeploymentTest(unittest.TestCase):
    def test_should_add_host_header_to_liveness_and_readiness_probes(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {"base_url": "https://example.com:21222/mypath/path"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {"name": "Host", "value": "example.com"} in jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0]
        )
        assert {"name": "Host", "value": "example.com"} in jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0]
        )

    def test_should_add_path_to_liveness_and_readiness_probes(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {"base_url": "https://example.com:21222/mypath/path"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.path", docs[0])
            == "/mypath/path/health"
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.path", docs[0])
            == "/mypath/path/health"
        )

    @parameterized.expand(
        [
            ({"config": {"webserver": {"base_url": ""}}},),
            ({},),
        ]
    )
    def test_should_not_contain_host_header(self, values):
        print(values)
        docs = render_chart(values=values, show_only=["templates/webserver/webserver-deployment.yaml"])

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders", docs[0])
            is None
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders", docs[0])
            is None
        )

    def test_should_use_templated_base_url_for_probes(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {
                        "base_url": "https://{{ .Release.Name }}.com:21222/mypath/{{ .Release.Name }}/path"
                    },
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        container = jmespath.search("spec.template.spec.containers[0]", docs[0])

        assert {"name": "Host", "value": "RELEASE-NAME.com"} in jmespath.search(
            "livenessProbe.httpGet.httpHeaders", container
        )
        assert {"name": "Host", "value": "RELEASE-NAME.com"} in jmespath.search(
            "readinessProbe.httpGet.httpHeaders", container
        )
        assert "/mypath/RELEASE-NAME/path/health" == jmespath.search("livenessProbe.httpGet.path", container)
        assert "/mypath/RELEASE-NAME/path/health" == jmespath.search("readinessProbe.httpGet.path", container)

    def test_should_add_volume_and_volume_mount_when_exist_webserver_config(self):
        docs = render_chart(
            values={"webserver": {"webserverConfig": "CSRF_ENABLED = True"}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "webserver-config",
            "configMap": {"name": "RELEASE-NAME-webserver-config"},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

        assert {
            "name": "webserver-config",
            "mountPath": "/opt/airflow/webserver_config.py",
            "subPath": "webserver_config.py",
            "readOnly": True,
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "webserver": {
                    "extraContainers": [
                        {"name": "test-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "test-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "webserver": {
                    "extraInitContainers": [
                        {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "webserver": {
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
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
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

    @parameterized.expand(
        [
            ({"enabled": False}, None),
            ({"enabled": True}, "RELEASE-NAME-logs"),
            ({"enabled": True, "existingClaim": "test-claim"}, "test-claim"),
        ]
    )
    def test_logs_persistence_adds_volume_and_mount(self, log_persistence_values, expected_claim_name):
        docs = render_chart(
            values={"logs": {"persistence": log_persistence_values}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        if expected_claim_name:
            assert {
                "name": "logs",
                "persistentVolumeClaim": {"claimName": expected_claim_name},
            } == jmespath.search("spec.template.spec.volumes[1]", docs[0])
            assert {
                "name": "logs",
                "mountPath": "/opt/airflow/logs",
            } == jmespath.search("spec.template.spec.containers[0].volumeMounts[1]", docs[0])
        else:
            assert "logs" not in [v["name"] for v in jmespath.search("spec.template.spec.volumes", docs[0])]
            assert "logs" not in [
                v["name"] for v in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
            ]

    def test_webserver_resources_are_configurable(self):
        docs = render_chart(
            values={
                "webserver": {
                    "resources": {
                        "limits": {"cpu": "200m", 'memory': "128Mi"},
                        "requests": {"cpu": "300m", 'memory': "169Mi"},
                    }
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert "128Mi" == jmespath.search("spec.template.spec.containers[0].resources.limits.memory", docs[0])
        assert "169Mi" == jmespath.search(
            "spec.template.spec.containers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", docs[0])

    def test_webserver_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    @parameterized.expand(
        [
            ("2.0.2", {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}}),
            ("1.10.14", {"type": "Recreate"}),
            ("1.9.0", {"type": "Recreate"}),
            ("2.1.0", {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}}),
        ],
    )
    def test_default_update_strategy(self, airflow_version, expected_strategy):
        docs = render_chart(
            values={"airflowVersion": airflow_version},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_update_strategy(self):
        expected_strategy = {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 1}}
        docs = render_chart(
            values={"webserver": {"strategy": expected_strategy}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_no_airflow_local_settings_by_default(self):
        docs = render_chart(show_only=["templates/webserver/webserver-deployment.yaml"])
        volume_mounts = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])


class WebserverServiceTest(unittest.TestCase):
    def test_default_service(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-service.yaml"],
        )

        assert "RELEASE-NAME-webserver" == jmespath.search("metadata.name", docs[0])
        assert jmespath.search("metadata.annotations", docs[0]) is None
        assert {"tier": "airflow", "component": "webserver", "release": "RELEASE-NAME"} == jmespath.search(
            "spec.selector", docs[0]
        )
        assert "ClusterIP" == jmespath.search("spec.type", docs[0])
        assert {"name": "airflow-ui", "protocol": "TCP", "port": 8080} in jmespath.search(
            "spec.ports", docs[0]
        )

    def test_overrides(self):
        docs = render_chart(
            values={
                "ports": {"airflowUI": 9000},
                "webserver": {
                    "service": {
                        "type": "LoadBalancer",
                        "loadBalancerIP": "127.0.0.1",
                        "annotations": {"foo": "bar"},
                    }
                },
            },
            show_only=["templates/webserver/webserver-service.yaml"],
        )

        assert {"foo": "bar"} == jmespath.search("metadata.annotations", docs[0])
        assert "LoadBalancer" == jmespath.search("spec.type", docs[0])
        assert {"name": "airflow-ui", "protocol": "TCP", "port": 9000} in jmespath.search(
            "spec.ports", docs[0]
        )
        assert "127.0.0.1" == jmespath.search("spec.loadBalancerIP", docs[0])


class WebserverConfigmapTest(unittest.TestCase):
    def test_no_webserver_config_configmap_by_default(self):
        docs = render_chart(show_only=["templates/configmaps/webserver-configmap.yaml"])
        assert 0 == len(docs)

    def test_webserver_config_configmap(self):
        docs = render_chart(
            values={"webserver": {"webserverConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}"}},
            show_only=["templates/configmaps/webserver-configmap.yaml"],
        )

        assert "ConfigMap" == docs[0]["kind"]
        assert "RELEASE-NAME-webserver-config" == jmespath.search("metadata.name", docs[0])
        assert (
            "CSRF_ENABLED = True  # RELEASE-NAME"
            == jmespath.search('data."webserver_config.py"', docs[0]).strip()
        )
