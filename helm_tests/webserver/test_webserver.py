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


class TestWebserverDeployment:
    """Tests webserver deployment."""

    def test_can_be_disabled(self):
        """
        Webserver should be able to be disabled if the users desires.

        For example, user may be disabled when using webserver and having it deployed on another host.
        """
        docs = render_chart(
            values={"webserver": {"enabled": False}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert 0 == len(docs)

    def test_should_remove_replicas_field(self):
        docs = render_chart(
            values={
                "webserver": {
                    "hpa": {"enabled": True},
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert "replicas" not in jmespath.search("spec", docs[0])

    def test_should_not_remove_replicas_field(self):
        docs = render_chart(
            values={
                "webserver": {
                    "hpa": {"enabled": False},
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert "replicas" in jmespath.search("spec", docs[0])

    def test_should_add_host_header_to_liveness_and_readiness_and_startup_probes(self):
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
        assert {"name": "Host", "value": "example.com"} in jmespath.search(
            "spec.template.spec.containers[0].startupProbe.httpGet.httpHeaders", docs[0]
        )

    def test_should_add_path_to_liveness_and_readiness_and_startup_probes(self):
        docs = render_chart(
            values={
                "config": {
                    "webserver": {"base_url": "https://example.com:21222/mypath/path"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.containers[0].livenessProbe.httpGet.path", docs[0]
            )
            == "/mypath/path/health"
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].readinessProbe.httpGet.path", docs[0]
            )
            == "/mypath/path/health"
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].startupProbe.httpGet.path", docs[0]
            )
            == "/mypath/path/health"
        )

    @pytest.mark.parametrize(
        "revision_history_limit, global_revision_history_limit",
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(
        self, revision_history_limit, global_revision_history_limit
    ):
        values = {"webserver": {}}
        if revision_history_limit:
            values["webserver"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        expected_result = (
            revision_history_limit
            if revision_history_limit
            else global_revision_history_limit
        )
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    @pytest.mark.parametrize("values", [{"config": {"webserver": {"base_url": ""}}}, {}])
    def test_should_not_contain_host_header(self, values):
        print(values)
        docs = render_chart(
            values=values, show_only=["templates/webserver/webserver-deployment.yaml"]
        )

        assert (
            jmespath.search(
                "spec.template.spec.containers[0].livenessProbe.httpGet.httpHeaders",
                docs[0],
            )
            is None
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].readinessProbe.httpGet.httpHeaders",
                docs[0],
            )
            is None
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].startupProbe.httpGet.httpHeaders",
                docs[0],
            )
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

        assert {"name": "Host", "value": "release-name.com"} in jmespath.search(
            "livenessProbe.httpGet.httpHeaders", container
        )
        assert {"name": "Host", "value": "release-name.com"} in jmespath.search(
            "readinessProbe.httpGet.httpHeaders", container
        )
        assert {"name": "Host", "value": "release-name.com"} in jmespath.search(
            "startupProbe.httpGet.httpHeaders", container
        )
        assert "/mypath/release-name/path/health" == jmespath.search(
            "livenessProbe.httpGet.path", container
        )
        assert "/mypath/release-name/path/health" == jmespath.search(
            "readinessProbe.httpGet.path", container
        )
        assert "/mypath/release-name/path/health" == jmespath.search(
            "startupProbe.httpGet.path", container
        )

    def test_should_add_scheme_to_liveness_and_readiness_and_startup_probes(self):
        docs = render_chart(
            values={
                "webserver": {
                    "livenessProbe": {"scheme": "HTTPS"},
                    "readinessProbe": {"scheme": "HTTPS"},
                    "startupProbe": {"scheme": "HTTPS"},
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "HTTPS" in jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.httpGet.scheme", docs[0]
        )
        assert "HTTPS" in jmespath.search(
            "spec.template.spec.containers[0].readinessProbe.httpGet.scheme", docs[0]
        )
        assert "HTTPS" in jmespath.search(
            "spec.template.spec.containers[0].startupProbe.httpGet.scheme", docs[0]
        )

    def test_should_add_volume_and_volume_mount_when_exist_webserver_config(self):
        docs = render_chart(
            values={"webserver": {"webserverConfig": "CSRF_ENABLED = True"}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "webserver-config",
            "configMap": {"name": "release-name-webserver-config"},
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
                        {
                            "name": "{{.Chart.Name}}",
                            "image": "test-registry/test-repo:test-tag",
                        }
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "webserver": {
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "release-name-test-container",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "webserver": {
                    "env": [
                        {"name": "TEST_ENV_1", "value": "test_env_1"},
                        {
                            "name": "TEST_ENV_2",
                            "valueFrom": {
                                "secretKeyRef": {"name": "my-secret", "key": "my-key"}
                            },
                        },
                        {
                            "name": "TEST_ENV_3",
                            "valueFrom": {
                                "configMapKeyRef": {
                                    "name": "my-config-map",
                                    "key": "my-key",
                                }
                            },
                        },
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )
        assert {
            "name": "TEST_ENV_2",
            "valueFrom": {"secretKeyRef": {"name": "my-secret", "key": "my-key"}},
        } in jmespath.search("spec.template.spec.containers[0].env", docs[0])
        assert {
            "name": "TEST_ENV_3",
            "valueFrom": {"configMapKeyRef": {"name": "my-config-map", "key": "my-key"}},
        } in jmespath.search("spec.template.spec.containers[0].env", docs[0])

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "webserver": {
                    "extraVolumes": [
                        {"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}
                    ],
                    "extraVolumeMounts": [
                        {
                            "name": "test-volume-{{ .Chart.Name }}",
                            "mountPath": "/opt/test",
                        }
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "test-volume-airflow" == jmespath.search(
            "spec.template.spec.volumes[-1].name", docs[0]
        )
        assert "test-volume-airflow" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[-1].name", docs[0]
        )
        assert "test-volume-airflow" == jmespath.search(
            "spec.template.spec.initContainers[0].volumeMounts[-1].name", docs[0]
        )

    def test_should_add_global_volume_and_global_volume_mount(self):
        docs = render_chart(
            values={
                "volumes": [{"name": "test-volume", "emptyDir": {}}],
                "volumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "test-volume" == jmespath.search(
            "spec.template.spec.volumes[-1].name", docs[0]
        )
        assert "test-volume" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[-1].name", docs[0]
        )

    def test_should_add_extraEnvs_to_wait_for_migration_container(self):
        docs = render_chart(
            values={
                "webserver": {
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                    },
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    @pytest.mark.parametrize(
        "airflow_version, expected_arg",
        [
            (
                "2.0.0",
                ["airflow", "db", "check-migrations", "--migration-wait-timeout=60"],
            ),
            (
                "2.1.0",
                ["airflow", "db", "check-migrations", "--migration-wait-timeout=60"],
            ),
            ("1.10.2", ["python", "-c"]),
        ],
    )
    def test_wait_for_migration_airflow_version(self, airflow_version, expected_arg):
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        # Don't test the full string, just the length of the expect matches
        actual = jmespath.search("spec.template.spec.initContainers[0].args", docs[0])
        assert expected_arg == actual[: len(expected_arg)]

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "webserver": {
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']",
            docs[0],
        )
        assert actual is None

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "webserver": {
                    "extraInitContainers": [
                        {
                            "name": "test-init-container",
                            "image": "test-registry/test-repo:test-tag",
                        }
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "webserver": {
                    "extraInitContainers": [
                        {"name": "{{ .Release.Name }}-init-container"}
                    ],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "release-name-init-container",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "webserver": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

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
                                            {
                                                "key": "foo",
                                                "operator": "In",
                                                "values": ["true"],
                                            },
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    "tolerations": [
                        {
                            "key": "dynamic-pods",
                            "operator": "Equal",
                            "value": "true",
                            "effect": "NoSchedule",
                        }
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

    def test_should_create_default_affinity(self):
        docs = render_chart(show_only=["templates/webserver/webserver-deployment.yaml"])

        assert {"component": "webserver"} == jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity."
            "preferredDuringSchedulingIgnoredDuringExecution[0]."
            "podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        )

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(
        self,
    ):
        """When given both global and webserver affinity etc, webserver affinity etc is used."""
        expected_affinity = {
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
        }
        expected_topology_spread_constraints = {
            "maxSkew": 1,
            "topologyKey": "foo",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": {"tier": "airflow"}},
        }
        docs = render_chart(
            values={
                "webserver": {
                    "affinity": expected_affinity,
                    "tolerations": [
                        {
                            "key": "dynamic-pods",
                            "operator": "Equal",
                            "value": "true",
                            "effect": "NoSchedule",
                        }
                    ],
                    "topologySpreadConstraints": [expected_topology_spread_constraints],
                    "nodeSelector": {"type": "ssd"},
                },
                "affinity": {
                    "nodeAffinity": {
                        "preferredDuringSchedulingIgnoredDuringExecution": [
                            {
                                "weight": 1,
                                "preference": {
                                    "matchExpressions": [
                                        {
                                            "key": "not-me",
                                            "operator": "In",
                                            "values": ["true"],
                                        },
                                    ]
                                },
                            }
                        ]
                    }
                },
                "tolerations": [
                    {
                        "key": "not-me",
                        "operator": "Equal",
                        "value": "true",
                        "effect": "NoSchedule",
                    }
                ],
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "not-me",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"tier": "airflow"}},
                    }
                ],
                "nodeSelector": {"type": "not-me"},
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert expected_affinity == jmespath.search(
            "spec.template.spec.affinity", docs[0]
        )
        assert "ssd" == jmespath.search(
            "spec.template.spec.nodeSelector.type",
            docs[0],
        )
        tolerations = jmespath.search("spec.template.spec.tolerations", docs[0])
        assert 1 == len(tolerations)
        assert "dynamic-pods" == tolerations[0]["key"]
        assert expected_topology_spread_constraints == jmespath.search(
            "spec.template.spec.topologySpreadConstraints[0]", docs[0]
        )

    def test_scheduler_name(self):
        docs = render_chart(
            values={"schedulerName": "airflow-scheduler"},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "airflow-scheduler" == jmespath.search(
            "spec.template.spec.schedulerName",
            docs[0],
        )

    @pytest.mark.parametrize(
        "log_persistence_values, expected_claim_name",
        [
            ({"enabled": False}, None),
            ({"enabled": True}, "release-name-logs"),
            ({"enabled": True, "existingClaim": "test-claim"}, "test-claim"),
        ],
    )
    def test_logs_persistence_adds_volume_and_mount(
        self, log_persistence_values, expected_claim_name
    ):
        docs = render_chart(
            values={"logs": {"persistence": log_persistence_values}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        if expected_claim_name:
            assert {
                "name": "logs",
                "persistentVolumeClaim": {"claimName": expected_claim_name},
            } in jmespath.search("spec.template.spec.volumes", docs[0])
            assert {
                "name": "logs",
                "mountPath": "/opt/airflow/logs",
            } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        else:
            assert "logs" not in [
                v["name"] for v in jmespath.search("spec.template.spec.volumes", docs[0])
            ]
            assert "logs" not in [
                v["name"]
                for v in jmespath.search(
                    "spec.template.spec.containers[0].volumeMounts", docs[0]
                )
            ]

    @pytest.mark.parametrize(
        "af_version, pod_template_file_expected",
        [
            ("1.10.10", False),
            ("1.10.12", True),
            ("2.1.0", True),
        ],
    )
    def test_config_volumes_and_mounts(self, af_version, pod_template_file_expected):
        # setup
        docs = render_chart(
            values={"airflowVersion": af_version},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        # default config
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "readOnly": True,
            "subPath": "airflow.cfg",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

        # pod_template_file config
        assert pod_template_file_expected == (
            {
                "name": "config",
                "mountPath": "/opt/airflow/pod_templates/pod_template_file.yaml",
                "readOnly": True,
                "subPath": "pod_template_file.yaml",
            }
            in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        )

    def test_webserver_resources_are_configurable(self):
        docs = render_chart(
            values={
                "webserver": {
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    }
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert "128Mi" == jmespath.search(
            "spec.template.spec.containers[0].resources.limits.memory", docs[0]
        )
        assert "200m" == jmespath.search(
            "spec.template.spec.containers[0].resources.limits.cpu", docs[0]
        )

        assert "169Mi" == jmespath.search(
            "spec.template.spec.containers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search(
            "spec.template.spec.containers[0].resources.requests.cpu", docs[0]
        )

        # initContainer wait-for-airflow-migrations
        assert "128Mi" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.limits.memory", docs[0]
        )
        assert "200m" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.limits.cpu", docs[0]
        )

        assert "169Mi" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.requests.cpu", docs[0]
        )

    def test_webserver_security_contexts_are_configurable(self):
        docs = render_chart(
            values={
                "webserver": {
                    "securityContexts": {
                        "pod": {
                            "fsGroup": 1000,
                            "runAsGroup": 1001,
                            "runAsNonRoot": True,
                            "runAsUser": 2000,
                        },
                        "container": {
                            "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True,
                        },
                    }
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
        } == jmespath.search("spec.template.spec.containers[0].securityContext", docs[0])

        assert {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        } == jmespath.search("spec.template.spec.securityContext", docs[0])

    def test_webserver_security_context_legacy(self):
        docs = render_chart(
            values={
                "webserver": {
                    "securityContext": {
                        "fsGroup": 1000,
                        "runAsGroup": 1001,
                        "runAsNonRoot": True,
                        "runAsUser": 2000,
                    }
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        } == jmespath.search("spec.template.spec.securityContext", docs[0])

    def test_webserver_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}
        )
        assert (
            jmespath.search("spec.template.spec.initContainers[0].resources", docs[0])
            == {}
        )

    @pytest.mark.parametrize(
        "airflow_version, expected_strategy",
        [
            (
                "2.0.2",
                {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0},
                },
            ),
            ("1.10.14", {"type": "Recreate"}),
            ("1.9.0", {"type": "Recreate"}),
            (
                "2.1.0",
                {
                    "type": "RollingUpdate",
                    "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0},
                },
            ),
        ],
    )
    def test_default_update_strategy(self, airflow_version, expected_strategy):
        docs = render_chart(
            values={"airflowVersion": airflow_version},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_update_strategy(self):
        expected_strategy = {
            "type": "RollingUpdate",
            "rollingUpdate": {"maxUnavailable": 1},
        }
        docs = render_chart(
            values={"webserver": {"strategy": expected_strategy}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": None},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        volume_mounts = jmespath.search(
            "spec.template.spec.containers[0].volumeMounts", docs[0]
        )
        assert "airflow_local_settings.py" not in str(volume_mounts)
        volume_mounts_init = jmespath.search(
            "spec.template.spec.containers[0].volumeMounts", docs[0]
        )
        assert "airflow_local_settings.py" not in str(volume_mounts_init)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        volume_mount = {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        }
        assert volume_mount in jmespath.search(
            "spec.template.spec.containers[0].volumeMounts", docs[0]
        )
        assert volume_mount in jmespath.search(
            "spec.template.spec.initContainers[0].volumeMounts", docs[0]
        )

    def test_default_command_and_args(self):
        docs = render_chart(show_only=["templates/webserver/webserver-deployment.yaml"])

        assert (
            jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        )
        assert ["bash", "-c", "exec airflow webserver"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"webserver": {"command": command, "args": args}},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert command == jmespath.search(
            "spec.template.spec.containers[0].command", docs[0]
        )
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "webserver": {
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert ["release-name"] == jmespath.search(
            "spec.template.spec.containers[0].command", docs[0]
        )
        assert ["Helm"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

    @pytest.mark.parametrize(
        "airflow_version, dag_values",
        [
            ("1.10.15", {"gitSync": {"enabled": False}}),
            ("1.10.15", {"persistence": {"enabled": False}}),
            (
                "1.10.15",
                {"gitSync": {"enabled": False}, "persistence": {"enabled": False}},
            ),
            ("2.0.0", {"gitSync": {"enabled": True}}),
            ("2.0.0", {"gitSync": {"enabled": False}}),
            ("2.0.0", {"persistence": {"enabled": True}}),
            ("2.0.0", {"persistence": {"enabled": False}}),
            ("2.0.0", {"gitSync": {"enabled": True}, "persistence": {"enabled": True}}),
        ],
    )
    def test_no_dags_mount_or_volume_or_gitsync_sidecar_expected(
        self, airflow_version, dag_values
    ):
        docs = render_chart(
            values={"dags": dag_values, "airflowVersion": airflow_version},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "dags" not in [
            vm["name"]
            for vm in jmespath.search(
                "spec.template.spec.containers[0].volumeMounts", docs[0]
            )
        ]
        assert "dags" not in [
            vm["name"] for vm in jmespath.search("spec.template.spec.volumes", docs[0])
        ]
        assert 1 == len(jmespath.search("spec.template.spec.containers", docs[0]))

    @pytest.mark.parametrize(
        "airflow_version, dag_values, expected_read_only",
        [
            ("1.10.15", {"gitSync": {"enabled": True}}, True),
            ("1.10.15", {"persistence": {"enabled": True}}, False),
            (
                "1.10.15",
                {"gitSync": {"enabled": True}, "persistence": {"enabled": True}},
                True,
            ),
        ],
    )
    def test_dags_mount(self, airflow_version, dag_values, expected_read_only):
        docs = render_chart(
            values={"dags": dag_values, "airflowVersion": airflow_version},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "mountPath": "/opt/airflow/dags",
            "name": "dags",
            "readOnly": expected_read_only,
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_dags_gitsync_volume_and_sidecar_and_init_container(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}, "airflowVersion": "1.10.15"},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {"name": "dags", "emptyDir": {}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )
        assert "git-sync" in [
            c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])
        ]
        assert "git-sync-init" in [
            c["name"]
            for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    @pytest.mark.parametrize(
        "dags_values, expected_claim_name",
        [
            ({"persistence": {"enabled": True}}, "release-name-dags"),
            (
                {"persistence": {"enabled": True, "existingClaim": "test-claim"}},
                "test-claim",
            ),
            (
                {"persistence": {"enabled": True}, "gitSync": {"enabled": True}},
                "release-name-dags",
            ),
        ],
    )
    def test_dags_persistence_volume_no_sidecar(self, dags_values, expected_claim_name):
        docs = render_chart(
            values={"dags": dags_values, "airflowVersion": "1.10.15"},
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "dags",
            "persistentVolumeClaim": {"claimName": expected_claim_name},
        } in jmespath.search("spec.template.spec.volumes", docs[0])
        # No gitsync sidecar or init container
        assert 1 == len(jmespath.search("spec.template.spec.containers", docs[0]))
        assert 1 == len(jmespath.search("spec.template.spec.initContainers", docs[0]))

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "webserver": {
                    "annotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert (
            jmespath.search("metadata.annotations", docs[0])["test_annotation"]
            == "test_annotation_value"
        )

    def test_webserver_pod_hostaliases(self):
        docs = render_chart(
            values={
                "webserver": {
                    "hostAliases": [{"ip": "127.0.0.1", "hostnames": ["foo.local"]}],
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert "127.0.0.1" == jmespath.search(
            "spec.template.spec.hostAliases[0].ip", docs[0]
        )
        assert "foo.local" == jmespath.search(
            "spec.template.spec.hostAliases[0].hostnames[0]", docs[0]
        )

    def test_should_add_annotations_to_webserver_configmap(self):
        docs = render_chart(
            values={
                "webserver": {
                    "webserverConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}",
                    "configMapAnnotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/configmaps/webserver-configmap.yaml"],
        )

        assert "annotations" in jmespath.search("metadata", docs[0])
        assert (
            jmespath.search("metadata.annotations", docs[0])["test_annotation"]
            == "test_annotation_value"
        )

    @pytest.mark.parametrize(
        "webserver_values, expected",
        [
            ({}, 30),
            ({"webserver": {"terminationGracePeriodSeconds": 1200}}, 1200),
        ],
    )
    def test_webserver_termination_grace_period_seconds(self, webserver_values, expected):
        docs = render_chart(
            values=webserver_values,
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert expected == jmespath.search(
            "spec.template.spec.terminationGracePeriodSeconds", docs[0]
        )


class TestWebserverService:
    """Tests webserver service."""

    def test_default_service(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-service.yaml"],
        )

        assert "release-name-webserver" == jmespath.search("metadata.name", docs[0])
        assert jmespath.search("metadata.annotations", docs[0]) is None
        assert {
            "tier": "airflow",
            "component": "webserver",
            "release": "release-name",
        } == jmespath.search("spec.selector", docs[0])
        assert "ClusterIP" == jmespath.search("spec.type", docs[0])
        assert {"name": "airflow-ui", "port": 8080} in jmespath.search(
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
                        "loadBalancerSourceRanges": ["10.123.0.0/16"],
                    }
                },
            },
            show_only=["templates/webserver/webserver-service.yaml"],
        )

        assert {"foo": "bar"} == jmespath.search("metadata.annotations", docs[0])
        assert "LoadBalancer" == jmespath.search("spec.type", docs[0])
        assert {"name": "airflow-ui", "port": 9000} in jmespath.search(
            "spec.ports", docs[0]
        )
        assert "127.0.0.1" == jmespath.search("spec.loadBalancerIP", docs[0])
        assert ["10.123.0.0/16"] == jmespath.search(
            "spec.loadBalancerSourceRanges", docs[0]
        )

    @pytest.mark.parametrize(
        "ports, expected_ports",
        [
            ([{"port": 8888}], [{"port": 8888}]),  # name is optional with a single port
            (
                [
                    {
                        "name": "{{ .Release.Name }}",
                        "protocol": "UDP",
                        "port": "{{ .Values.ports.airflowUI }}",
                    }
                ],
                [{"name": "release-name", "protocol": "UDP", "port": 8080}],
            ),
            (
                [{"name": "only_sidecar", "port": "{{ int 9000 }}"}],
                [{"name": "only_sidecar", "port": 9000}],
            ),
            (
                [
                    {"name": "airflow-ui", "port": "{{ .Values.ports.airflowUI }}"},
                    {"name": "sidecar", "port": 80, "targetPort": "sidecar"},
                ],
                [
                    {"name": "airflow-ui", "port": 8080},
                    {"name": "sidecar", "port": 80, "targetPort": "sidecar"},
                ],
            ),
        ],
    )
    def test_ports_overrides(self, ports, expected_ports):
        docs = render_chart(
            values={
                "webserver": {"service": {"ports": ports}},
            },
            show_only=["templates/webserver/webserver-service.yaml"],
        )

        assert expected_ports == jmespath.search("spec.ports", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "webserver": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/webserver/webserver-service.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    @pytest.mark.parametrize(
        "ports, expected_ports",
        [
            (
                [{"nodePort": "31000", "port": "8080"}],
                [{"nodePort": 31000, "port": 8080}],
            ),
            (
                [{"port": "8080"}],
                [{"port": 8080}],
            ),
        ],
    )
    def test_nodeport_service(self, ports, expected_ports):
        docs = render_chart(
            values={
                "webserver": {
                    "service": {
                        "type": "NodePort",
                        "ports": ports,
                    }
                },
            },
            show_only=["templates/webserver/webserver-service.yaml"],
        )

        assert "NodePort" == jmespath.search("spec.type", docs[0])
        assert expected_ports == jmespath.search("spec.ports", docs[0])


class TestWebserverConfigmap:
    """Tests webserver configmap."""

    def test_no_webserver_config_configmap_by_default(self):
        docs = render_chart(show_only=["templates/configmaps/webserver-configmap.yaml"])
        assert 0 == len(docs)

    def test_no_webserver_config_configmap_with_configmap_name(self):
        docs = render_chart(
            values={
                "webserver": {
                    "webserverConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}",
                    "webserverConfigConfigMapName": "my-configmap",
                }
            },
            show_only=["templates/configmaps/webserver-configmap.yaml"],
        )
        assert 0 == len(docs)

    def test_webserver_config_configmap(self):
        docs = render_chart(
            values={
                "webserver": {
                    "webserverConfig": "CSRF_ENABLED = True  # {{ .Release.Name }}"
                }
            },
            show_only=["templates/configmaps/webserver-configmap.yaml"],
        )

        assert "ConfigMap" == docs[0]["kind"]
        assert "release-name-webserver-config" == jmespath.search(
            "metadata.name", docs[0]
        )
        assert (
            "CSRF_ENABLED = True  # release-name"
            == jmespath.search('data."webserver_config.py"', docs[0]).strip()
        )


class TestWebserverNetworkPolicy:
    """Tests webserver network policy."""

    def test_off_by_default(self):
        docs = render_chart(
            show_only=["templates/webserver/webserver-networkpolicy.yaml"],
        )
        assert 0 == len(docs)

    def test_defaults(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "webserver": {
                    "networkPolicy": {
                        "ingress": {
                            "from": [
                                {
                                    "namespaceSelector": {
                                        "matchLabels": {"release": "myrelease"}
                                    }
                                }
                            ]
                        }
                    }
                },
            },
            show_only=["templates/webserver/webserver-networkpolicy.yaml"],
        )

        assert 1 == len(docs)
        assert "NetworkPolicy" == docs[0]["kind"]
        assert [
            {"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}
        ] == jmespath.search("spec.ingress[0].from", docs[0])
        assert [{"port": 8080}] == jmespath.search("spec.ingress[0].ports", docs[0])

    @pytest.mark.parametrize(
        "ports, expected_ports",
        [
            ([{"port": "sidecar"}], [{"port": "sidecar"}]),
            (
                [
                    {"port": "{{ .Values.ports.airflowUI }}"},
                    {"port": 80},
                ],
                [
                    {"port": 8080},
                    {"port": 80},
                ],
            ),
        ],
    )
    def test_ports_overrides(self, ports, expected_ports):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "webserver": {
                    "networkPolicy": {
                        "ingress": {
                            "from": [
                                {
                                    "namespaceSelector": {
                                        "matchLabels": {"release": "myrelease"}
                                    }
                                }
                            ],
                            "ports": ports,
                        }
                    }
                },
            },
            show_only=["templates/webserver/webserver-networkpolicy.yaml"],
        )

        assert expected_ports == jmespath.search("spec.ingress[0].ports", docs[0])

    def test_deprecated_from_param(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "webserver": {
                    "extraNetworkPolicies": [
                        {"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}
                    ]
                },
            },
            show_only=["templates/webserver/webserver-networkpolicy.yaml"],
        )

        assert [
            {"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}
        ] == jmespath.search("spec.ingress[0].from", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "webserver": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/webserver/webserver-networkpolicy.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )


class TestWebserverServiceAccount:
    """Tests webserver service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "webserver": {
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/webserver/webserver-serviceaccount.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "webserver": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/webserver/webserver-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "webserver": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                    },
                },
            },
            show_only=["templates/webserver/webserver-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
