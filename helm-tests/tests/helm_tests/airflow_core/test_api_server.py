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

from subprocess import CalledProcessError

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart as _render_chart


# Everything in here needcs to set airflowVersion to get the API server to render
def render_chart(values=None, **kwargs):
    values = values or {}
    values.setdefault("airflowVersion", "3.0.0")
    return _render_chart(values=values, **kwargs)


class TestAPIServerDeployment:
    """Tests api-server deployment."""

    @pytest.mark.parametrize(
        ("revision_history_limit", "global_revision_history_limit"),
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {"apiServer": {}}
        if revision_history_limit:
            values["apiServer"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        expected_result = revision_history_limit if revision_history_limit else global_revision_history_limit
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    def test_should_add_scheme_to_liveness_and_readiness_and_startup_probes(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "livenessProbe": {"scheme": "HTTPS"},
                    "readinessProbe": {"scheme": "HTTPS"},
                    "startupProbe": {"scheme": "HTTPS"},
                }
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
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

    def test_should_use_monitor_health_for_http_probes(self):
        docs = render_chart(show_only=["templates/api-server/api-server-deployment.yaml"])

        for probe in ("livenessProbe", "readinessProbe", "startupProbe"):
            assert (
                jmespath.search(f"spec.template.spec.containers[0].{probe}.httpGet.path", docs[0])
                == "/api/v2/monitor/health"
            )

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "apiServer": {
                    "extraContainers": [
                        {"name": "{{.Chart.Name}}", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "env": [
                        {"name": "TEST_ENV_1", "value": "test_env_1"},
                        {"name": "TEST_ENV_2", "value": "test_env_2"},
                        {"name": "TEST_ENV_3", "value": "test_env_3"},
                    ],
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        env_result = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in env_result
        assert {"name": "TEST_ENV_2", "value": "test_env_2"} in env_result
        assert {"name": "TEST_ENV_3", "value": "test_env_3"} in env_result

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                    "extraVolumeMounts": [
                        {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                    ],
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[-1].name", docs[0]) == "test-volume-airflow"
        assert (
            jmespath.search("spec.template.spec.containers[0].volumeMounts[-1].name", docs[0])
            == "test-volume-airflow"
        )
        assert (
            jmespath.search("spec.template.spec.initContainers[0].volumeMounts[-1].name", docs[0])
            == "test-volume-airflow"
        )

    def test_should_add_global_volume_and_global_volume_mount(self):
        docs = render_chart(
            values={
                "volumes": [{"name": "test-volume", "emptyDir": {}}],
                "volumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[-1].name", docs[0]) == "test-volume"
        assert (
            jmespath.search("spec.template.spec.containers[0].volumeMounts[-1].name", docs[0])
            == "test-volume"
        )

    def test_should_add_extraEnvs_to_wait_for_migration_container(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                    },
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    def test_wait_for_migration_airflow_version(self):
        expected_arg = ["airflow", "db", "check-migrations", "--migration-wait-timeout=60"]
        docs = render_chart(
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        # Don't test the full string, just the length of the expect matches
        actual = jmespath.search("spec.template.spec.initContainers[0].args", docs[0])
        assert expected_arg == actual[: len(expected_arg)]

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']", docs[0]
        )
        assert actual is None

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "extraInitContainers": [
                        {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert jmespath.search("spec.template.metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "apiServer": {
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
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "Deployment"
        assert (
            jmespath.search(
                "spec.template.spec.affinity.nodeAffinity."
                "requiredDuringSchedulingIgnoredDuringExecution."
                "nodeSelectorTerms[0]."
                "matchExpressions[0]."
                "key",
                docs[0],
            )
            == "foo"
        )
        assert (
            jmespath.search(
                "spec.template.spec.nodeSelector.diskType",
                docs[0],
            )
            == "ssd"
        )
        assert (
            jmespath.search(
                "spec.template.spec.tolerations[0].key",
                docs[0],
            )
            == "dynamic-pods"
        )

    def test_should_create_default_affinity(self):
        docs = render_chart(
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity."
            "preferredDuringSchedulingIgnoredDuringExecution[0]."
            "podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        ) == {"component": "api-server"}

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(self):
        """When given both global and api-server affinity etc, api-server affinity etc is used."""
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
                "apiServer": {
                    "affinity": expected_affinity,
                    "tolerations": [
                        {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
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
                                        {"key": "not-me", "operator": "In", "values": ["true"]},
                                    ]
                                },
                            }
                        ]
                    }
                },
                "tolerations": [
                    {"key": "not-me", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
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
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert expected_affinity == jmespath.search("spec.template.spec.affinity", docs[0])
        assert (
            jmespath.search(
                "spec.template.spec.nodeSelector.type",
                docs[0],
            )
            == "ssd"
        )
        tolerations = jmespath.search("spec.template.spec.tolerations", docs[0])
        assert len(tolerations) == 1
        assert tolerations[0]["key"] == "dynamic-pods"
        assert expected_topology_spread_constraints == jmespath.search(
            "spec.template.spec.topologySpreadConstraints[0]", docs[0]
        )

    def test_scheduler_name(self):
        docs = render_chart(
            values={"schedulerName": "airflow-scheduler"},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.schedulerName",
                docs[0],
            )
            == "airflow-scheduler"
        )

    @pytest.mark.parametrize(
        ("log_persistence_values", "expected_claim_name"),
        [
            ({"enabled": False}, None),
            ({"enabled": True}, "release-name-logs"),
            ({"enabled": True, "existingClaim": "test-claim"}, "test-claim"),
        ],
    )
    def test_logs_persistence_adds_volume_and_mount(self, log_persistence_values, expected_claim_name):
        docs = render_chart(
            values={"logs": {"persistence": log_persistence_values}},
            show_only=["templates/api-server/api-server-deployment.yaml"],
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
            assert "logs" not in [v["name"] for v in jmespath.search("spec.template.spec.volumes", docs[0])]
            assert "logs" not in [
                v["name"] for v in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
            ]

    def test_config_volumes(self):
        docs = render_chart(
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        # default config
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "readOnly": True,
            "subPath": "airflow.cfg",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    def testapi_server_resources_are_configurable(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    },
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources.limits.memory", docs[0]) == "128Mi"
        assert jmespath.search("spec.template.spec.containers[0].resources.limits.cpu", docs[0]) == "200m"

        assert (
            jmespath.search("spec.template.spec.containers[0].resources.requests.memory", docs[0]) == "169Mi"
        )
        assert jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", docs[0]) == "300m"

        # initContainer wait-for-airflow-migrations
        assert (
            jmespath.search("spec.template.spec.initContainers[0].resources.limits.memory", docs[0])
            == "128Mi"
        )
        assert jmespath.search("spec.template.spec.initContainers[0].resources.limits.cpu", docs[0]) == "200m"

        assert (
            jmespath.search("spec.template.spec.initContainers[0].resources.requests.memory", docs[0])
            == "169Mi"
        )
        assert (
            jmespath.search("spec.template.spec.initContainers[0].resources.requests.cpu", docs[0]) == "300m"
        )

    def test_api_server_security_contexts_are_configurable(self):
        docs = render_chart(
            values={
                "apiServer": {
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
                    },
                },
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
        }

        assert jmespath.search("spec.template.spec.securityContext", docs[0]) == {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        }

    def test_api_server_security_context_legacy(self):
        with pytest.raises(CalledProcessError, match="Additional property securityContext is not allowed"):
            render_chart(
                values={
                    "apiServer": {
                        "securityContext": {
                            "fsGroup": 1000,
                            "runAsGroup": 1001,
                            "runAsNonRoot": True,
                            "runAsUser": 2000,
                        },
                    },
                },
                show_only=["templates/api-server/api-server-deployment.yaml"],
            )

    def test_api_server_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}
        assert jmespath.search("spec.template.spec.initContainers[0].resources", docs[0]) == {}

    @pytest.mark.parametrize(
        ("airflow_version", "strategy", "expected_strategy"),
        [
            pytest.param(
                "3.0.0",
                None,
                {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}},
                id="default",
            ),
            pytest.param(
                "3.0.0",
                {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 1}},
                {"type": "RollingUpdate", "rollingUpdate": {"maxSurge": 1, "maxUnavailable": 0}},
                id="custom-strategy",
            ),
        ],
    )
    def test_update_strategy(self, airflow_version, strategy, expected_strategy):
        docs = render_chart(
            values={"airflowVersion": airflow_version, "apiServer": {"strategy": expected_strategy}},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_default_command_and_args(self):
        docs = render_chart(
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            "exec airflow api-server",
        ]

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"apiServer": {"command": command, "args": args}},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                }
            },
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={"apiServer": {"annotations": {"test_annotation": "test_annotation_value"}}},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"

    def test_api_server_pod_hostaliases(self):
        docs = render_chart(
            values={"apiServer": {"hostAliases": [{"ip": "127.0.0.1", "hostnames": ["foo.local"]}]}},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.hostAliases[0].ip", docs[0]) == "127.0.0.1"
        assert jmespath.search("spec.template.spec.hostAliases[0].hostnames[0]", docs[0]) == "foo.local"

    def test_can_be_disabled(self):
        """
        API server can be disabled by configuration.
        """
        docs = render_chart(
            values={"apiServer": {"enabled": False}},
            show_only=["templates/api-server/api-server-deployment.yaml"],
        )

        assert len(docs) == 0


class TestAPIServerService:
    """Tests api-server service."""

    def test_default_service(self):
        docs = render_chart(
            show_only=["templates/api-server/api-server-service.yaml"],
        )

        assert jmespath.search("metadata.name", docs[0]) == "release-name-api-server"
        assert jmespath.search("metadata.annotations", docs[0]) is None
        assert jmespath.search("spec.selector", docs[0]) == {
            "tier": "airflow",
            "component": "api-server",
            "release": "release-name",
        }
        assert jmespath.search("spec.type", docs[0]) == "ClusterIP"
        assert {"name": "api-server", "port": 8080} in jmespath.search("spec.ports", docs[0])

    def test_overrides(self):
        docs = render_chart(
            values={
                "ports": {"apiServer": 9000},
                "apiServer": {
                    "service": {
                        "type": "LoadBalancer",
                        "loadBalancerIP": "127.0.0.1",
                        "annotations": {"foo": "bar"},
                        "loadBalancerSourceRanges": ["10.123.0.0/16"],
                    },
                },
            },
            show_only=["templates/api-server/api-server-service.yaml"],
        )

        assert jmespath.search("metadata.annotations", docs[0]) == {"foo": "bar"}
        assert jmespath.search("spec.type", docs[0]) == "LoadBalancer"
        assert {"name": "api-server", "port": 9000} in jmespath.search("spec.ports", docs[0])
        assert jmespath.search("spec.loadBalancerIP", docs[0]) == "127.0.0.1"
        assert jmespath.search("spec.loadBalancerSourceRanges", docs[0]) == ["10.123.0.0/16"]

    @pytest.mark.parametrize(
        ("ports", "expected_ports"),
        [
            ([{"port": 8888}], [{"port": 8888}]),  # name is optional with a single port
            (
                [
                    {
                        "name": "{{ .Release.Name }}",
                        "protocol": "UDP",
                        "port": "{{ .Values.ports.apiServer }}",
                    }
                ],
                [{"name": "release-name", "protocol": "UDP", "port": 8080}],
            ),
            ([{"name": "only_sidecar", "port": "{{ int 9000 }}"}], [{"name": "only_sidecar", "port": 9000}]),
            (
                [
                    {"name": "api-server", "port": "{{ .Values.ports.apiServer }}"},
                    {"name": "sidecar", "port": 80, "targetPort": "sidecar"},
                ],
                [
                    {"name": "api-server", "port": 8080},
                    {"name": "sidecar", "port": 80, "targetPort": "sidecar"},
                ],
            ),
        ],
    )
    def test_ports_overrides(self, ports, expected_ports):
        docs = render_chart(
            values={"apiServer": {"service": {"ports": ports}}},
            show_only=["templates/api-server/api-server-service.yaml"],
        )

        assert jmespath.search("spec.ports", docs[0]) == expected_ports

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={"apiServer": {"labels": {"test_label": "test_label_value"}}},
            show_only=["templates/api-server/api-server-service.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        ("ports", "expected_ports"),
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
            values={"apiServer": {"service": {"type": "NodePort", "ports": ports}}},
            show_only=["templates/api-server/api-server-service.yaml"],
        )

        assert jmespath.search("spec.type", docs[0]) == "NodePort"
        assert expected_ports == jmespath.search("spec.ports", docs[0])

    def test_can_be_disabled(self):
        """
        API server service can be disabled by configuration.
        """
        docs = render_chart(
            values={"apiServer": {"enabled": False}},
            show_only=["templates/api-server/api-server-service.yaml"],
        )

        assert len(docs) == 0


class TestAPIServerNetworkPolicy:
    """Tests api-server network policy."""

    def test_off_by_default(self):
        docs = render_chart(
            show_only=["templates/api-server/api-server-networkpolicy.yaml"],
        )
        assert len(docs) == 0

    def test_defaults(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "apiServer": {
                    "networkPolicy": {
                        "ingress": {
                            "from": [{"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}]
                        }
                    },
                },
            },
            show_only=["templates/api-server/api-server-networkpolicy.yaml"],
        )

        assert len(docs) == 1
        assert docs[0]["kind"] == "NetworkPolicy"
        assert jmespath.search("spec.ingress[0].from", docs[0]) == [
            {"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}
        ]
        assert jmespath.search("spec.ingress[0].ports", docs[0]) == [{"port": 8080}]

    @pytest.mark.parametrize(
        ("ports", "expected_ports"),
        [
            ([{"port": "sidecar"}], [{"port": "sidecar"}]),
            (
                [
                    {"port": "{{ .Values.ports.apiServer }}"},
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
                "apiServer": {
                    "networkPolicy": {
                        "ingress": {
                            "from": [{"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}],
                            "ports": ports,
                        }
                    },
                },
            },
            show_only=["templates/api-server/api-server-networkpolicy.yaml"],
        )

        assert expected_ports == jmespath.search("spec.ingress[0].ports", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "apiServer": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/api-server/api-server-networkpolicy.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_can_be_disabled(self):
        """
        API server networkpolicy can be disabled by configuration.
        """
        docs = render_chart(
            values={"apiServer": {"enabled": False}},
            show_only=["templates/api-server/api-server-networkpolicy.yaml"],
        )

        assert len(docs) == 0


class TestAPIServerServiceAccount:
    """Tests api-server service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/api-server/api-server-serviceaccount.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/api-server/api-server-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "serviceAccount": {"create": True, "automountServiceAccountToken": False},
                },
            },
            show_only=["templates/api-server/api-server-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False

    def test_can_be_disabled(self):
        """
        API Server should be able to be disabled if the users desires.
        """
        docs = render_chart(
            values={"apiServer": {"enabled": False}},
            show_only=["templates/api-server/api-server-serviceaccount.yaml"],
        )

        assert len(docs) == 0
