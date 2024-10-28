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

from tests.charts.helm_template_generator import render_chart


class TestRPCServerDeployment:
    """Tests rpc-server deployment."""

    def test_is_disabled_by_default(self):
        """
        RPC server should be disabled by default.
        """
        docs = render_chart(
            values={"_rpcServer": {}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert len(docs) == 0

    def test_should_add_host_header_to_liveness_and_readiness_and_startup_probes(self):
        docs = render_chart(
            values={
                "_rpcServer": {"enabled": True},
                "config": {
                    "core": {"internal_api_url": "https://example.com:21222/mypath/path"}
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
                "_rpcServer": {
                    "enabled": True,
                },
                "config": {
                    "core": {"internal_api_url": "https://example.com:21222/mypath/path"}
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.containers[0].livenessProbe.httpGet.path", docs[0]
            )
            == "/mypath/path/internal_api/v1/health"
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].readinessProbe.httpGet.path", docs[0]
            )
            == "/mypath/path/internal_api/v1/health"
        )
        assert (
            jmespath.search(
                "spec.template.spec.containers[0].startupProbe.httpGet.path", docs[0]
            )
            == "/mypath/path/internal_api/v1/health"
        )

    @pytest.mark.parametrize(
        "revision_history_limit, global_revision_history_limit",
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(
        self, revision_history_limit, global_revision_history_limit
    ):
        values = {
            "_rpcServer": {
                "enabled": True,
            }
        }
        if revision_history_limit:
            values["_rpcServer"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )
        expected_result = (
            revision_history_limit
            if revision_history_limit
            else global_revision_history_limit
        )
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    @pytest.mark.parametrize(
        "values",
        [
            {"_rpcServer": {"enabled": True}},
            {
                "_rpcServer": {"enabled": True},
                "config": {"core": {"internal_api_url": ""}},
            },
        ],
    )
    def test_should_not_contain_host_header(self, values):
        docs = render_chart(
            values=values, show_only=["templates/rpc-server/rpc-server-deployment.yaml"]
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
                "_rpcServer": {
                    "enabled": True,
                },
                "config": {
                    "core": {
                        "internal_api_url": "https://{{ .Release.Name }}.com:21222/mypath/{{ .Release.Name }}/path"
                    }
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
        assert "/mypath/release-name/path/internal_api/v1/health" == jmespath.search(
            "livenessProbe.httpGet.path", container
        )
        assert "/mypath/release-name/path/internal_api/v1/health" == jmespath.search(
            "readinessProbe.httpGet.path", container
        )
        assert "/mypath/release-name/path/internal_api/v1/health" == jmespath.search(
            "startupProbe.httpGet.path", container
        )

    def test_should_add_scheme_to_liveness_and_readiness_and_startup_probes(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "livenessProbe": {"scheme": "HTTPS"},
                    "readinessProbe": {"scheme": "HTTPS"},
                    "startupProbe": {"scheme": "HTTPS"},
                }
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "_rpcServer": {
                    "enabled": True,
                    "extraContainers": [
                        {
                            "name": "{{.Chart.Name}}",
                            "image": "test-registry/test-repo:test-tag",
                        }
                    ],
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
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
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
                "_rpcServer": {"enabled": True},
                "volumes": [{"name": "test-volume", "emptyDir": {}}],
                "volumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
                "_rpcServer": {
                    "enabled": True,
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
                "_rpcServer": {"enabled": True},
                "airflowVersion": airflow_version,
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )
        # Don't test the full string, just the length of the expect matches
        actual = jmespath.search("spec.template.spec.initContainers[0].args", docs[0])
        assert expected_arg == actual[: len(expected_arg)]

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']",
            docs[0],
        )
        assert actual is None

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "extraInitContainers": [
                        {
                            "name": "test-init-container",
                            "image": "test-registry/test-repo:test-tag",
                        }
                    ],
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
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
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
        docs = render_chart(
            values={"_rpcServer": {"enabled": True}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert {"component": "rpc-server"} == jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity."
            "preferredDuringSchedulingIgnoredDuringExecution[0]."
            "podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        )

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(
        self,
    ):
        """When given both global and rpc-server affinity etc, rpc-server affinity etc is used."""
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
                "_rpcServer": {
                    "enabled": True,
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
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
            values={
                "_rpcServer": {"enabled": True},
                "schedulerName": "airflow-scheduler",
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
            values={
                "_rpcServer": {"enabled": True},
                "logs": {"persistence": log_persistence_values},
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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

    def test_config_volumes(self):
        docs = render_chart(
            values={"_rpcServer": {"enabled": True}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        # default config
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "readOnly": True,
            "subPath": "airflow.cfg",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_rpc_server_resources_are_configurable(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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

    def test_rpc_server_security_contexts_are_configurable(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
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
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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

    def test_rpc_server_security_context_legacy(self):
        with pytest.raises(
            CalledProcessError, match="Additional property securityContext is not allowed"
        ):
            render_chart(
                values={
                    "_rpcServer": {
                        "enabled": True,
                        "securityContext": {
                            "fsGroup": 1000,
                            "runAsGroup": 1001,
                            "runAsNonRoot": True,
                            "runAsUser": 2000,
                        },
                    },
                },
                show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
            )

    def test_rpc_server_resources_are_not_added_by_default(self):
        docs = render_chart(
            values={"_rpcServer": {"enabled": True}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
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
            values={"_rpcServer": {"enabled": True}, "airflowVersion": airflow_version},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_update_strategy(self):
        expected_strategy = {
            "type": "RollingUpdate",
            "rollingUpdate": {"maxUnavailable": 1},
        }
        docs = render_chart(
            values={"_rpcServer": {"enabled": True, "strategy": expected_strategy}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == expected_strategy

    def test_default_command_and_args(self):
        docs = render_chart(
            values={"_rpcServer": {"enabled": True}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == [
            "bash"
        ]
        assert ["-c", "exec airflow internal-api"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"_rpcServer": {"enabled": True, "command": command, "args": args}},
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert command == jmespath.search(
            "spec.template.spec.containers[0].command", docs[0]
        )
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                }
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert ["release-name"] == jmespath.search(
            "spec.template.spec.containers[0].command", docs[0]
        )
        assert ["Helm"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "annotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert (
            jmespath.search("metadata.annotations", docs[0])["test_annotation"]
            == "test_annotation_value"
        )

    def test_rpc_server_pod_hostaliases(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "hostAliases": [{"ip": "127.0.0.1", "hostnames": ["foo.local"]}],
                },
            },
            show_only=["templates/rpc-server/rpc-server-deployment.yaml"],
        )

        assert "127.0.0.1" == jmespath.search(
            "spec.template.spec.hostAliases[0].ip", docs[0]
        )
        assert "foo.local" == jmespath.search(
            "spec.template.spec.hostAliases[0].hostnames[0]", docs[0]
        )


class TestRPCServerService:
    """Tests rpc-server service."""

    def test_default_service(self):
        docs = render_chart(
            values={"_rpcServer": {"enabled": True}},
            show_only=["templates/rpc-server/rpc-server-service.yaml"],
        )

        assert "release-name-rpc-server" == jmespath.search("metadata.name", docs[0])
        assert jmespath.search("metadata.annotations", docs[0]) is None
        assert {
            "tier": "airflow",
            "component": "rpc-server",
            "release": "release-name",
        } == jmespath.search("spec.selector", docs[0])
        assert "ClusterIP" == jmespath.search("spec.type", docs[0])
        assert {"name": "rpc-server", "port": 9080} in jmespath.search(
            "spec.ports", docs[0]
        )

    def test_overrides(self):
        docs = render_chart(
            values={
                "ports": {"_rpcServer": 9000},
                "_rpcServer": {
                    "enabled": True,
                    "service": {
                        "type": "LoadBalancer",
                        "loadBalancerIP": "127.0.0.1",
                        "annotations": {"foo": "bar"},
                        "loadBalancerSourceRanges": ["10.123.0.0/16"],
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-service.yaml"],
        )

        assert {"foo": "bar"} == jmespath.search("metadata.annotations", docs[0])
        assert "LoadBalancer" == jmespath.search("spec.type", docs[0])
        assert {"name": "rpc-server", "port": 9000} in jmespath.search(
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
                        "port": "{{ .Values.ports._rpcServer }}",
                    }
                ],
                [{"name": "release-name", "protocol": "UDP", "port": 9080}],
            ),
            (
                [{"name": "only_sidecar", "port": "{{ int 9000 }}"}],
                [{"name": "only_sidecar", "port": 9000}],
            ),
            (
                [
                    {"name": "rpc-server", "port": "{{ .Values.ports._rpcServer }}"},
                    {"name": "sidecar", "port": 80, "targetPort": "sidecar"},
                ],
                [
                    {"name": "rpc-server", "port": 9080},
                    {"name": "sidecar", "port": 80, "targetPort": "sidecar"},
                ],
            ),
        ],
    )
    def test_ports_overrides(self, ports, expected_ports):
        docs = render_chart(
            values={
                "_rpcServer": {"enabled": True, "service": {"ports": ports}},
            },
            show_only=["templates/rpc-server/rpc-server-service.yaml"],
        )

        assert jmespath.search("spec.ports", docs[0]) == expected_ports

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/rpc-server/rpc-server-service.yaml"],
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
                "_rpcServer": {
                    "enabled": True,
                    "service": {
                        "type": "NodePort",
                        "ports": ports,
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-service.yaml"],
        )

        assert "NodePort" == jmespath.search("spec.type", docs[0])
        assert expected_ports == jmespath.search("spec.ports", docs[0])


class TestRPCServerNetworkPolicy:
    """Tests rpc-server network policy."""

    def test_off_by_default(self):
        docs = render_chart(
            show_only=["templates/rpc-server/rpc-server-networkpolicy.yaml"],
        )
        assert 0 == len(docs)

    def test_defaults(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "_rpcServer": {
                    "enabled": True,
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
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-networkpolicy.yaml"],
        )

        assert 1 == len(docs)
        assert "NetworkPolicy" == docs[0]["kind"]
        assert [
            {"namespaceSelector": {"matchLabels": {"release": "myrelease"}}}
        ] == jmespath.search("spec.ingress[0].from", docs[0])
        assert jmespath.search("spec.ingress[0].ports", docs[0]) == [{"port": 9080}]

    @pytest.mark.parametrize(
        "ports, expected_ports",
        [
            ([{"port": "sidecar"}], [{"port": "sidecar"}]),
            (
                [
                    {"port": "{{ .Values.ports._rpcServer }}"},
                    {"port": 80},
                ],
                [
                    {"port": 9080},
                    {"port": 80},
                ],
            ),
        ],
    )
    def test_ports_overrides(self, ports, expected_ports):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "_rpcServer": {
                    "enabled": True,
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
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-networkpolicy.yaml"],
        )

        assert expected_ports == jmespath.search("spec.ingress[0].ports", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "_rpcServer": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/rpc-server/rpc-server-networkpolicy.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )


class TestRPCServerServiceAccount:
    """Tests rpc-server service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/rpc-server/rpc-server-serviceaccount.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/rpc-server/rpc-server-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "_rpcServer": {
                    "enabled": True,
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                    },
                },
            },
            show_only=["templates/rpc-server/rpc-server-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
