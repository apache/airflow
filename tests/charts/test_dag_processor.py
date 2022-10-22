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


class TestDagProcessor:
    @pytest.mark.parametrize(
        "airflow_version, num_docs",
        [
            ("2.2.0", 0),
            ("2.3.0", 1),
        ],
    )
    def test_only_exists_on_new_airflow_versions(self, airflow_version, num_docs):
        """Standalone Dag Processor was only added from Airflow 2.3 onwards"""
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert num_docs == len(docs)

    def test_can_be_disabled(self):
        """
        Standalone Dag Processor is disabled by default.
        """
        docs = render_chart(
            values={"dagProcessor": {"enabled": False}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert 0 == len(docs)

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        actual = jmespath.search("spec.template.spec.initContainers", docs[0])
        assert actual is None

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraContainers": [
                        {"name": "test-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert {
            "name": "test-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraInitContainers": [
                        {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraVolumes": [{"name": "test-volume", "emptyDir": {}}],
                    "extraVolumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert "test-volume" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])
        assert "test-volume" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[0].name", docs[0]
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert {'name': 'TEST_ENV_1', 'value': 'test_env_1'} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )

    def test_should_add_extraEnvs_to_wait_for_migration_container(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                    },
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert {'name': 'TEST_ENV_1', 'value': 'test_env_1'} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
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
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
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

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(self):
        """When given both global and triggerer affinity etc, triggerer affinity etc is used"""
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
                "dagProcessor": {
                    "enabled": True,
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
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert expected_affinity == jmespath.search("spec.template.spec.affinity", docs[0])
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

    def test_should_create_default_affinity(self):
        docs = render_chart(show_only=["templates/scheduler/scheduler-deployment.yaml"])

        assert {"component": "scheduler"} == jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity."
            "preferredDuringSchedulingIgnoredDuringExecution[0]."
            "podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        )

    def test_livenessprobe_values_are_configurable(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "livenessProbe": {
                        "initialDelaySeconds": 111,
                        "timeoutSeconds": 222,
                        "failureThreshold": 333,
                        "periodSeconds": 444,
                        "command": ["sh", "-c", "echo", "wow such test"],
                    },
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert 111 == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.initialDelaySeconds", docs[0]
        )
        assert 222 == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.timeoutSeconds", docs[0]
        )
        assert 333 == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.failureThreshold", docs[0]
        )
        assert 444 == jmespath.search("spec.template.spec.containers[0].livenessProbe.periodSeconds", docs[0])

        assert ["sh", "-c", "echo", "wow such test"] == jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]
        )

    @pytest.mark.parametrize(
        "log_persistence_values, expected_volume",
        [
            ({"enabled": False}, {"emptyDir": {}}),
            ({"enabled": True}, {"persistentVolumeClaim": {"claimName": "release-name-logs"}}),
            (
                {"enabled": True, "existingClaim": "test-claim"},
                {"persistentVolumeClaim": {"claimName": "test-claim"}},
            ),
        ],
    )
    def test_logs_persistence_changes_volume(self, log_persistence_values, expected_volume):
        docs = render_chart(
            values={
                "logs": {"persistence": log_persistence_values},
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert {"name": "logs", **expected_volume} == jmespath.search(
            "spec.template.spec.volumes[1]", docs[0]
        )

    def test_resources_are_configurable(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "resources": {
                        "limits": {"cpu": "200m", 'memory': "128Mi"},
                        "requests": {"cpu": "300m", 'memory': "169Mi"},
                    },
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert "128Mi" == jmespath.search("spec.template.spec.containers[0].resources.limits.memory", docs[0])
        assert "200m" == jmespath.search("spec.template.spec.containers[0].resources.limits.cpu", docs[0])
        assert "169Mi" == jmespath.search(
            "spec.template.spec.containers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", docs[0])

        assert "128Mi" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.limits.memory", docs[0]
        )
        assert "200m" == jmespath.search("spec.template.spec.initContainers[0].resources.limits.cpu", docs[0])
        assert "169Mi" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search(
            "spec.template.spec.initContainers[0].resources.requests.cpu", docs[0]
        )

    def test_resources_are_not_added_by_default(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    @pytest.mark.parametrize(
        "strategy, expected_strategy",
        [
            (None, None),
            (
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
            ),
        ],
    )
    def test_strategy(self, strategy, expected_strategy):
        """strategy should be used when we aren't using both LocalExecutor and workers.persistence"""
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True, "strategy": strategy},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert expected_strategy == jmespath.search("spec.strategy", docs[0])

    def test_default_command_and_args(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert ["bash", "-c", "exec airflow dag-processor"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

    @pytest.mark.parametrize(
        "revision_history_limit, global_revision_history_limit",
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {
            "dagProcessor": {
                "enabled": True,
            }
        }
        if revision_history_limit:
            values['dagProcessor']['revisionHistoryLimit'] = revision_history_limit
        if global_revision_history_limit:
            values['revisionHistoryLimit'] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        expected_result = revision_history_limit if revision_history_limit else global_revision_history_limit
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "command": command,
                    "args": args,
                }
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert ["release-name"] == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert ["Helm"] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_dags_volume_mount_with_persistence_true(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}, "dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert "dags" in [
            vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        ]
        assert "dags" in [vm["name"] for vm in jmespath.search("spec.template.spec.volumes", docs[0])]

    def test_dags_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}, "dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])]
        assert "git-sync-init" in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    def test_dags_gitsync_with_persistence_no_sidecar_or_init_container(self):
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True},
                "dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        # No gitsync sidecar or init container
        assert "git-sync" not in [
            c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])
        ]
        assert "git-sync-init" not in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]
