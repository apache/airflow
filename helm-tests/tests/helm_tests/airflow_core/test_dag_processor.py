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
from chart_utils.log_groomer import LogGroomerTestBase
from helm_tests.utils import (
    _get_enabled_git_sync_test_params,
    _get_git_sync_test_params_for_no_containers,
    _test_git_sync_presence,
)


class TestDagProcessor:
    """Tests DAG processor."""

    @pytest.mark.parametrize(
        ("airflow_version", "num_docs"),
        [
            ("2.2.0", 0),
            ("2.3.0", 1),
        ],
    )
    def test_only_exists_on_new_airflow_versions(self, airflow_version, num_docs):
        """Standalone Dag Processor was only added from Airflow 2.3 onwards."""
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == num_docs

    @pytest.mark.parametrize(
        ("airflow_version", "num_docs"),
        [
            ("2.10.4", 0),
            ("3.0.0", 1),
        ],
    )
    def test_enabled_by_airflow_version(self, airflow_version, num_docs):
        """Tests that Dag Processor is enabled by default with Airflow 3"""
        docs = render_chart(
            values={"airflowVersion": airflow_version},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == num_docs

    @pytest.mark.parametrize(
        ("airflow_version", "enabled"),
        [
            ("2.10.4", False),
            ("2.10.4", True),
            ("3.0.0", False),
            ("3.0.0", True),
        ],
    )
    def test_enabled_explicit(self, airflow_version, enabled):
        """Tests that Dag Processor can be enabled/disabled regardless of version"""
        docs = render_chart(
            values={"airflowVersion": airflow_version, "dagProcessor": {"enabled": enabled}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        if enabled:
            assert len(docs) == 1
        else:
            assert len(docs) == 0

    def test_can_be_disabled(self):
        """Standalone Dag Processor is disabled by default."""
        docs = render_chart(
            values={"dagProcessor": {"enabled": False}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 0

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
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']", docs[0]
        )
        assert actual is None

    def test_wait_for_migration_security_contexts_are_configurable(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "waitForMigrations": {
                        "enabled": True,
                        "securityContexts": {
                            "container": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                            },
                        },
                    },
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
        }

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraContainers": [
                        {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "release-name-test-container"
        }

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

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraInitContainers": [{"name": "{{ .Release.Name }}-test-init-container"}],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "release-name-test-init-container"
        }

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                    "extraVolumeMounts": [
                        {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                    ],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1].name", docs[0]) == "test-volume-airflow"
        assert (
            jmespath.search("spec.template.spec.containers[0].volumeMounts[0].name", docs[0])
            == "test-volume-airflow"
        )
        assert (
            jmespath.search("spec.template.spec.initContainers[0].volumeMounts[0].name", docs[0])
            == "test-volume-airflow"
        )

    def test_should_add_global_volume_and_global_volume_mount(self):
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True},
                "volumes": [{"name": "test-volume", "emptyDir": {}}],
                "volumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1].name", docs[0]) == "test-volume"
        assert (
            jmespath.search("spec.template.spec.containers[0].volumeMounts[0].name", docs[0]) == "test-volume"
        )
        assert (
            jmespath.search("spec.template.spec.initContainers[0].volumeMounts[0].name", docs[0])
            == "test-volume"
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "env": [
                        {"name": "TEST_ENV_1", "value": "test_env_1"},
                        {
                            "name": "TEST_ENV_2",
                            "valueFrom": {"secretKeyRef": {"name": "my-secret", "key": "my-key"}},
                        },
                        {
                            "name": "TEST_ENV_3",
                            "valueFrom": {"configMapKeyRef": {"name": "my-config-map", "key": "my-key"}},
                        },
                    ],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
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

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    def test_scheduler_name(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}, "schedulerName": "airflow-scheduler"},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.schedulerName",
                docs[0],
            )
            == "airflow-scheduler"
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

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(self):
        """When given both global and triggerer affinity etc, triggerer affinity etc is used."""
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

    def test_should_create_default_affinity(self):
        docs = render_chart(show_only=["templates/scheduler/scheduler-deployment.yaml"])

        assert jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity."
            "preferredDuringSchedulingIgnoredDuringExecution[0]."
            "podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        ) == {"component": "scheduler"}

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

        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.initialDelaySeconds", docs[0])
            == 111
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.timeoutSeconds", docs[0]) == 222
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].livenessProbe.failureThreshold", docs[0]) == 333
        )
        assert jmespath.search("spec.template.spec.containers[0].livenessProbe.periodSeconds", docs[0]) == 444

        assert jmespath.search("spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]) == [
            "sh",
            "-c",
            "echo",
            "wow such test",
        ]

    @pytest.mark.parametrize(
        ("airflow_version", "probe_command"),
        [
            ("2.4.9", "airflow jobs check --hostname $(hostname)"),
            ("2.5.0", "airflow jobs check --local"),
            ("2.5.2", "airflow jobs check --local --job-type DagProcessorJob"),
        ],
    )
    def test_livenessprobe_command_depends_on_airflow_version(self, airflow_version, probe_command):
        docs = render_chart(
            values={"airflowVersion": f"{airflow_version}", "dagProcessor": {"enabled": True}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert (
            probe_command
            in jmespath.search("spec.template.spec.containers[0].livenessProbe.exec.command", docs[0])[-1]
        )

    @pytest.mark.parametrize(
        ("log_values", "expected_volume"),
        [
            ({"persistence": {"enabled": False}}, {"emptyDir": {}}),
            (
                {"persistence": {"enabled": False}, "emptyDirConfig": {"sizeLimit": "10Gi"}},
                {"emptyDir": {"sizeLimit": "10Gi"}},
            ),
            (
                {"persistence": {"enabled": True}},
                {"persistentVolumeClaim": {"claimName": "release-name-logs"}},
            ),
            (
                {"persistence": {"enabled": True, "existingClaim": "test-claim"}},
                {"persistentVolumeClaim": {"claimName": "test-claim"}},
            ),
        ],
    )
    def test_logs_persistence_changes_volume(self, log_values, expected_volume):
        docs = render_chart(
            values={
                "logs": log_values,
                "dagProcessor": {"enabled": True},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1]", docs[0]) == {
            "name": "logs",
            **expected_volume,
        }

    def test_resources_are_configurable(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    },
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources.limits.memory", docs[0]) == "128Mi"
        assert jmespath.search("spec.template.spec.containers[0].resources.limits.cpu", docs[0]) == "200m"
        assert (
            jmespath.search("spec.template.spec.containers[0].resources.requests.memory", docs[0]) == "169Mi"
        )
        assert jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", docs[0]) == "300m"

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

    def test_resources_are_not_added_by_default(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    @pytest.mark.parametrize(
        ("strategy", "expected_strategy"),
        [
            (None, None),
            (
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
            ),
        ],
    )
    def test_strategy(self, strategy, expected_strategy):
        """Strategy should be used when we aren't using both LocalExecutor and workers.persistence."""
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
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            "exec airflow dag-processor",
        ]

    @pytest.mark.parametrize(
        ("revision_history_limit", "global_revision_history_limit"),
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {
            "dagProcessor": {
                "enabled": True,
            }
        }
        if revision_history_limit:
            values["dagProcessor"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        expected_result = revision_history_limit or global_revision_history_limit
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

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

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

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}, "airflowLocalSettings": None},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        volume_mounts = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)
        volume_mounts_init = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts_init)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"dagProcessor": {"enabled": True}, "airflowLocalSettings": "# Well hello!"},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        volume_mount = {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        }
        assert volume_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert volume_mount in jmespath.search("spec.template.spec.initContainers[0].volumeMounts", docs[0])

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "annotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"

    @pytest.mark.parametrize(
        ("webserver_config", "should_add_volume"),
        [
            ("CSRF_ENABLED = True", True),
            (None, False),
        ],
    )
    def test_should_add_webserver_config_volume_and_volume_mount_when_exists(
        self, webserver_config, should_add_volume
    ):
        expected_volume = {
            "name": "webserver-config",
            "configMap": {"name": "release-name-webserver-config"},
        }
        expected_volume_mount = {
            "name": "webserver-config",
            "mountPath": "/opt/airflow/webserver_config.py",
            "subPath": "webserver_config.py",
            "readOnly": True,
        }

        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True},
                "webserver": {"webserverConfig": webserver_config},
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        created_volumes = jmespath.search("spec.template.spec.volumes", docs[0])
        created_volume_mounts = jmespath.search("spec.template.spec.containers[1].volumeMounts", docs[0])

        if should_add_volume:
            assert expected_volume in created_volumes
            assert expected_volume_mount in created_volume_mounts
        else:
            assert expected_volume not in created_volumes
            assert expected_volume_mount not in created_volume_mounts

    def test_validate_if_ssh_params_are_added_with_git_ssh_key(self):
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": True},
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "sshKey": "my-ssh-key",
                    },
                    "persistence": {"enabled": False},
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert {"name": "GIT_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "GIT_SYNC_SSH", "value": "true"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH", "value": "true"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "GIT_KNOWN_HOSTS", "value": "false"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH_KNOWN_HOSTS", "value": "false"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {
            "name": "git-sync-ssh-key",
            "secret": {"secretName": "release-name-ssh-secret", "defaultMode": 288},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

    @pytest.mark.parametrize(
        ("git_sync_values", "dags_persistence_enabled"),
        _get_git_sync_test_params_for_no_containers("dagProcessor"),
    )
    def test_git_sync_not_added_when_disabled_or_persistent_dags(
        self, git_sync_values, dags_persistence_enabled
    ):
        _test_git_sync_presence(
            git_sync_values=git_sync_values,
            dags_persistence_enabled=dags_persistence_enabled,
            template_path="templates/dag-processor/dag-processor-deployment.yaml",
            extra_values={"dagProcessor": {"enabled": True}},
            in_init_containers=False,
            in_containers=False,
        )

    @pytest.mark.parametrize(
        ("git_sync_values", "dags_persistence_enabled"),
        _get_enabled_git_sync_test_params("dagProcessor"),
    )
    def test_git_sync_added_when_enabled(self, git_sync_values, dags_persistence_enabled):
        _test_git_sync_presence(
            git_sync_values=git_sync_values,
            dags_persistence_enabled=dags_persistence_enabled,
            template_path="templates/dag-processor/dag-processor-deployment.yaml",
            extra_values={"dagProcessor": {"enabled": True}},
            in_init_containers=True,
            in_containers=True,
        )




class TestDagProcessorLogGroomer(LogGroomerTestBase):
    """DAG processor log groomer."""

    obj_name = "dag-processor"
    folder = "dag-processor"


class TestDagProcessorServiceAccount:
    """Tests DAG processor service account."""

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/dag-processor/dag-processor-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "enabled": True,
                    "serviceAccount": {"create": True, "automountServiceAccountToken": False},
                },
            },
            show_only=["templates/dag-processor/dag-processor-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
