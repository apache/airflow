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


class TestTriggerer:
    """Tests triggerer."""

    @pytest.mark.parametrize(
        ("airflow_version", "num_docs"),
        [
            ("2.1.0", 0),
            ("2.2.0", 1),
        ],
    )
    def test_only_exists_on_new_airflow_versions(self, airflow_version, num_docs):
        """Trigger was only added from Airflow 2.2 onwards."""
        docs = render_chart(
            values={"airflowVersion": airflow_version},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert num_docs == len(docs)

    def test_can_be_disabled(self):
        """
        Triggerer should be able to be disabled if the users desires.

        For example, user may be disabled when using Python 3.6 or doesn't want to use async tasks.
        """
        docs = render_chart(
            values={"triggerer": {"enabled": False}},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert len(docs) == 0

    @pytest.mark.parametrize(
        ("revision_history_limit", "global_revision_history_limit"),
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {
            "triggerer": {
                "enabled": True,
            }
        }
        if revision_history_limit:
            values["triggerer"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )
        expected_result = revision_history_limit or global_revision_history_limit
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']", docs[0]
        )
        assert actual is None

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "extraContainers": [
                        {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "release-name-test-container"
        }

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "extraInitContainers": [
                        {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "extraInitContainers": [{"name": "{{ .Release.Name }}-test-init-container"}],
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "release-name-test-init-container"
        }

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                    "extraVolumeMounts": [
                        {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                    ],
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1].name", docs[0]) == "test-volume-airflow"
        assert (
            jmespath.search("spec.template.spec.containers[0].volumeMounts[0].name", docs[0])
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
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1].name", docs[0]) == "test-volume"
        assert (
            jmespath.search("spec.template.spec.containers[0].volumeMounts[0].name", docs[0]) == "test-volume"
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "triggerer": {
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
                }
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
                "triggerer": {
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                    },
                }
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert jmespath.search("spec.template.metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_scheduler_name(self):
        docs = render_chart(
            values={"schedulerName": "airflow-scheduler"},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
                "triggerer": {
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
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "StatefulSet"
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
                "triggerer": {
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
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
                "triggerer": {
                    "livenessProbe": {
                        "initialDelaySeconds": 111,
                        "timeoutSeconds": 222,
                        "failureThreshold": 333,
                        "periodSeconds": 444,
                        "command": ["sh", "-c", "echo", "wow such test"],
                    }
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
            ("2.4.9", "airflow jobs check --job-type TriggererJob --hostname $(hostname)"),
            ("2.5.0", "airflow jobs check --job-type TriggererJob --local"),
        ],
    )
    def test_livenessprobe_command_depends_on_airflow_version(self, airflow_version, probe_command):
        docs = render_chart(
            values={"airflowVersion": f"{airflow_version}"},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
                "triggerer": {"persistence": {"enabled": False}},
                "logs": log_values,
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1]", docs[0]) == {
            "name": "logs",
            **expected_volume,
        }

    def test_resources_are_configurable(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    }
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    @pytest.mark.parametrize(
        ("persistence", "update_strategy", "expected_update_strategy"),
        [
            (False, None, None),
            (True, {"rollingUpdate": {"partition": 0}}, {"rollingUpdate": {"partition": 0}}),
            (True, None, None),
        ],
    )
    def test_update_strategy(self, persistence, update_strategy, expected_update_strategy):
        docs = render_chart(
            values={
                "airflowVersion": "2.6.0",
                "executor": "CeleryExecutor",
                "triggerer": {
                    "persistence": {"enabled": persistence},
                    "updateStrategy": update_strategy,
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert expected_update_strategy == jmespath.search("spec.updateStrategy", docs[0])

    @pytest.mark.parametrize(
        ("persistence", "strategy", "expected_strategy"),
        [
            (True, None, None),
            (
                False,
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
            ),
            (False, None, None),
        ],
    )
    def test_strategy(self, persistence, strategy, expected_strategy):
        docs = render_chart(
            values={
                "triggerer": {"persistence": {"enabled": persistence}, "strategy": strategy},
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert expected_strategy == jmespath.search("spec.strategy", docs[0])

    def test_default_command_and_args(self):
        docs = render_chart(
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            "exec airflow triggerer",
        ]

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"triggerer": {"command": command, "args": args}},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "triggerer": {"command": ["{{ .Release.Name }}"], "args": ["{{ .Release.Service }}"]},
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

    def test_dags_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])]
        assert "git-sync-init" in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    def test_dags_gitsync_with_persistence_no_sidecar_or_init_container(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}}},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
            values={"airflowLocalSettings": None}, show_only=["templates/triggerer/triggerer-deployment.yaml"]
        )
        volume_mounts = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)
        volume_mounts_init = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts_init)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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
                "triggerer": {
                    "annotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"

    def test_triggerer_pod_hostaliases(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "hostAliases": [{"ip": "127.0.0.1", "hostnames": ["foo.local"]}],
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.hostAliases[0].ip", docs[0]) == "127.0.0.1"
        assert jmespath.search("spec.template.spec.hostAliases[0].hostnames[0]", docs[0]) == "foo.local"

    def test_triggerer_template_storage_class_name(self):
        docs = render_chart(
            values={"triggerer": {"persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"}}},
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.volumeClaimTemplates[0].spec.storageClassName", docs[0])
            == "release-name-storage-class"
        )

    def test_persistent_volume_claim_retention_policy(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "triggerer": {
                    "persistence": {
                        "enabled": True,
                        "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"},
                    }
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert jmespath.search("spec.persistentVolumeClaimRetentionPolicy", docs[0]) == {
            "whenDeleted": "Delete",
        }


class TestTriggererServiceAccount:
    """Tests triggerer service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/triggerer/triggerer-serviceaccount.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/triggerer/triggerer-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "serviceAccount": {"create": True, "automountServiceAccountToken": False},
                },
            },
            show_only=["templates/triggerer/triggerer-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False


class TestTriggererLogGroomer(LogGroomerTestBase):
    """Triggerer log groomer."""

    obj_name = "triggerer"
    folder = "triggerer"


class TestTriggererKedaAutoScaler:
    """Tests triggerer keda autoscaler."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "keda": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/triggerer/triggerer-kedaautoscaler.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_should_remove_replicas_field(self):
        docs = render_chart(
            values={
                "triggerer": {
                    "keda": {"enabled": True},
                },
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )

        assert "replicas" not in jmespath.search("spec", docs[0])

    @pytest.mark.parametrize(
        "executor", ["CeleryExecutor", "CeleryKubernetesExecutor", "CeleryExecutor,KubernetesExecutor"]
    )
    def test_include_event_source_container_name_in_scaled_object_for_triggerer(self, executor):
        docs = render_chart(
            values={
                "triggerer": {
                    "keda": {"enabled": True},
                },
            },
            show_only=["templates/triggerer/triggerer-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.scaleTargetRef.envSourceContainerName", docs[0]) == "triggerer"

    @pytest.mark.parametrize(
        ("query", "expected_query"),
        [
            # default query
            (
                None,
                "SELECT ceil(COUNT(*)::decimal / 1000) FROM trigger",
            ),
            # test custom static query
            (
                "SELECT ceil(COUNT(*)::decimal / 2000) FROM trigger",
                "SELECT ceil(COUNT(*)::decimal / 2000) FROM trigger",
            ),
            # test custom template query
            (
                'SELECT ceil(COUNT(*)::decimal / {{ mul (include "triggerer.capacity" . | int) 2 }})'
                " FROM trigger",
                "SELECT ceil(COUNT(*)::decimal / 2000) FROM trigger",
            ),
        ],
    )
    def test_should_use_keda_query(self, query, expected_query):
        docs = render_chart(
            values={
                "triggerer": {
                    "enabled": True,
                    "keda": {"enabled": True, **({"query": query} if query else {})},
                },
            },
            show_only=["templates/triggerer/triggerer-kedaautoscaler.yaml"],
        )
        assert expected_query == jmespath.search("spec.triggers[0].metadata.query", docs[0])

    def test_mysql_db_backend_keda_default_value(self):
        docs = render_chart(
            values={
                "data": {"metadataConnection": {"protocol": "mysql"}},
                "triggerer": {
                    "enabled": True,
                    "keda": {"enabled": True},
                },
            },
            show_only=["templates/triggerer/triggerer-kedaautoscaler.yaml"],
        )

        assert jmespath.search("spec.triggerers[0].metadata.keda.usePgBouncer", docs[0]) is None

    def test_mysql_db_backend_keda(self):
        docs = render_chart(
            values={
                "data": {"metadataConnection": {"protocol": "mysql"}},
                "triggerer": {
                    "enabled": True,
                    "keda": {"enabled": True},
                },
            },
            show_only=["templates/triggerer/triggerer-kedaautoscaler.yaml"],
        )
        assert jmespath.search("spec.triggers[0].metadata.queryValue", docs[0]) == "1"
        assert jmespath.search("spec.triggers[0].metadata.targetQueryValue", docs[0]) is None

        assert jmespath.search("spec.triggers[0].metadata.connectionStringFromEnv", docs[0]) == "KEDA_DB_CONN"
        assert jmespath.search("spec.triggers[0].metadata.connectionFromEnv", docs[0]) is None
