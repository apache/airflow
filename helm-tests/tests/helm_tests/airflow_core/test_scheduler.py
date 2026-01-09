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


class TestScheduler:
    """Tests scheduler."""

    @pytest.mark.parametrize(
        ("executor", "workers_values", "kind"),
        [
            # Test workers.celery.persistence.enabled flag
            ("CeleryExecutor", {"celery": {"persistence": {"enabled": False}}}, "Deployment"),
            ("CeleryExecutor", {"celery": {"persistence": {"enabled": True}}}, "Deployment"),
            ("CeleryKubernetesExecutor", {"celery": {"persistence": {"enabled": True}}}, "Deployment"),
            (
                "CeleryExecutor,KubernetesExecutor",
                {"celery": {"persistence": {"enabled": True}}},
                "Deployment",
            ),
            ("KubernetesExecutor", {"celery": {"persistence": {"enabled": True}}}, "Deployment"),
            ("LocalKubernetesExecutor", {"celery": {"persistence": {"enabled": False}}}, "Deployment"),
            ("LocalKubernetesExecutor", {"celery": {"persistence": {"enabled": True}}}, "StatefulSet"),
            ("LocalExecutor", {"celery": {"persistence": {"enabled": True}}}, "StatefulSet"),
            (
                "LocalExecutor,KubernetesExecutor",
                {"celery": {"persistence": {"enabled": True}}},
                "StatefulSet",
            ),
            ("LocalExecutor", {"celery": {"persistence": {"enabled": False}}}, "Deployment"),
            # Test workers.persistence.enabled flag when celery one is default (expected no impact on kind)
            ("CeleryExecutor", {"persistence": {"enabled": False}}, "Deployment"),
            ("CeleryExecutor", {"persistence": {"enabled": True}}, "Deployment"),
            ("CeleryKubernetesExecutor", {"persistence": {"enabled": True}}, "Deployment"),
            ("CeleryExecutor,KubernetesExecutor", {"persistence": {"enabled": True}}, "Deployment"),
            ("KubernetesExecutor", {"persistence": {"enabled": True}}, "Deployment"),
            ("LocalKubernetesExecutor", {"persistence": {"enabled": False}}, "StatefulSet"),
            ("LocalKubernetesExecutor", {"persistence": {"enabled": True}}, "StatefulSet"),
            ("LocalExecutor", {"persistence": {"enabled": True}}, "StatefulSet"),
            ("LocalExecutor,KubernetesExecutor", {"persistence": {"enabled": True}}, "StatefulSet"),
            ("LocalExecutor", {"persistence": {"enabled": False}}, "StatefulSet"),
            # Test workers.persistence.enabled flag when celery one is unset
            (
                "CeleryExecutor",
                {"persistence": {"enabled": False}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
            (
                "CeleryExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
            (
                "CeleryKubernetesExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
            (
                "CeleryExecutor,KubernetesExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
            (
                "KubernetesExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
            (
                "LocalKubernetesExecutor",
                {"persistence": {"enabled": False}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
            (
                "LocalKubernetesExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "StatefulSet",
            ),
            (
                "LocalExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "StatefulSet",
            ),
            (
                "LocalExecutor,KubernetesExecutor",
                {"persistence": {"enabled": True}, "celery": {"persistence": {"enabled": None}}},
                "StatefulSet",
            ),
            (
                "LocalExecutor",
                {"persistence": {"enabled": False}, "celery": {"persistence": {"enabled": None}}},
                "Deployment",
            ),
        ],
    )
    def test_scheduler_kind(self, executor, workers_values, kind):
        """Test scheduler kind is StatefulSet only with a local executor & worker persistence is enabled."""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": workers_values,
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert kind == jmespath.search("kind", docs[0])

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "scheduler": {
                    "extraContainers": [
                        {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "scheduler": {
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "release-name-test-container"
        }

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']", docs[0]
        )
        assert actual is None

    @pytest.mark.parametrize(
        ("logs_values", "expect_sub_path"),
        [
            ({"persistence": {"enabled": False}}, None),
            ({"persistence": {"enabled": True, "subPath": "test/logs"}}, "test/logs"),
        ],
    )
    def test_logs_mount_on_wait_for_migrations_initcontainer(self, logs_values, expect_sub_path):
        docs = render_chart(
            values={
                "logs": logs_values,
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        mounts = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations'] | [0].volumeMounts",
            docs[0],
        )
        assert mounts is not None, (
            "wait-for-airflow-migrations initContainer not found or has no volumeMounts"
        )
        assert any(m.get("name") == "logs" and m.get("mountPath") == "/opt/airflow/logs" for m in mounts)
        if expect_sub_path is not None:
            assert any(
                m.get("name") == "logs"
                and m.get("mountPath") == "/opt/airflow/logs"
                and m.get("subPath") == expect_sub_path
                for m in mounts
            )

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "extraInitContainers": [
                        {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "extraInitContainers": [{"name": "{{ .Release.Name }}-test-init-container"}],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[-1]", docs[0]) == {
            "name": "release-name-test-init-container"
        }

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "scheduler": {
                    "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                    "extraVolumeMounts": [
                        {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                    ],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "test-volume-airflow" in jmespath.search("spec.template.spec.volumes[*].name", docs[0])
        assert "test-volume-airflow" in jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[*].name", docs[0]
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "test-volume" in jmespath.search("spec.template.spec.volumes[*].name", docs[0])
        assert "test-volume" in jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[*].name", docs[0]
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "scheduler": {
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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
                "scheduler": {
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "scheduler": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert jmespath.search("spec.template.metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        ("revision_history_limit", "global_revision_history_limit"),
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {"scheduler": {}}
        if revision_history_limit:
            values["scheduler"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        expected_result = revision_history_limit or global_revision_history_limit
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "scheduler": {
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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
        """When given both global and scheduler affinity etc, scheduler affinity etc is used."""
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
                "scheduler": {
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.schedulerName",
                docs[0],
            )
            == "airflow-scheduler"
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
                "scheduler": {
                    "livenessProbe": {
                        "initialDelaySeconds": 111,
                        "timeoutSeconds": 222,
                        "failureThreshold": 333,
                        "periodSeconds": 444,
                        "command": ["sh", "-c", "echo", "wow such test"],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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

    def test_startupprobe_values_are_configurable(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "startupProbe": {
                        "timeoutSeconds": 111,
                        "failureThreshold": 222,
                        "periodSeconds": 333,
                        "command": ["sh", "-c", "echo", "wow such test"],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].startupProbe.timeoutSeconds", docs[0]) == 111
        assert (
            jmespath.search("spec.template.spec.containers[0].startupProbe.failureThreshold", docs[0]) == 222
        )
        assert jmespath.search("spec.template.spec.containers[0].startupProbe.periodSeconds", docs[0]) == 333
        assert jmespath.search("spec.template.spec.containers[0].startupProbe.exec.command", docs[0]) == [
            "sh",
            "-c",
            "echo",
            "wow such test",
        ]

    @pytest.mark.parametrize(
        ("airflow_version", "probe_command"),
        [
            ("1.9.0", "from airflow.jobs.scheduler_job import SchedulerJob"),
            ("2.1.0", "airflow jobs check --job-type SchedulerJob --hostname $(hostname)"),
            ("2.5.0", "airflow jobs check --job-type SchedulerJob --local"),
        ],
    )
    def test_livenessprobe_command_depends_on_airflow_version(self, airflow_version, probe_command):
        docs = render_chart(
            values={"airflowVersion": f"{airflow_version}"},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert (
            probe_command
            in jmespath.search("spec.template.spec.containers[0].livenessProbe.exec.command", docs[0])[-1]
        )

    @pytest.mark.parametrize(
        ("airflow_version", "probe_command"),
        [
            ("1.9.0", "from airflow.jobs.scheduler_job import SchedulerJob"),
            ("2.1.0", "airflow jobs check --job-type SchedulerJob --hostname $(hostname)"),
            ("2.5.0", "airflow jobs check --job-type SchedulerJob --local"),
        ],
    )
    def test_startupprobe_command_depends_on_airflow_version(self, airflow_version, probe_command):
        docs = render_chart(
            values={"airflowVersion": f"{airflow_version}"},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert (
            probe_command
            in jmespath.search("spec.template.spec.containers[0].startupProbe.exec.command", docs[0])[-1]
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
            values={"logs": log_values},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "logs", **expected_volume} in jmespath.search("spec.template.spec.volumes", docs[0])

    def test_scheduler_security_contexts_are_configurable(self):
        docs = render_chart(
            values={
                "scheduler": {
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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

    def test_scheduler_security_context_legacy(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "securityContext": {
                        "fsGroup": 1000,
                        "runAsGroup": 1001,
                        "runAsNonRoot": True,
                        "runAsUser": 2000,
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.securityContext", docs[0]) == {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        }

    def test_scheduler_resources_are_configurable(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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

    def test_scheduler_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": None}, show_only=["templates/scheduler/scheduler-deployment.yaml"]
        )
        volume_mounts = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)
        volume_mounts_init = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts_init)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        volume_mount = {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        }
        assert volume_mount in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert volume_mount in jmespath.search("spec.template.spec.initContainers[0].volumeMounts", docs[0])

    @pytest.mark.parametrize(
        ("executor", "persistence", "update_strategy", "expected_update_strategy"),
        [
            ("CeleryExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            ("CeleryExecutor", True, {"rollingUpdate": {"partition": 0}}, None),
            ("LocalKubernetesExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            ("LocalExecutor,KubernetesExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            (
                "LocalKubernetesExecutor",
                True,
                {"rollingUpdate": {"partition": 0}},
                {"rollingUpdate": {"partition": 0}},
            ),
            (
                "LocalExecutor,KubernetesExecutor",
                True,
                {"rollingUpdate": {"partition": 0}},
                {"rollingUpdate": {"partition": 0}},
            ),
            ("LocalExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            ("LocalExecutor", True, {"rollingUpdate": {"partition": 0}}, {"rollingUpdate": {"partition": 0}}),
            ("LocalExecutor", True, None, None),
            ("LocalExecutor,KubernetesExecutor", True, None, None),
        ],
    )
    def test_scheduler_update_strategy(
        self, executor, persistence, update_strategy, expected_update_strategy
    ):
        """UpdateStrategy should only be used when we have a local executor and workers.persistence."""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"celery": {"persistence": {"enabled": persistence}}},
                "scheduler": {"updateStrategy": update_strategy},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert expected_update_strategy == jmespath.search("spec.updateStrategy", docs[0])

    @pytest.mark.parametrize(
        ("executor", "persistence", "strategy", "expected_strategy"),
        [
            ("LocalExecutor", False, None, None),
            ("LocalExecutor", False, {"type": "Recreate"}, {"type": "Recreate"}),
            ("LocalExecutor", True, {"type": "Recreate"}, None),
            ("LocalKubernetesExecutor", False, {"type": "Recreate"}, {"type": "Recreate"}),
            ("LocalKubernetesExecutor", True, {"type": "Recreate"}, None),
            ("CeleryExecutor", True, None, None),
            ("CeleryExecutor", False, None, None),
            ("CeleryExecutor", True, {"type": "Recreate"}, {"type": "Recreate"}),
            (
                "CeleryExecutor",
                False,
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
                {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}},
            ),
        ],
    )
    def test_scheduler_strategy(self, executor, persistence, strategy, expected_strategy):
        """Strategy should be used when we aren't using both a local executor and workers.persistence."""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"celery": {"persistence": {"enabled": persistence}}},
                "scheduler": {"strategy": strategy},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert expected_strategy == jmespath.search("spec.strategy", docs[0])

    def test_default_command_and_args(self):
        docs = render_chart(show_only=["templates/scheduler/scheduler-deployment.yaml"])

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == [
            "bash",
            "-c",
            "exec airflow scheduler",
        ]

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"scheduler": {"command": command, "args": args}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={"scheduler": {"command": ["{{ .Release.Name }}"], "args": ["{{ .Release.Service }}"]}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

    @pytest.mark.parametrize(
        "dags_values",
        [
            {"gitSync": {"enabled": True}},
            {"gitSync": {"enabled": True}, "persistence": {"enabled": True}},
        ],
    )
    def test_dags_gitsync_sidecar_and_init_container_with_airflow_2(self, dags_values):
        docs = render_chart(
            values={"dags": dags_values, "airflowVersion": "2.10.4"},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])]
        assert "git-sync-init" in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    @pytest.mark.parametrize(
        ("airflow_version", "dag_processor", "executor", "skip_dags_mount"),
        [
            # standalone dag_processor is optional on 2.10, so we can skip dags for non-local if its on
            ("2.10.4", True, "LocalExecutor", False),
            ("2.10.4", True, "CeleryExecutor", True),
            ("2.10.4", True, "KubernetesExecutor", True),
            ("2.10.4", True, "LocalKubernetesExecutor", False),
            # but if standalone dag_processor is off, we must always have dags
            ("2.10.4", False, "LocalExecutor", False),
            ("2.10.4", False, "CeleryExecutor", False),
            ("2.10.4", False, "KubernetesExecutor", False),
            ("2.10.4", False, "LocalKubernetesExecutor", False),
            # by default, we don't have a standalone dag_processor
            ("2.10.4", None, "LocalExecutor", False),
            ("2.10.4", None, "CeleryExecutor", False),
            ("2.10.4", None, "KubernetesExecutor", False),
            ("2.10.4", None, "LocalKubernetesExecutor", False),
            # but in airflow 3, standalone dag_processor required, so we again can skip dags for non-local
            ("3.0.0", None, "LocalExecutor", False),
            ("3.0.0", None, "CeleryExecutor", True),
            ("3.0.0", None, "KubernetesExecutor", True),
            ("3.0.0", None, "LocalKubernetesExecutor", False),
        ],
    )
    def test_dags_mount_and_gitsync_expected_with_dag_processor(
        self, airflow_version, dag_processor, executor, skip_dags_mount
    ):
        """
        DAG Processor can move gitsync and DAGs mount from the scheduler to the DAG Processor only.

        The only exception is when we have a Local executor.
        In these cases, the scheduler does the worker role and needs access to DAGs anyway.
        """
        values = {
            "airflowVersion": airflow_version,
            "executor": executor,
            "dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}},
            "scheduler": {"logGroomerSidecar": {"enabled": False}},
        }
        if dag_processor is not None:
            values["dagProcessor"] = {"enabled": dag_processor}
        docs = render_chart(
            values=values,
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        if skip_dags_mount:
            assert "dags" not in [
                vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
            ]
            assert "dags" not in [vm["name"] for vm in jmespath.search("spec.template.spec.volumes", docs[0])]
            assert len(jmespath.search("spec.template.spec.containers", docs[0])) == 1
        else:
            assert "dags" in [
                vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
            ]
            assert "dags" in [vm["name"] for vm in jmespath.search("spec.template.spec.volumes", docs[0])]
            assert "git-sync" in [
                c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])
            ]
            assert "git-sync-init" in [
                c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
            ]

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"persistence": {"annotations": {"foo": "bar"}}},
            {"celery": {"persistence": {"annotations": {"foo": "bar"}}}},
            {
                "persistence": {"annotations": {"a": "b"}},
                "celery": {"persistence": {"annotations": {"foo": "bar"}}},
            },
        ],
    )
    def test_persistence_volume_annotations(self, workers_values):
        docs = render_chart(
            values={"executor": "LocalExecutor", "workers": workers_values},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert jmespath.search("spec.volumeClaimTemplates[0].metadata.annotations", docs[0]) == {"foo": "bar"}

    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "LocalKubernetesExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "CeleryKubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ],
    )
    def test_scheduler_deployment_has_executor_label(self, executor):
        docs = render_chart(
            values={"executor": executor},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert len(docs) == 1
        assert executor.replace(",", "-") == docs[0]["metadata"]["labels"].get("executor")

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "annotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"

    def test_scheduler_pod_hostaliases(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "hostAliases": [{"ip": "127.0.0.1", "hostnames": ["foo.local"]}],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.hostAliases[0].ip", docs[0]) == "127.0.0.1"
        assert jmespath.search("spec.template.spec.hostAliases[0].hostnames[0]", docs[0]) == "foo.local"

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"},
                "celery": {"enabled": True},
            },
            {
                "celery": {
                    "enabled": True,
                    "persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"},
                }
            },
            {
                "persistence": {"storageClassName": "{{ .Release.Name }}"},
                "celery": {
                    "enabled": True,
                    "persistence": {"storageClassName": "{{ .Release.Name }}-storage-class"},
                },
            },
        ],
    )
    def test_scheduler_template_storage_class_name(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
                "logs": {"persistence": {"enabled": False}},
                "executor": "LocalExecutor",
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.volumeClaimTemplates[0].spec.storageClassName", docs[0])
            == "release-name-storage-class"
        )

    @pytest.mark.parametrize(
        "workers_values",
        [
            {
                "persistence": {"persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}},
                "celery": {"enabled": True},
            },
            {
                "celery": {
                    "enabled": True,
                    "persistence": {"persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}},
                }
            },
            {
                "persistence": {"persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Retain"}},
                "celery": {
                    "enabled": True,
                    "persistence": {"persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"}},
                },
            },
        ],
    )
    def test_persistent_volume_claim_retention_policy(self, workers_values):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "workers": workers_values,
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.persistentVolumeClaimRetentionPolicy", docs[0]) == {
            "whenDeleted": "Delete",
        }

    @pytest.mark.parametrize(
        ("scheduler_values", "expected"),
        [
            ({}, 10),
            ({"scheduler": {"terminationGracePeriodSeconds": 1200}}, 1200),
        ],
    )
    def test_scheduler_termination_grace_period_seconds(self, scheduler_values, expected):
        docs = render_chart(
            values=scheduler_values,
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert expected == jmespath.search("spec.template.spec.terminationGracePeriodSeconds", docs[0])

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"persistence": {"size": "50Gi"}, "celery": {"persistence": {"size": None}}},
            {"celery": {"persistence": {"size": "50Gi"}}},
            {"persistence": {"size": "10Gi"}, "celery": {"persistence": {"size": "50Gi"}}},
        ],
    )
    def test_scheduler_template_storage_size(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
                "logs": {"persistence": {"enabled": False}},
                "executor": "LocalExecutor",
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.volumeClaimTemplates[0].spec.resources.requests.storage", docs[0]) == "50Gi"
        )


class TestSchedulerNetworkPolicy:
    """Tests scheduler network policy."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "scheduler": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-networkpolicy.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"


class TestSchedulerLogGroomer(LogGroomerTestBase):
    """Scheduler log groomer."""

    obj_name = "scheduler"
    folder = "scheduler"


class TestSchedulerService:
    """Tests scheduler service."""

    @pytest.mark.parametrize(
        ("executor", "creates_service"),
        [
            ("LocalExecutor", True),
            ("CeleryExecutor", False),
            ("CeleryKubernetesExecutor", False),
            ("KubernetesExecutor", False),
            ("LocalKubernetesExecutor", True),
        ],
    )
    def test_should_create_scheduler_service_for_specific_executors(self, executor, creates_service):
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-service.yaml"],
        )
        if creates_service:
            assert jmespath.search("kind", docs[0]) == "Service"
            assert "test_label" in jmespath.search("metadata.labels", docs[0])
            assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"
        else:
            assert docs == []

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "scheduler": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-service.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        ("executor", "expected_label"),
        [
            ("LocalExecutor", "LocalExecutor"),
            ("CeleryExecutor", "CeleryExecutor"),
            ("CeleryKubernetesExecutor", "CeleryKubernetesExecutor"),
            ("KubernetesExecutor", "KubernetesExecutor"),
            ("LocalKubernetesExecutor", "LocalKubernetesExecutor"),
            ("CeleryExecutor,KubernetesExecutor", "CeleryExecutor-KubernetesExecutor"),
        ],
    )
    def test_should_add_executor_labels(self, executor, expected_label):
        docs = render_chart(
            values={
                "executor": executor,
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "executor" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["executor"] == expected_label


class TestSchedulerServiceAccount:
    """Tests scheduler service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    @pytest.mark.parametrize(
        ("executor", "default_automount_service_account"),
        [
            ("LocalExecutor", None),
            ("CeleryExecutor", None),
            ("CeleryKubernetesExecutor", None),
            ("KubernetesExecutor", None),
            ("LocalKubernetesExecutor", None),
            ("CeleryExecutor,KubernetesExecutor", None),
        ],
    )
    def test_default_automount_service_account_token(self, executor, default_automount_service_account):
        docs = render_chart(
            values={
                "scheduler": {
                    "serviceAccount": {"create": True},
                },
                "executor": executor,
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is default_automount_service_account

    @pytest.mark.parametrize(
        ("executor", "automount_service_account", "should_automount_service_account"),
        [
            ("LocalExecutor", True, None),
            ("CeleryExecutor", False, False),
            ("CeleryKubernetesExecutor", False, False),
            ("KubernetesExecutor", False, False),
            ("LocalKubernetesExecutor", False, False),
            ("CeleryExecutor,KubernetesExecutor", False, False),
        ],
    )
    def test_overridden_automount_service_account_token(
        self, executor, automount_service_account, should_automount_service_account
    ):
        docs = render_chart(
            values={
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": automount_service_account,
                    },
                },
                "executor": executor,
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is should_automount_service_account


class TestSchedulerServiceAccountTokenVolume:
    """Tests scheduler service account token volume features."""

    @pytest.mark.parametrize(
        ("executor", "is_pod_launching"),
        [
            ("LocalExecutor", False),
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
            ("CeleryExecutor,KubernetesExecutor", True),
        ],
    )
    def test_pod_level_automount_service_account_token_default(self, executor, is_pod_launching):
        """Test that pod-level automountServiceAccountToken is not set by default for any executor."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # With default values, automountServiceAccountToken should not be set for any executor
        assert jmespath.search("spec.template.spec.automountServiceAccountToken", docs[0]) is None

    @pytest.mark.parametrize(
        ("executor", "automount_service_account", "is_pod_launching"),
        [
            ("LocalExecutor", False, False),
            ("CeleryExecutor", False, True),
            ("CeleryKubernetesExecutor", False, True),
            ("KubernetesExecutor", False, True),
            ("LocalKubernetesExecutor", False, True),
            ("CeleryExecutor,KubernetesExecutor", False, True),
        ],
    )
    def test_pod_level_automount_service_account_token_disabled(
        self, executor, automount_service_account, is_pod_launching
    ):
        """Test that pod-level automountServiceAccountToken is set to false when disabled for pod-launching executors."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": automount_service_account,
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        expected_value = False if is_pod_launching else None
        assert jmespath.search("spec.template.spec.automountServiceAccountToken", docs[0]) is expected_value

    @pytest.mark.parametrize(
        ("executor", "is_pod_launching"),
        [
            ("LocalExecutor", False),
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
            ("CeleryExecutor,KubernetesExecutor", True),
        ],
    )
    def test_service_account_token_volume_not_created_when_disabled(self, executor, is_pod_launching):
        """Test that service account token volume is not created when serviceAccountTokenVolume is disabled."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": False},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        volume_names = [v.get("name") for v in volumes]
        assert "service-account-token" not in volume_names

    @pytest.mark.parametrize(
        ("executor", "is_pod_launching"),
        [
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
            ("CeleryExecutor,KubernetesExecutor", True),
        ],
    )
    def test_service_account_token_volume_created_when_enabled(self, executor, is_pod_launching):
        """Test that service account token volume is created when enabled for pod-launching executors."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        service_account_volume = next((v for v in volumes if v.get("name") == "kube-api-access"), None)

        assert service_account_volume is not None
        assert "projected" in service_account_volume
        assert "sources" in service_account_volume["projected"]

        sources = service_account_volume["projected"]["sources"]
        assert len(sources) == 3

        # Check serviceAccountToken source
        sa_token_source = next((s for s in sources if "serviceAccountToken" in s), None)
        assert sa_token_source is not None
        assert sa_token_source["serviceAccountToken"]["path"] == "token"
        assert sa_token_source["serviceAccountToken"]["expirationSeconds"] == 3600

        # Check configMap source
        configmap_source = next((s for s in sources if "configMap" in s), None)
        assert configmap_source is not None
        assert configmap_source["configMap"]["name"] == "kube-root-ca.crt"
        assert configmap_source["configMap"]["items"][0]["key"] == "ca.crt"
        assert configmap_source["configMap"]["items"][0]["path"] == "ca.crt"

        # Check downwardAPI source
        downward_source = next((s for s in sources if "downwardAPI" in s), None)
        assert downward_source is not None
        assert downward_source["downwardAPI"]["items"][0]["path"] == "namespace"
        assert downward_source["downwardAPI"]["items"][0]["fieldRef"]["fieldPath"] == "metadata.namespace"

    def test_service_account_token_volume_not_created_for_non_pod_launching_executor(self):
        """Test that service account token volume is not created for non-pod-launching executors."""
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        volume_names = [v.get("name") for v in volumes]
        assert "service-account-token" not in volume_names

    @pytest.mark.parametrize(
        ("executor", "mount_path", "expiration_seconds"),
        [
            ("CeleryExecutor", "/custom/path", 7200),
            ("KubernetesExecutor", "/var/run/secrets/kubernetes.io/serviceaccount", 1800),
            ("LocalKubernetesExecutor", "/opt/airflow/secrets", 3600),
        ],
    )
    def test_service_account_token_volume_custom_configuration(
        self, executor, mount_path, expiration_seconds
    ):
        """Test that service account token volume respects custom configuration."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {
                            "enabled": True,
                            "mountPath": mount_path,
                            "expirationSeconds": expiration_seconds,
                        },
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # Check volume configuration
        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        service_account_volume = next((v for v in volumes if v.get("name") == "kube-api-access"), None)

        assert service_account_volume is not None
        sources = service_account_volume["projected"]["sources"]
        sa_token_source = next((s for s in sources if "serviceAccountToken" in s), None)
        assert sa_token_source["serviceAccountToken"]["expirationSeconds"] == expiration_seconds

    @pytest.mark.parametrize(
        ("executor", "container_index", "container_name"),
        [
            ("CeleryExecutor", 0, "scheduler"),
            ("KubernetesExecutor", 0, "scheduler"),
            ("LocalKubernetesExecutor", 0, "scheduler"),
        ],
    )
    def test_service_account_token_volume_mounted_in_scheduler_container(
        self, executor, container_index, container_name
    ):
        """Test that service account token volume is mounted in the scheduler container."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volume_mounts = (
            jmespath.search(f"spec.template.spec.containers[{container_index}].volumeMounts", docs[0]) or []
        )
        sa_token_mount = next((vm for vm in volume_mounts if vm.get("name") == "kube-api-access"), None)

        assert sa_token_mount is not None
        assert sa_token_mount["mountPath"] == "/var/run/secrets/kubernetes.io/serviceaccount"
        assert sa_token_mount["readOnly"] is True

    @pytest.mark.parametrize(
        ("executor", "has_init_containers"),
        [
            ("CeleryExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
        ],
    )
    def test_service_account_token_volume_NOT_mounted_in_init_containers(self, executor, has_init_containers):
        """Test that service account token volume is NOT mounted in init containers for security."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
        if has_init_containers and init_containers:
            # Check first init container (wait-for-airflow-migrations)
            volume_mounts = init_containers[0].get("volumeMounts", [])
            sa_token_mount = next((vm for vm in volume_mounts if vm.get("name") == "kube-api-access"), None)

            # SECURITY: Init containers should NOT have service account token access
            assert sa_token_mount is None, (
                "Init container should not have service account token volume mount for security"
            )

    def test_service_account_token_volume_NOT_mounted_in_log_groomer_sidecar(self):
        """Test that service account token volume is NOT mounted in log groomer sidecar for security."""
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                    "logGroomerSidecar": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        containers = jmespath.search("spec.template.spec.containers", docs[0]) or []
        log_groomer_container = next(
            (c for c in containers if c.get("name") == "scheduler-log-groomer"), None
        )

        if log_groomer_container:
            volume_mounts = log_groomer_container.get("volumeMounts", [])
            sa_token_mount = next((vm for vm in volume_mounts if vm.get("name") == "kube-api-access"), None)

            # SECURITY: Sidecar containers should NOT have service account token access
            assert sa_token_mount is None, (
                "Log groomer sidecar should not have service account token volume mount for security"
            )

    def test_service_account_token_volume_conditional_logic(self):
        """Test that service account token volume is only created when all conditions are met."""
        # Test case 1: Pod-launching executor + automountServiceAccountToken=false + serviceAccountTokenVolume.enabled=true
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        volume_names = [v.get("name") for v in volumes]
        assert "kube-api-access" in volume_names

        # Test case 2: Pod-launching executor + automountServiceAccountToken=true + serviceAccountTokenVolume.enabled=true
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": True,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        volume_names = [v.get("name") for v in volumes]
        assert "service-account-token" not in volume_names

        # Test case 3: Non-pod-launching executor + automountServiceAccountToken=false + serviceAccountTokenVolume.enabled=true
        docs = render_chart(
            values={
                "executor": "LocalExecutor",
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        volume_names = [v.get("name") for v in volumes]
        assert "service-account-token" not in volume_names

    @pytest.mark.parametrize(
        ("executor", "is_pod_launching"),
        [
            ("LocalExecutor", False),
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
            ("CeleryExecutor,KubernetesExecutor", True),
        ],
    )
    def test_service_account_token_volume_mount_only_in_scheduler_container(self, executor, is_pod_launching):
        """Test that Service Account Token Volume is mounted ONLY in scheduler container, not in init or sidecar containers."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                    "logGroomerSidecar": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        if not is_pod_launching:
            # For non-pod-launching executors, no service account token volume should be created
            volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
            volume_names = [v.get("name") for v in volumes]
            assert "kube-api-access" not in volume_names
            return

        # For pod-launching executors, check container-specific mount logic
        containers = jmespath.search("spec.template.spec.containers", docs[0]) or []
        init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []

        # 1. POSITIVE TEST: Scheduler container SHOULD have the mount
        scheduler_container = next((c for c in containers if c.get("name") == "scheduler"), None)
        assert scheduler_container is not None, "Scheduler container should exist"

        scheduler_volume_mounts = scheduler_container.get("volumeMounts", [])
        scheduler_sa_token_mount = next(
            (vm for vm in scheduler_volume_mounts if vm.get("name") == "kube-api-access"), None
        )
        assert scheduler_sa_token_mount is not None, (
            "Scheduler container should have service account token volume mount"
        )
        assert scheduler_sa_token_mount["mountPath"] == "/var/run/secrets/kubernetes.io/serviceaccount"
        assert scheduler_sa_token_mount["readOnly"] is True

        # 2. NEGATIVE TEST: Init containers should NOT have the mount
        for init_container in init_containers:
            init_volume_mounts = init_container.get("volumeMounts", [])
            init_sa_token_mount = next(
                (vm for vm in init_volume_mounts if vm.get("name") == "kube-api-access"), None
            )
            assert init_sa_token_mount is None, (
                f"Init container '{init_container.get('name')}' should not have service account token volume mount"
            )

        # 3. NEGATIVE TEST: Sidecar containers should NOT have the mount
        sidecar_containers = [c for c in containers if c.get("name") != "scheduler"]
        for sidecar_container in sidecar_containers:
            sidecar_volume_mounts = sidecar_container.get("volumeMounts", [])
            sidecar_sa_token_mount = next(
                (vm for vm in sidecar_volume_mounts if vm.get("name") == "kube-api-access"), None
            )
            assert sidecar_sa_token_mount is None, (
                f"Sidecar container '{sidecar_container.get('name')}' should not have service account token volume mount"
            )

    @pytest.mark.parametrize(
        ("executor", "container_name", "should_have_mount"),
        [
            # Scheduler container should have mount for pod-launching executors
            ("CeleryExecutor", "scheduler", True),
            ("KubernetesExecutor", "scheduler", True),
            ("LocalKubernetesExecutor", "scheduler", True),
            # Init containers should never have mount
            ("CeleryExecutor", "wait-for-airflow-migrations", False),
            ("KubernetesExecutor", "wait-for-airflow-migrations", False),
            ("LocalKubernetesExecutor", "wait-for-airflow-migrations", False),
            # Sidecar containers should never have mount
            ("CeleryExecutor", "scheduler-log-groomer", False),
            ("KubernetesExecutor", "scheduler-log-groomer", False),
            ("LocalKubernetesExecutor", "scheduler-log-groomer", False),
        ],
    )
    def test_service_account_token_volume_mount_per_container_security_policy(
        self, executor, container_name, should_have_mount
    ):
        """Test that service account token volume mount follows principle of least privilege per container."""
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                        "serviceAccountTokenVolume": {"enabled": True},
                    },
                    "logGroomerSidecar": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # Find the specific container
        all_containers = []
        containers = jmespath.search("spec.template.spec.containers", docs[0]) or []
        init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
        all_containers.extend(containers)
        all_containers.extend(init_containers)

        target_container = next((c for c in all_containers if c.get("name") == container_name), None)

        if target_container is None:
            # Container might not exist in this configuration, skip test
            pytest.skip(f"Container '{container_name}' not found in this configuration")

        volume_mounts = target_container.get("volumeMounts", [])
        sa_token_mount = next((vm for vm in volume_mounts if vm.get("name") == "kube-api-access"), None)

        if should_have_mount:
            assert sa_token_mount is not None, (
                f"Container '{container_name}' should have service account token volume mount"
            )
            assert sa_token_mount["mountPath"] == "/var/run/secrets/kubernetes.io/serviceaccount"
            assert sa_token_mount["readOnly"] is True
        else:
            assert sa_token_mount is None, (
                f"Container '{container_name}' should NOT have service account token volume mount for security"
            )

    def test_service_account_token_volume_mount_security_validation_all_executors(self):
        """Comprehensive test validating security policy across all executor types."""
        executors = [
            "LocalExecutor",
            "CeleryExecutor",
            "CeleryKubernetesExecutor",
            "KubernetesExecutor",
            "LocalKubernetesExecutor",
            "CeleryExecutor,KubernetesExecutor",
        ]

        for executor in executors:
            docs = render_chart(
                values={
                    "executor": executor,
                    "scheduler": {
                        "serviceAccount": {
                            "create": True,
                            "automountServiceAccountToken": False,
                            "serviceAccountTokenVolume": {"enabled": True},
                        },
                        "logGroomerSidecar": {"enabled": True},
                    },
                },
                show_only=["templates/scheduler/scheduler-deployment.yaml"],
            )

            is_pod_launching = executor in [
                "CeleryExecutor",
                "CeleryKubernetesExecutor",
                "KubernetesExecutor",
                "LocalKubernetesExecutor",
                "CeleryExecutor,KubernetesExecutor",
            ]

            if not is_pod_launching:
                # Non-pod-launching executors should not have service account token volume
                volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
                volume_names = [v.get("name") for v in volumes]
                assert "kube-api-access" not in volume_names, (
                    f"Non-pod-launching executor '{executor}' should not have service account token volume"
                )
                continue

            # Pod-launching executors should have volume but only mount it in scheduler container
            volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
            volume_names = [v.get("name") for v in volumes]
            assert "kube-api-access" in volume_names, (
                f"Pod-launching executor '{executor}' should have service account token volume"
            )

            # Check all containers for proper mount restrictions
            all_containers = []
            containers = jmespath.search("spec.template.spec.containers", docs[0]) or []
            init_containers = jmespath.search("spec.template.spec.initContainers", docs[0]) or []
            all_containers.extend([("container", c) for c in containers])
            all_containers.extend([("init_container", c) for c in init_containers])

            for container_type, container in all_containers:
                container_name = container.get("name")
                volume_mounts = container.get("volumeMounts", [])
                sa_token_mount = next(
                    (vm for vm in volume_mounts if vm.get("name") == "kube-api-access"), None
                )

                if container_name == "scheduler":
                    assert sa_token_mount is not None, (
                        f"Scheduler container should have service account token mount for executor '{executor}'"
                    )
                else:
                    assert sa_token_mount is None, (
                        f"Container '{container_name}' ({container_type}) should NOT have service account token mount for executor '{executor}' (security policy)"
                    )


class TestSchedulerHelperFunctions:
    """Tests for scheduler helper functions."""

    @pytest.mark.parametrize(
        ("executor", "is_pod_launching"),
        [
            ("LocalExecutor", False),
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
            ("CeleryExecutor,KubernetesExecutor", True),
        ],
    )
    def test_pod_launching_executor_helper_function(self, executor, is_pod_launching):
        """Test that the airflow.podLaunchingExecutor helper function works correctly."""
        # Test ServiceAccount template which uses the helper function
        # automountServiceAccountToken should only be set when explicitly disabled for pod-launching executors
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )

        # With default values, automountServiceAccountToken should not be set for any executor
        assert jmespath.search("automountServiceAccountToken", docs[0]) is None

        # Test Deployment template which also uses the helper function
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # With default values, automountServiceAccountToken should not be set for any executor
        assert jmespath.search("spec.template.spec.automountServiceAccountToken", docs[0]) is None


class TestSchedulerCreation:
    """Tests scheduler deployment creation."""

    def test_can_be_disabled(self):
        """
        Scheduler should be able to be disabled if the users desires.

        For example, user may be disabled when using scheduler and having it deployed on another host.
        """
        docs = render_chart(
            values={"scheduler": {"enabled": False}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert len(docs) == 0
