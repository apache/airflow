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
from tests.charts.log_groomer import LogGroomerTestBase


class TestScheduler:
    """Tests scheduler."""

    @pytest.mark.parametrize(
        "executor, persistence, kind",
        [
            ("CeleryExecutor", False, "Deployment"),
            ("CeleryExecutor", True, "Deployment"),
            ("CeleryKubernetesExecutor", True, "Deployment"),
            ("KubernetesExecutor", True, "Deployment"),
            ("LocalKubernetesExecutor", False, "Deployment"),
            ("LocalKubernetesExecutor", True, "StatefulSet"),
            ("LocalExecutor", True, "StatefulSet"),
            ("LocalExecutor", False, "Deployment"),
        ],
    )
    def test_scheduler_kind(self, executor, persistence, kind):
        """Test scheduler kind is StatefulSet only with a local executor & worker persistence is enabled."""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"persistence": {"enabled": persistence}},
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
                        {"name": "test-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {
            "name": "test-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

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

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

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
        assert "test-volume-airflow" == jmespath.search(
            "spec.template.spec.initContainers[0].volumeMounts[-1].name", docs[0]
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
                    "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )

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
        "revision_history_limit, global_revision_history_limit",
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "airflow-scheduler" == jmespath.search(
            "spec.template.spec.schedulerName",
            docs[0],
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

        assert 111 == jmespath.search("spec.template.spec.containers[0].startupProbe.timeoutSeconds", docs[0])
        assert 222 == jmespath.search(
            "spec.template.spec.containers[0].startupProbe.failureThreshold", docs[0]
        )
        assert 333 == jmespath.search("spec.template.spec.containers[0].startupProbe.periodSeconds", docs[0])
        assert ["sh", "-c", "echo", "wow such test"] == jmespath.search(
            "spec.template.spec.containers[0].startupProbe.exec.command", docs[0]
        )

    @pytest.mark.parametrize(
        "airflow_version, probe_command",
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
        "airflow_version, probe_command",
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
        "log_values, expected_volume",
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
        assert {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True} == jmespath.search(
            "spec.template.spec.containers[0].securityContext", docs[0]
        )

        assert {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        } == jmespath.search("spec.template.spec.securityContext", docs[0])

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

        assert {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        } == jmespath.search("spec.template.spec.securityContext", docs[0])

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
        "executor, persistence, update_strategy, expected_update_strategy",
        [
            ("CeleryExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            ("CeleryExecutor", True, {"rollingUpdate": {"partition": 0}}, None),
            ("LocalKubernetesExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            (
                "LocalKubernetesExecutor",
                True,
                {"rollingUpdate": {"partition": 0}},
                {"rollingUpdate": {"partition": 0}},
            ),
            ("LocalExecutor", False, {"rollingUpdate": {"partition": 0}}, None),
            ("LocalExecutor", True, {"rollingUpdate": {"partition": 0}}, {"rollingUpdate": {"partition": 0}}),
            ("LocalExecutor", True, None, None),
        ],
    )
    def test_scheduler_update_strategy(
        self, executor, persistence, update_strategy, expected_update_strategy
    ):
        """UpdateStrategy should only be used when we have a local executor and workers.persistence."""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"persistence": {"enabled": persistence}},
                "scheduler": {"updateStrategy": update_strategy},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert expected_update_strategy == jmespath.search("spec.updateStrategy", docs[0])

    @pytest.mark.parametrize(
        "executor, persistence, strategy, expected_strategy",
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
                "workers": {"persistence": {"enabled": persistence}},
                "scheduler": {"strategy": strategy},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert expected_strategy == jmespath.search("spec.strategy", docs[0])

    def test_default_command_and_args(self):
        docs = render_chart(show_only=["templates/scheduler/scheduler-deployment.yaml"])

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert ["bash", "-c", "exec airflow scheduler"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

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

        assert ["release-name"] == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert ["Helm"] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    @pytest.mark.parametrize(
        "dags_values",
        [
            {"gitSync": {"enabled": True}},
            {"gitSync": {"enabled": True}, "persistence": {"enabled": True}},
        ],
    )
    def test_dags_gitsync_sidecar_and_init_container(self, dags_values):
        docs = render_chart(
            values={"dags": dags_values},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])]
        assert "git-sync-init" in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    @pytest.mark.parametrize(
        "dag_processor, executor, skip_dags_mount",
        [
            (True, "LocalExecutor", False),
            (True, "CeleryExecutor", True),
            (True, "KubernetesExecutor", True),
            (True, "LocalKubernetesExecutor", False),
            (False, "LocalExecutor", False),
            (False, "CeleryExecutor", False),
            (False, "KubernetesExecutor", False),
            (False, "LocalKubernetesExecutor", False),
        ],
    )
    def test_dags_mount_and_gitsync_expected_with_dag_processor(
        self, dag_processor, executor, skip_dags_mount
    ):
        """
        DAG Processor can move gitsync and DAGs mount from the scheduler to the DAG Processor only.

        The only exception is when we have a Local executor.
        In these cases, the scheduler does the worker role and needs access to DAGs anyway.
        """
        docs = render_chart(
            values={
                "dagProcessor": {"enabled": dag_processor},
                "executor": executor,
                "dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}},
                "scheduler": {"logGroomerSidecar": {"enabled": False}},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        if skip_dags_mount:
            assert "dags" not in [
                vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
            ]
            assert "dags" not in [vm["name"] for vm in jmespath.search("spec.template.spec.volumes", docs[0])]
            assert 1 == len(jmespath.search("spec.template.spec.containers", docs[0]))
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

    def test_persistence_volume_annotations(self):
        docs = render_chart(
            values={"executor": "LocalExecutor", "workers": {"persistence": {"annotations": {"foo": "bar"}}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert {"foo": "bar"} == jmespath.search("spec.volumeClaimTemplates[0].metadata.annotations", docs[0])

    @pytest.mark.parametrize(
        "executor",
        [
            "LocalExecutor",
            "LocalKubernetesExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "CeleryKubernetesExecutor",
        ],
    )
    def test_scheduler_deployment_has_executor_label(self, executor):
        docs = render_chart(
            values={"executor": executor},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert 1 == len(docs)
        assert executor == docs[0]["metadata"]["labels"].get("executor")

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

        assert "127.0.0.1" == jmespath.search("spec.template.spec.hostAliases[0].ip", docs[0])
        assert "foo.local" == jmespath.search("spec.template.spec.hostAliases[0].hostnames[0]", docs[0])

    def test_scheduler_template_storage_class_name(self):
        docs = render_chart(
            values={
                "workers": {
                    "persistence": {
                        "storageClassName": "{{ .Release.Name }}-storage-class",
                        "enabled": True,
                    }
                },
                "logs": {"persistence": {"enabled": False}},
                "executor": "LocalExecutor",
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert "release-name-storage-class" == jmespath.search(
            "spec.volumeClaimTemplates[0].spec.storageClassName", docs[0]
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
        "executor, creates_service",
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

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "serviceAccount": {"create": True, "automountServiceAccountToken": False},
                },
            },
            show_only=["templates/scheduler/scheduler-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
