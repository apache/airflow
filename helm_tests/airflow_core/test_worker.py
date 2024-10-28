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


class TestWorker:
    """Tests worker."""

    @pytest.mark.parametrize(
        "executor, persistence, kind",
        [
            ("CeleryExecutor", False, "Deployment"),
            ("CeleryExecutor", True, "StatefulSet"),
            ("CeleryKubernetesExecutor", False, "Deployment"),
            ("CeleryKubernetesExecutor", True, "StatefulSet"),
        ],
    )
    def test_worker_kind(self, executor, persistence, kind):
        """Test worker kind is StatefulSet when worker persistence is enabled."""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"persistence": {"enabled": persistence}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert kind == jmespath.search("kind", docs[0])

    @pytest.mark.parametrize(
        "revision_history_limit, global_revision_history_limit",
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(
        self, revision_history_limit, global_revision_history_limit
    ):
        values = {"workers": {}}
        if revision_history_limit:
            values["workers"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        expected_result = revision_history_limit or global_revision_history_limit
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "extraContainers": [
                        {
                            "name": "{{ .Chart.Name }}",
                            "image": "test-registry/test-repo:test-tag",
                        }
                    ],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

    def test_persistent_volume_claim_retention_policy(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "persistence": {
                        "enabled": True,
                        "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Delete"},
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {
            "whenDeleted": "Delete",
        } == jmespath.search("spec.persistentVolumeClaimRetentionPolicy", docs[0])

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "release-name-test-container"} == jmespath.search(
            "spec.template.spec.containers[-1]", docs[0]
        )

    def test_disable_wait_for_migration(self):
        docs = render_chart(
            values={
                "workers": {
                    "waitForMigrations": {"enabled": False},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        actual = jmespath.search(
            "spec.template.spec.initContainers[?name=='wait-for-airflow-migrations']",
            docs[0],
        )
        assert actual is None

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraInitContainers": [
                        {
                            "name": "test-init-container",
                            "image": "test-registry/test-repo:test-tag",
                        }
                    ],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.initContainers[-1]", docs[0])

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraInitContainers": [
                        {"name": "{{ .Release.Name }}-test-init-container"}
                    ],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "release-name-test-init-container"} == jmespath.search(
            "spec.template.spec.initContainers[-1]", docs[0]
        )

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
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
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test-volume-airflow" == jmespath.search(
            "spec.template.spec.volumes[0].name", docs[0]
        )
        assert "test-volume-airflow" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[0].name", docs[0]
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
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test-volume" == jmespath.search(
            "spec.template.spec.volumes[0].name", docs[0]
        )
        assert "test-volume" == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[0].name", docs[0]
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "workers": {
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
            show_only=["templates/workers/worker-deployment.yaml"],
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
                "workers": {
                    "waitForMigrations": {
                        "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}]
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.initContainers[0].env", docs[0]
        )

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_workers_host_aliases(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "hostAliases": [{"ip": "127.0.0.2", "hostnames": ["test.hostname"]}],
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "127.0.0.2" == jmespath.search(
            "spec.template.spec.hostAliases[0].ip", docs[0]
        )
        assert "test.hostname" == jmespath.search(
            "spec.template.spec.hostAliases[0].hostnames[0]", docs[0]
        )

    @pytest.mark.parametrize(
        "persistence, update_strategy, expected_update_strategy",
        [
            (False, None, None),
            (
                True,
                {"rollingUpdate": {"partition": 0}},
                {"rollingUpdate": {"partition": 0}},
            ),
            (True, None, None),
        ],
    )
    def test_workers_update_strategy(
        self, persistence, update_strategy, expected_update_strategy
    ):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "persistence": {"enabled": persistence},
                    "updateStrategy": update_strategy,
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert expected_update_strategy == jmespath.search("spec.updateStrategy", docs[0])

    @pytest.mark.parametrize(
        "persistence, strategy, expected_strategy",
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
    def test_workers_strategy(self, persistence, strategy, expected_strategy):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "persistence": {"enabled": persistence},
                    "strategy": strategy,
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert expected_strategy == jmespath.search("spec.strategy", docs[0])

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
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
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "StatefulSet" == jmespath.search("kind", docs[0])
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

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(
        self,
    ):
        """When given both global and worker affinity etc, worker affinity etc is used."""
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
                "workers": {
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
            show_only=["templates/workers/worker-deployment.yaml"],
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
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "airflow-scheduler" == jmespath.search(
            "spec.template.spec.schedulerName",
            docs[0],
        )

    def test_should_create_default_affinity(self):
        docs = render_chart(show_only=["templates/workers/worker-deployment.yaml"])

        assert {"component": "worker"} == jmespath.search(
            "spec.template.spec.affinity.podAntiAffinity."
            "preferredDuringSchedulingIgnoredDuringExecution[0]."
            "podAffinityTerm.labelSelector.matchLabels",
            docs[0],
        )

    def test_runtime_class_name_values_are_configurable(self):
        docs = render_chart(
            values={
                "workers": {"runtimeClassName": "nvidia"},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.runtimeClassName", docs[0]) == "nvidia"

    @pytest.mark.parametrize(
        "airflow_version, default_cmd",
        [
            ("2.7.0", "airflow.providers.celery.executors.celery_executor.app"),
            ("2.6.3", "airflow.executors.celery_executor.app"),
        ],
    )
    def test_livenessprobe_default_command(self, airflow_version, default_cmd):
        docs = render_chart(
            values={"airflowVersion": airflow_version},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        livenessprobe_cmd = jmespath.search(
            "spec.template.spec.containers[0].livenessProbe.exec.command", docs[0]
        )
        assert default_cmd in livenessprobe_cmd[-1]

    def test_livenessprobe_values_are_configurable(self):
        docs = render_chart(
            values={
                "workers": {
                    "livenessProbe": {
                        "initialDelaySeconds": 111,
                        "timeoutSeconds": 222,
                        "failureThreshold": 333,
                        "periodSeconds": 444,
                        "command": ["sh", "-c", "echo", "wow such test"],
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        livenessprobe = jmespath.search(
            "spec.template.spec.containers[0].livenessProbe", docs[0]
        )
        assert livenessprobe == {
            "initialDelaySeconds": 111,
            "timeoutSeconds": 222,
            "failureThreshold": 333,
            "periodSeconds": 444,
            "exec": {
                "command": ["sh", "-c", "echo", "wow such test"],
            },
        }

    def test_disable_livenessprobe(self):
        docs = render_chart(
            values={
                "workers": {"livenessProbe": {"enabled": False}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        livenessprobe = jmespath.search(
            "spec.template.spec.containers[0].livenessProbe", docs[0]
        )
        assert livenessprobe is None

    def test_extra_init_container_restart_policy_is_configurable(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraInitContainers": [
                        {
                            "name": "test-init-container",
                            "image": "test-registry/test-repo:test-tag",
                            "restartPolicy": "Always",
                        }
                    ]
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "Always" == jmespath.search(
            "spec.template.spec.initContainers[1].restartPolicy", docs[0]
        )

    @pytest.mark.parametrize(
        "log_values, expected_volume",
        [
            ({"persistence": {"enabled": False}}, {"emptyDir": {}}),
            (
                {
                    "persistence": {"enabled": False},
                    "emptyDirConfig": {"sizeLimit": "10Gi"},
                },
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
                "executor": "CeleryExecutor",
                "workers": {"persistence": {"enabled": False}},
                "logs": log_values,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "logs", **expected_volume} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

    def test_worker_resources_are_configurable(self):
        docs = render_chart(
            values={
                "workers": {
                    "resources": {
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        # main container
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

        # initContainer wait-for-airflow-configurations
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

    def test_worker_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}
        )

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": None},
            show_only=["templates/workers/worker-deployment.yaml"],
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
            show_only=["templates/workers/worker-deployment.yaml"],
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

    def test_airflow_local_settings_kerberos_sidecar(self):
        docs = render_chart(
            values={
                "airflowLocalSettings": "# Well hello!",
                "workers": {"kerberosSidecar": {"enabled": True}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.template.spec.containers[2].volumeMounts", docs[0])

    @pytest.mark.parametrize(
        "airflow_version, init_container_enabled, expected_init_containers",
        [
            ("1.9.0", True, 2),
            ("1.9.0", False, 2),
            ("1.10.14", True, 2),
            ("1.10.14", False, 2),
            ("2.0.2", True, 2),
            ("2.0.2", False, 2),
            ("2.1.0", True, 2),
            ("2.1.0", False, 2),
            ("2.8.0", True, 3),
            ("2.8.0", False, 2),
        ],
    )
    def test_airflow_kerberos_init_container(
        self, airflow_version, init_container_enabled, expected_init_containers
    ):
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
                "workers": {
                    "kerberosInitContainer": {"enabled": init_container_enabled},
                    "persistence": {"fixPermissions": True},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        initContainers = jmespath.search("spec.template.spec.initContainers", docs[0])
        assert len(initContainers) == expected_init_containers

        if expected_init_containers == 3:
            assert initContainers[1]["name"] == "kerberos-init"
            assert initContainers[1]["args"] == ["kerberos", "-o"]

    @pytest.mark.parametrize(
        "airflow_version, expected_arg",
        [
            ("1.9.0", "airflow worker"),
            ("1.10.14", "airflow worker"),
            ("2.0.2", "airflow celery worker"),
            ("2.1.0", "airflow celery worker"),
        ],
    )
    def test_default_command_and_args_airflow_version(
        self, airflow_version, expected_arg
    ):
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert (
            jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        )
        assert [
            "bash",
            "-c",
            f"exec \\\n{expected_arg}",
        ] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"workers": {"command": command, "args": args}},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert command == jmespath.search(
            "spec.template.spec.containers[0].command", docs[0]
        )
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "workers": {
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert ["release-name"] == jmespath.search(
            "spec.template.spec.containers[0].command", docs[0]
        )
        assert ["Helm"] == jmespath.search(
            "spec.template.spec.containers[0].args", docs[0]
        )

    def test_dags_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "git-sync" in [
            c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])
        ]
        assert "git-sync-init" in [
            c["name"]
            for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    def test_dags_gitsync_with_persistence_no_sidecar_or_init_container(self):
        docs = render_chart(
            values={
                "dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}}
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        # No gitsync sidecar or init container
        assert "git-sync" not in [
            c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])
        ]
        assert "git-sync-init" not in [
            c["name"]
            for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    def test_persistence_volume_annotations(self):
        docs = render_chart(
            values={"workers": {"persistence": {"annotations": {"foo": "bar"}}}},
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert {"foo": "bar"} == jmespath.search(
            "spec.volumeClaimTemplates[0].metadata.annotations", docs[0]
        )

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "workers": {
                    "annotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert (
            jmespath.search("metadata.annotations", docs[0])["test_annotation"]
            == "test_annotation_value"
        )

    @pytest.mark.parametrize(
        "globalScope, localScope, precedence",
        [
            ({}, {}, "false"),
            ({}, {"safeToEvict": True}, "true"),
            ({}, {"safeToEvict": False}, "false"),
            (
                {},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
                    },
                    "safeToEvict": True,
                },
                "true",
            ),
            (
                {},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
                    },
                    "safeToEvict": False,
                },
                "true",
            ),
            (
                {},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                    "safeToEvict": True,
                },
                "false",
            ),
            (
                {},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                    "safeToEvict": False,
                },
                "false",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
                {"safeToEvict": True},
                "true",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
                {"safeToEvict": False},
                "false",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                {"safeToEvict": True},
                "true",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                {"safeToEvict": False},
                "false",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
                    },
                    "safeToEvict": False,
                },
                "true",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                    "safeToEvict": False,
                },
                "false",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
                    },
                    "safeToEvict": False,
                },
                "true",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                    "safeToEvict": False,
                },
                "false",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
                    },
                    "safeToEvict": True,
                },
                "true",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                    "safeToEvict": True,
                },
                "false",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
                    },
                    "safeToEvict": True,
                },
                "true",
            ),
            (
                {"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"},
                {
                    "podAnnotations": {
                        "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
                    },
                    "safeToEvict": True,
                },
                "false",
            ),
        ],
    )
    def test_safetoevict_annotations(self, globalScope, localScope, precedence):
        docs = render_chart(
            values={"airflowPodAnnotations": globalScope, "workers": localScope},
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert (
            jmespath.search("spec.template.metadata.annotations", docs[0])[
                "cluster-autoscaler.kubernetes.io/safe-to-evict"
            ]
            == precedence
        )

    def test_should_add_extra_volume_claim_templates(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "volumeClaimTemplates": [
                        {
                            "metadata": {"name": "test-volume-airflow-1"},
                            "spec": {
                                "storageClassName": "storage-class-1",
                                "accessModes": ["ReadWriteOnce"],
                                "resources": {"requests": {"storage": "10Gi"}},
                            },
                        },
                        {
                            "metadata": {"name": "test-volume-airflow-2"},
                            "spec": {
                                "storageClassName": "storage-class-2",
                                "accessModes": ["ReadWriteOnce"],
                                "resources": {"requests": {"storage": "20Gi"}},
                            },
                        },
                    ]
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "test-volume-airflow-1" == jmespath.search(
            "spec.volumeClaimTemplates[1].metadata.name", docs[0]
        )
        assert "test-volume-airflow-2" == jmespath.search(
            "spec.volumeClaimTemplates[2].metadata.name", docs[0]
        )
        assert "storage-class-1" == jmespath.search(
            "spec.volumeClaimTemplates[1].spec.storageClassName", docs[0]
        )
        assert "storage-class-2" == jmespath.search(
            "spec.volumeClaimTemplates[2].spec.storageClassName", docs[0]
        )
        assert ["ReadWriteOnce"] == jmespath.search(
            "spec.volumeClaimTemplates[1].spec.accessModes", docs[0]
        )
        assert ["ReadWriteOnce"] == jmespath.search(
            "spec.volumeClaimTemplates[2].spec.accessModes", docs[0]
        )
        assert "10Gi" == jmespath.search(
            "spec.volumeClaimTemplates[1].spec.resources.requests.storage", docs[0]
        )
        assert "20Gi" == jmespath.search(
            "spec.volumeClaimTemplates[2].spec.resources.requests.storage", docs[0]
        )

    @pytest.mark.parametrize(
        "globalScope, localScope, precedence",
        [
            ({"scope": "global"}, {"podAnnotations": {}}, "global"),
            ({}, {"podAnnotations": {"scope": "local"}}, "local"),
            ({"scope": "global"}, {"podAnnotations": {"scope": "local"}}, "local"),
            ({}, {}, None),
        ],
    )
    def test_podannotations_precedence(self, globalScope, localScope, precedence):
        docs = render_chart(
            values={"airflowPodAnnotations": globalScope, "workers": localScope},
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        if precedence:
            assert (
                jmespath.search("spec.template.metadata.annotations", docs[0])["scope"]
                == precedence
            )
        else:
            assert (
                jmespath.search("spec.template.metadata.annotations.scope", docs[0])
                is None
            )

    def test_worker_template_storage_class_name(self):
        docs = render_chart(
            values={
                "workers": {
                    "persistence": {
                        "storageClassName": "{{ .Release.Name }}-storage-class"
                    }
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert "release-name-storage-class" == jmespath.search(
            "spec.volumeClaimTemplates[0].spec.storageClassName", docs[0]
        )


class TestWorkerLogGroomer(LogGroomerTestBase):
    """Worker groomer."""

    obj_name = "worker"
    folder = "workers"


class TestWorkerKedaAutoScaler:
    """Tests worker keda auto scaler."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "keda": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_should_remove_replicas_field(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "keda": {"enabled": True},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "replicas" not in jmespath.search("spec", docs[0])

    @pytest.mark.parametrize(
        "query, executor, expected_query",
        [
            # default query with CeleryExecutor
            (
                None,
                "CeleryExecutor",
                "SELECT ceil(COUNT(*)::decimal / 16) FROM task_instance"
                " WHERE (state='running' OR state='queued')",
            ),
            # default query with CeleryKubernetesExecutor
            (
                None,
                "CeleryKubernetesExecutor",
                "SELECT ceil(COUNT(*)::decimal / 16) FROM task_instance"
                " WHERE (state='running' OR state='queued') AND queue != 'kubernetes'",
            ),
            # test custom static query
            (
                "SELECT ceil(COUNT(*)::decimal / 16) FROM task_instance",
                "CeleryKubernetesExecutor",
                "SELECT ceil(COUNT(*)::decimal / 16) FROM task_instance",
            ),
            # test custom template query
            (
                "SELECT ceil(COUNT(*)::decimal / {{ mul .Values.config.celery.worker_concurrency 2 }})"
                " FROM task_instance",
                "CeleryKubernetesExecutor",
                "SELECT ceil(COUNT(*)::decimal / 32) FROM task_instance",
            ),
        ],
    )
    def test_should_use_keda_query(self, query, executor, expected_query):
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {
                    "keda": {"enabled": True, **({"query": query} if query else {})},
                },
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert expected_query == jmespath.search(
            "spec.triggers[0].metadata.query", docs[0]
        )

    def test_mysql_db_backend_keda_worker(self):
        docs = render_chart(
            values={
                "data": {"metadataConnection": {"protocol": "mysql"}},
                "workers": {
                    "keda": {"enabled": True},
                },
            },
            show_only=["templates/workers/worker-kedaautoscaler.yaml"],
        )
        assert "1" == jmespath.search("spec.triggers[0].metadata.queryValue", docs[0])
        assert (
            jmespath.search("spec.triggers[0].metadata.targetQueryValue", docs[0]) is None
        )

        assert "KEDA_DB_CONN" == jmespath.search(
            "spec.triggers[0].metadata.connectionStringFromEnv", docs[0]
        )
        assert (
            jmespath.search("spec.triggers[0].metadata.connectionFromEnv", docs[0])
            is None
        )


class TestWorkerHPAAutoScaler:
    """Tests worker HPA auto scaler."""

    def test_should_be_disabled_on_keda_enabled(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "keda": {"enabled": True},
                    "hpa": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=[
                "templates/workers/worker-kedaautoscaler.yaml",
                "templates/workers/worker-hpa.yaml",
            ],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )
        assert len(docs) == 1

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "hpa": {"enabled": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_should_remove_replicas_field(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "hpa": {"enabled": True},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert "replicas" not in jmespath.search("spec", docs[0])

    @pytest.mark.parametrize(
        "metrics, executor, expected_metrics",
        [
            # default metrics
            (
                None,
                "CeleryExecutor",
                {
                    "type": "Resource",
                    "resource": {
                        "name": "cpu",
                        "target": {"type": "Utilization", "averageUtilization": 80},
                    },
                },
            ),
            # custom metric
            (
                [
                    {
                        "type": "Pods",
                        "pods": {
                            "metric": {"name": "custom"},
                            "target": {"type": "Utilization", "averageUtilization": 80},
                        },
                    }
                ],
                "CeleryKubernetesExecutor",
                {
                    "type": "Pods",
                    "pods": {
                        "metric": {"name": "custom"},
                        "target": {"type": "Utilization", "averageUtilization": 80},
                    },
                },
            ),
        ],
    )
    def test_should_use_hpa_metrics(self, metrics, executor, expected_metrics):
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {
                    "hpa": {"enabled": True, **({"metrics": metrics} if metrics else {})},
                },
            },
            show_only=["templates/workers/worker-hpa.yaml"],
        )
        assert expected_metrics == jmespath.search("spec.metrics[0]", docs[0])


class TestWorkerNetworkPolicy:
    """Tests worker network policy."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "networkPolicies": {"enabled": True},
                "executor": "CeleryExecutor",
                "workers": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-networkpolicy.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )


class TestWorkerService:
    """Tests worker service."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-service.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )


class TestWorkerServiceAccount:
    """Tests worker service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "workers": {
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-service.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    @pytest.mark.parametrize(
        "executor, creates_service_account",
        [
            ("LocalExecutor", False),
            ("CeleryExecutor", True),
            ("CeleryKubernetesExecutor", True),
            ("KubernetesExecutor", True),
            ("LocalKubernetesExecutor", True),
        ],
    )
    def test_should_create_worker_service_account_for_specific_executors(
        self, executor, creates_service_account
    ):
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {
                    "serviceAccount": {"create": True},
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )
        if creates_service_account:
            assert jmespath.search("kind", docs[0]) == "ServiceAccount"
            assert "test_label" in jmespath.search("metadata.labels", docs[0])
            assert (
                jmespath.search("metadata.labels", docs[0])["test_label"]
                == "test_label_value"
            )
        else:
            assert docs == []

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "workers": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "workers": {
                    "serviceAccount": {
                        "create": True,
                        "automountServiceAccountToken": False,
                    },
                },
            },
            show_only=["templates/workers/worker-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
