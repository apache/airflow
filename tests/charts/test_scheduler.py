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

import unittest

import jmespath
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class SchedulerTest(unittest.TestCase):
    @parameterized.expand(
        [
            ("CeleryExecutor", False, "Deployment"),
            ("CeleryExecutor", True, "Deployment"),
            ("CeleryKubernetesExecutor", True, "Deployment"),
            ("KubernetesExecutor", True, "Deployment"),
            ("LocalKubernetesExecutor", False, "Deployment"),
            ("LocalKubernetesExecutor", True, "StatefulSet"),
            ("LocalExecutor", True, "StatefulSet"),
            ("LocalExecutor", False, "Deployment"),
        ]
    )
    def test_scheduler_kind(self, executor, persistence, kind):
        """
        Test scheduler kind is StatefulSet only when using a local executor &
        worker persistence is enabled.
        """
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
        actual = jmespath.search("spec.template.spec.initContainers", docs[0])
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
                    "extraVolumes": [{"name": "test-volume", "emptyDir": {}}],
                    "extraVolumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
                },
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

        assert {'name': 'TEST_ENV_1', 'value': 'test_env_1'} in jmespath.search(
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

        assert {'name': 'TEST_ENV_1', 'value': 'test_env_1'} in jmespath.search(
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

    @parameterized.expand([(8, 10), (10, 8), (8, None), (None, 10), (None, None)])
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {"scheduler": {}}
        if revision_history_limit:
            values['scheduler']['revisionHistoryLimit'] = revision_history_limit
        if global_revision_history_limit:
            values['revisionHistoryLimit'] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        expected_result = revision_history_limit if revision_history_limit else global_revision_history_limit
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
        """When given both global and scheduler affinity etc, scheduler affinity etc is used"""
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

    @parameterized.expand(
        [
            ({"enabled": False}, {"emptyDir": {}}),
            ({"enabled": True}, {"persistentVolumeClaim": {"claimName": "release-name-logs"}}),
            (
                {"enabled": True, "existingClaim": "test-claim"},
                {"persistentVolumeClaim": {"claimName": "test-claim"}},
            ),
        ]
    )
    def test_logs_persistence_changes_volume(self, log_persistence_values, expected_volume):
        docs = render_chart(
            values={"logs": {"persistence": log_persistence_values}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "logs", **expected_volume} in jmespath.search("spec.template.spec.volumes", docs[0])

    def test_scheduler_resources_are_configurable(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "resources": {
                        "limits": {"cpu": "200m", 'memory': "128Mi"},
                        "requests": {"cpu": "300m", 'memory': "169Mi"},
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

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    @parameterized.expand(
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
        ]
    )
    def test_scheduler_update_strategy(
        self, executor, persistence, update_strategy, expected_update_strategy
    ):
        """updateStrategy should only be used when we have a local executor and workers.persistence"""
        docs = render_chart(
            values={
                "executor": executor,
                "workers": {"persistence": {"enabled": persistence}},
                "scheduler": {"updateStrategy": update_strategy},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert expected_update_strategy == jmespath.search("spec.updateStrategy", docs[0])

    @parameterized.expand(
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
        ]
    )
    def test_scheduler_strategy(self, executor, persistence, strategy, expected_strategy):
        """strategy should be used when we aren't using both a local executor and workers.persistence"""
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

    @parameterized.expand(
        [
            (None, None),
            (None, ["custom", "args"]),
            (["custom", "command"], None),
            (["custom", "command"], ["custom", "args"]),
        ]
    )
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

    def test_log_groomer_collector_can_be_disabled(self):
        docs = render_chart(
            values={"scheduler": {"logGroomerSidecar": {"enabled": False}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert 1 == len(jmespath.search("spec.template.spec.containers", docs[0]))

    def test_log_groomer_collector_default_command_and_args(self):
        docs = render_chart(show_only=["templates/scheduler/scheduler-deployment.yaml"])

        assert jmespath.search("spec.template.spec.containers[1].command", docs[0]) is None
        assert ["bash", "/clean-logs"] == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    def test_log_groomer_collector_default_retention_days(self):
        docs = render_chart(show_only=["templates/scheduler/scheduler-deployment.yaml"])

        assert "AIRFLOW__LOG_RETENTION_DAYS" == jmespath.search(
            "spec.template.spec.containers[1].env[0].name", docs[0]
        )
        assert "15" == jmespath.search("spec.template.spec.containers[1].env[0].value", docs[0])

    @parameterized.expand(
        [
            (None, None),
            (None, ["custom", "args"]),
            (["custom", "command"], None),
            (["custom", "command"], ["custom", "args"]),
        ]
    )
    def test_log_groomer_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"scheduler": {"logGroomerSidecar": {"command": command, "args": args}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[1].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    def test_log_groomer_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "logGroomerSidecar": {
                        "command": ["{{ .Release.Name }}"],
                        "args": ["{{ .Release.Service }}"],
                    }
                }
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert ["release-name"] == jmespath.search("spec.template.spec.containers[1].command", docs[0])
        assert ["Helm"] == jmespath.search("spec.template.spec.containers[1].args", docs[0])

    @parameterized.expand(
        [
            (None, None),
            (30, "30"),
        ]
    )
    def test_log_groomer_retention_days_overrides(self, retention_days, retention_result):
        docs = render_chart(
            values={"scheduler": {"logGroomerSidecar": {"retentionDays": retention_days}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        if retention_result:
            assert "AIRFLOW__LOG_RETENTION_DAYS" == jmespath.search(
                "spec.template.spec.containers[1].env[0].name", docs[0]
            )
            assert retention_result == jmespath.search(
                "spec.template.spec.containers[1].env[0].value", docs[0]
            )
        else:
            assert jmespath.search("spec.template.spec.containers[1].env", docs[0]) is None

    @parameterized.expand(
        [
            ({"gitSync": {"enabled": True}},),
            ({"gitSync": {"enabled": True}, "persistence": {"enabled": True}},),
        ]
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

    @parameterized.expand(
        [
            (True, "LocalExecutor", False),
            (True, "CeleryExecutor", True),
            (True, "KubernetesExecutor", True),
            (True, "LocalKubernetesExecutor", False),
            (False, "LocalExecutor", False),
            (False, "CeleryExecutor", False),
            (False, "KubernetesExecutor", False),
            (False, "LocalKubernetesExecutor", False),
        ]
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

    def test_log_groomer_resources(self):
        docs = render_chart(
            values={
                "scheduler": {
                    "logGroomerSidecar": {
                        "resources": {
                            "requests": {"memory": "2Gi", "cpu": "1"},
                            "limits": {"memory": "3Gi", "cpu": "2"},
                        }
                    }
                }
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {
            "limits": {
                "cpu": "2",
                "memory": "3Gi",
            },
            "requests": {
                "cpu": "1",
                "memory": "2Gi",
            },
        } == jmespath.search("spec.template.spec.containers[1].resources", docs[0])

    def test_persistence_volume_annotations(self):
        docs = render_chart(
            values={"executor": "LocalExecutor", "workers": {"persistence": {"annotations": {"foo": "bar"}}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert {"foo": "bar"} == jmespath.search("spec.volumeClaimTemplates[0].metadata.annotations", docs[0])

    @parameterized.expand(
        [
            "LocalExecutor",
            "LocalKubernetesExecutor",
            "CeleryExecutor",
            "KubernetesExecutor",
            "CeleryKubernetesExecutor",
        ]
    )
    def test_scheduler_deployment_has_executor_label(self, executor):
        docs = render_chart(
            values={"executor": executor},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert 1 == len(docs)
        assert executor == docs[0]['metadata']['labels'].get('executor')


class SchedulerNetworkPolicyTest(unittest.TestCase):
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


class SchedulerServiceTest(unittest.TestCase):
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


class SchedulerServiceAccountTest(unittest.TestCase):
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
