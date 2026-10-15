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

from configparser import ConfigParser
from pathlib import Path
from subprocess import CalledProcessError

import jmespath
import pytest
import yaml
from chart_utils.helm_template_generator import render_chart
from chart_utils.log_groomer import LogGroomerTestBase


class TestDagProcessor:
    """Tests DAG processor."""

    def test_enabled_by_airflow_version(self):
        """Tests that Dag Processor is enabled by default with Airflow 3"""
        docs = render_chart(
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert len(docs) == 1

    @pytest.mark.parametrize("enabled", [False, True])
    def test_enabled_explicit(self, enabled):
        """Tests that Dag Processor can be enabled/disabled regardless of version"""
        docs = render_chart(
            values={"dagProcessor": {"enabled": enabled}},
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
            values={"dagProcessor": {"waitForMigrations": {"enabled": False}}},
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
            values={"schedulerName": "airflow-scheduler"},
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

    def test_livenessprobe_command(self):
        docs = render_chart(show_only=["templates/dag-processor/dag-processor-deployment.yaml"])
        assert (
            "airflow jobs check --local --job-type DagProcessorJob"
            in jmespath.search("spec.template.spec.containers[0].livenessProbe.exec.command", docs[0])[-1]
        )

    def test_api_mode_uses_local_liveness_probe(self):
        docs = render_chart(
            values={"config": {"dag_processor": {"execution_api_token_file": "/run/processor.jwt"}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        command = jmespath.search("spec.template.spec.containers[0].livenessProbe.exec.command", docs[0])[-1]
        assert "airflow dag-processor --check-health" in command
        readiness = docs[0]["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["exec"]["command"]
        assert "airflow dag-processor --check-ready" in readiness[-1]
        assert "airflow jobs check" not in command

    def test_api_mode_replaces_pods_with_recreate(self):
        docs = render_chart(
            values={
                "config": {"dag_processor": {"execution_api_token_file": "/run/processor.jwt"}},
                "dagProcessor": {
                    "strategy": {"rollingUpdate": {"maxSurge": "100%", "maxUnavailable": "50%"}}
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.strategy", docs[0]) == {"type": "Recreate"}

    @pytest.mark.parametrize(("replicas", "rejected"), [(0, False), (1, False), (2, True)])
    def test_api_mode_rejects_replicas_sharing_a_session_token(self, replicas, rejected):
        values = {
            "config": {"dag_processor": {"execution_api_token_file": "/run/processor.jwt"}},
            "dagProcessor": {"replicas": replicas},
        }
        show_only = ["templates/dag-processor/dag-processor-deployment.yaml"]
        if rejected:
            with pytest.raises(CalledProcessError, match="must not exceed 1"):
                render_chart(values=values, show_only=show_only)
        else:
            docs = render_chart(values=values, show_only=show_only)
            assert jmespath.search("spec.replicas", docs[0]) == replicas

    def test_isolated_api_deployment_example(self):
        examples = Path(__file__).resolve().parents[4] / "airflow-core/docs/security/dag-processor-api"
        values = yaml.safe_load((examples / "helm-values.yaml").read_text())
        docs = render_chart(values=values, namespace="dag-processors")
        workloads = [doc for doc in docs if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}]
        assert len(workloads) == 1
        deployment = workloads[0]
        assert deployment["metadata"]["labels"]["component"] == "dag-processor"
        assert deployment["spec"]["strategy"] == {"type": "Recreate"}
        pod = deployment["spec"]["template"]["spec"]
        assert not pod.get("initContainers")
        for container in pod["containers"]:
            assert not container.get("envFrom")
            assert all("valueFrom" not in env for env in container.get("env", []))
        processor = next(container for container in pod["containers"] if container["name"] == "dag-processor")
        assert {env["name"]: env["value"] for env in processor["env"]} == {
            "AIRFLOW_HOME": "/opt/airflow",
            "CONNECTION_CHECK_MAX_COUNT": "0",
        }
        assert "--check-health" in processor["livenessProbe"]["exec"]["command"][-1]
        assert "--check-ready" in processor["readinessProbe"]["exec"]["command"][-1]
        assert {v["secret"]["secretName"] for v in pod["volumes"] if "secret" in v} == {
            "dag-processor-session"
        }
        token_mount = next(m for m in processor["volumeMounts"] if m["name"] == "processor-token")
        assert token_mount["readOnly"]
        assert "subPath" not in token_mount
        account = next(
            doc
            for doc in docs
            if doc["kind"] == "ServiceAccount" and doc["metadata"]["name"] == pod["serviceAccountName"]
        )
        assert account["automountServiceAccountToken"] is False
        configmap = next(
            doc for doc in docs if doc["kind"] == "ConfigMap" and "airflow.cfg" in doc.get("data", {})
        )
        config = ConfigParser(interpolation=None)
        config.read_string(configmap["data"]["airflow.cfg"])
        for section, key in (
            ("database", "sql_alchemy_conn"),
            ("core", "fernet_key"),
            ("api_auth", "jwt_secret"),
            ("api_auth", "jwt_private_key_path"),
        ):
            assert not config.has_option(section, key)
        assert config["dag_processor"]["execution_api_token_file"] == "/run/dag-processor/processor.jwt"
        policy = yaml.safe_load((examples / "network-policy.yaml").read_text())["spec"]
        labels = deployment["spec"]["template"]["metadata"]["labels"]
        assert policy["podSelector"]["matchLabels"].items() <= labels.items()
        assert set(policy["policyTypes"]) == {"Ingress", "Egress"}
        assert not policy["ingress"]
        assert len(policy["egress"]) == 2
        assert policy["egress"][0] == {
            "to": [
                {
                    "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "airflow"}},
                    "podSelector": {"matchLabels": {"component": "api-server", "release": "airflow"}},
                }
            ],
            "ports": [{"protocol": "TCP", "port": 8080}],
        }
        assert policy["egress"][1]["to"][0]["podSelector"]["matchLabels"] == {"k8s-app": "kube-dns"}
        assert {port["port"] for port in policy["egress"][1]["ports"]} == {53}

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
        docs = render_chart(show_only=["templates/dag-processor/dag-processor-deployment.yaml"])
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
            values={"dagProcessor": {"strategy": strategy}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert expected_strategy == jmespath.search("spec.strategy", docs[0])

    def test_default_command_and_args(self):
        docs = render_chart(show_only=["templates/dag-processor/dag-processor-deployment.yaml"])

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
        values = {"dagProcessor": {}}
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

    @pytest.mark.parametrize(
        ("revision_history_limit", "global_revision_history_limit", "expected"),
        [(0, None, 0), (None, 0, 0), (0, 10, 0)],
    )
    def test_revision_history_limit_zero(
        self, revision_history_limit, global_revision_history_limit, expected
    ):
        """Test that revisionHistoryLimit can be set to 0."""
        values = {"dagProcessor": {}}
        if revision_history_limit is not None:
            values["dagProcessor"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit is not None:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"dagProcessor": {"command": command, "args": args}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={"dagProcessor": {"command": ["{{ .Release.Name }}"], "args": ["{{ .Release.Service }}"]}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

    def test_dags_volume_mount_with_persistence_true(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert "dags" in [
            vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        ]
        assert "dags" in [vm["name"] for vm in jmespath.search("spec.template.spec.volumes", docs[0])]

    def test_dags_gitsync_sidecar_and_init_container(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])]
        assert "git-sync-init" in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    def test_dags_gitsync_with_persistence(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": True}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )

        # The git-sync container should be present when persistence is enabled
        # (it's the part that runs the sync for everything else), but the
        # git-sync-init container should not be present
        assert "git-sync" in [c["name"] for c in jmespath.search("spec.template.spec.containers", docs[0])]
        assert "git-sync-init" not in [
            c["name"] for c in jmespath.search("spec.template.spec.initContainers", docs[0])
        ]

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": None},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        volume_mounts = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)
        volume_mounts_init = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts_init)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
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
            values={"dagProcessor": {"annotations": {"test_annotation": "test_annotation_value"}}},
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"

    def test_dag_processor_pod_hostaliases(self):
        docs = render_chart(
            values={
                "dagProcessor": {
                    "hostAliases": [{"ip": "127.0.0.1", "hostnames": ["foo.local"]}],
                },
            },
            show_only=["templates/dag-processor/dag-processor-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.hostAliases[0].ip", docs[0]) == "127.0.0.1"
        assert jmespath.search("spec.template.spec.hostAliases[0].hostnames[0]", docs[0]) == "foo.local"

    def test_validate_if_ssh_params_are_added_with_git_ssh_key(self):
        docs = render_chart(
            values={
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


class TestDagProcessorLogGroomer(LogGroomerTestBase):
    """DAG processor log groomer."""

    obj_name = "dag-processor"
    folder = "dag-processor"


class TestDagProcessorServiceAccount:
    """Tests DAG processor service account."""

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={"dagProcessor": {"serviceAccount": {"create": True}}},
            show_only=["templates/dag-processor/dag-processor-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "dagProcessor": {"serviceAccount": {"create": True, "automountServiceAccountToken": False}}
            },
            show_only=["templates/dag-processor/dag-processor-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
