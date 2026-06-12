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

import re
from pathlib import Path
from shutil import copyfile, copytree

import jmespath
import pytest
from chart_utils.helm_template_generator import render_chart


@pytest.fixture(scope="class")
def isolate_chart(request, tmp_path_factory) -> Path:
    chart_dir = Path(__file__).parents[4] / "chart"
    tmp_dir = tmp_path_factory.mktemp(request.cls.__name__)
    temp_chart_dir = tmp_dir / "chart"

    copytree(chart_dir, temp_chart_dir)
    copyfile(
        temp_chart_dir / "files/pod-template-file.kubernetes-helm-yaml",
        temp_chart_dir / "templates/pod-template-file.yaml",
    )
    return temp_chart_dir


class TestPodTemplateFile:
    """Tests pod template file."""

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, isolate_chart):
        self.temp_chart_dir = isolate_chart.as_posix()

    def test_should_work(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert jmespath.search("spec.containers[0].image", docs[0]) is not None
        assert jmespath.search("spec.containers[0].name", docs[0]) == "base"

    def test_should_add_an_init_container_if_git_sync_is_true(self):
        docs = render_chart(
            values={
                "images": {
                    "gitSync": {
                        "repository": "test-registry/test-repo",
                        "tag": "test-tag",
                        "pullPolicy": "Always",
                    }
                },
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "wait": None,
                        "period": "66s",
                        "maxFailures": 70,
                        "subPath": "path1/path2",
                        "rev": "HEAD",
                        "ref": "test-branch",
                        "depth": 1,
                        "repo": "https://github.com/apache/airflow.git",
                        "branch": "test-branch",
                        "sshKeySecret": None,
                        "credentialsSecret": None,
                        "knownHosts": None,
                        "envFrom": "- secretRef:\n    name: 'proxy-config'\n",
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert jmespath.search("spec.initContainers[0]", docs[0]) == {
            "name": "git-sync-test-init",
            "securityContext": {"runAsUser": 65533},
            "image": "test-registry/test-repo:test-tag",
            "imagePullPolicy": "Always",
            "envFrom": [{"secretRef": {"name": "proxy-config"}}],
            "env": [
                {"name": "GIT_SYNC_REV", "value": "HEAD"},
                {"name": "GITSYNC_REF", "value": "test-branch"},
                {"name": "GIT_SYNC_BRANCH", "value": "test-branch"},
                {"name": "GIT_SYNC_REPO", "value": "https://github.com/apache/airflow.git"},
                {"name": "GITSYNC_REPO", "value": "https://github.com/apache/airflow.git"},
                {"name": "GIT_SYNC_DEPTH", "value": "1"},
                {"name": "GITSYNC_DEPTH", "value": "1"},
                {"name": "GIT_SYNC_ROOT", "value": "/git"},
                {"name": "GITSYNC_ROOT", "value": "/git"},
                {"name": "GIT_SYNC_DEST", "value": "repo"},
                {"name": "GITSYNC_LINK", "value": "repo"},
                {"name": "GIT_SYNC_ADD_USER", "value": "true"},
                {"name": "GITSYNC_ADD_USER", "value": "true"},
                {"name": "GITSYNC_PERIOD", "value": "66s"},
                {"name": "GIT_SYNC_MAX_SYNC_FAILURES", "value": "70"},
                {"name": "GITSYNC_MAX_FAILURES", "value": "70"},
                {"name": "GIT_SYNC_ONE_TIME", "value": "true"},
                {"name": "GITSYNC_ONE_TIME", "value": "true"},
            ],
            "volumeMounts": [{"mountPath": "/git", "name": "dags"}],
            "resources": {},
        }

    def test_should_not_add_init_container_if_dag_persistence_is_true(self):
        docs = render_chart(
            values={
                "dags": {
                    "persistence": {"enabled": True},
                    "gitSync": {"enabled": True},
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers", docs[0]) is None

    @pytest.mark.parametrize(
        ("dag_values", "expected_read_only"),
        [
            ({"gitSync": {"enabled": True}}, True),
            ({"persistence": {"enabled": True}}, False),
            (
                {
                    "gitSync": {"enabled": True},
                    "persistence": {"enabled": True},
                },
                True,
            ),
        ],
    )
    def test_dags_mount(self, dag_values, expected_read_only):
        docs = render_chart(
            values={"dags": dag_values},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {
            "mountPath": "/opt/airflow/dags",
            "name": "dags",
            "readOnly": expected_read_only,
        } in jmespath.search("spec.containers[0].volumeMounts", docs[0])

    def test_should_add_global_volume_and_global_volume_mount(self):
        expected_volume = {"name": "test-volume", "emptyDir": {}}
        expected_volume_mount = {"name": "test-volume", "mountPath": "/opt/test"}
        docs = render_chart(
            values={
                "volumes": [expected_volume],
                "volumeMounts": [expected_volume_mount],
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert expected_volume in jmespath.search("spec.volumes", docs[0])
        assert expected_volume_mount in jmespath.search("spec.containers[0].volumeMounts", docs[0])

    def test_validate_if_ssh_params_are_added(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "sshKeySecret": "ssh-secret",
                        "knownHosts": None,
                        "branch": "test-branch",
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "GIT_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {"name": "GIT_SYNC_SSH", "value": "true"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH", "value": "true"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {"name": "GIT_KNOWN_HOSTS", "value": "false"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH_KNOWN_HOSTS", "value": "false"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {
            "name": "git-sync-ssh-key",
            "mountPath": "/etc/git-secret/ssh",
            "subPath": "gitSshKey",
            "readOnly": True,
        } in jmespath.search("spec.initContainers[0].volumeMounts", docs[0])
        assert {
            "name": "git-sync-ssh-key",
            "secret": {"secretName": "ssh-secret", "defaultMode": 288},
        } in jmespath.search("spec.volumes", docs[0])

    def test_validate_if_ssh_known_hosts_are_added(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "sshKeySecret": "ssh-secret",
                        "knownHosts": "github.com ssh-rsa AAAABdummy",
                        "branch": "test-branch",
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert {"name": "GIT_KNOWN_HOSTS", "value": "true"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {
            "name": "GIT_SSH_KNOWN_HOSTS_FILE",
            "value": "/etc/git-secret/known_hosts",
        } in jmespath.search("spec.initContainers[0].env", docs[0])
        assert {
            "name": "config",
            "mountPath": "/etc/git-secret/known_hosts",
            "subPath": "known_hosts",
            "readOnly": True,
        } in jmespath.search("spec.initContainers[0].volumeMounts", docs[0])

    def test_should_set_username_and_pass_env_variables(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "credentialsSecret": "user-pass-secret",
                        "sshKeySecret": None,
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {
            "name": "GIT_SYNC_USERNAME",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_USERNAME"}},
        } in jmespath.search("spec.initContainers[0].env", docs[0])
        assert {
            "name": "GIT_SYNC_PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_PASSWORD"}},
        } in jmespath.search("spec.initContainers[0].env", docs[0])

        # Testing git-sync v4
        assert {
            "name": "GITSYNC_USERNAME",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GITSYNC_USERNAME"}},
        } in jmespath.search("spec.initContainers[0].env", docs[0])
        assert {
            "name": "GITSYNC_PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GITSYNC_PASSWORD"}},
        } in jmespath.search("spec.initContainers[0].env", docs[0])

    def test_should_set_the_dags_volume_claim_correctly_when_using_an_existing_claim(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": True, "existingClaim": "test-claim"}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "dags", "persistentVolumeClaim": {"claimName": "test-claim"}} in jmespath.search(
            "spec.volumes", docs[0]
        )

    @pytest.mark.parametrize(
        ("dags_gitsync_values", "expected"),
        [
            ({"enabled": True}, {"emptyDir": {}}),
            ({"enabled": True, "emptyDirConfig": {"sizeLimit": "10Gi"}}, {"emptyDir": {"sizeLimit": "10Gi"}}),
        ],
    )
    def test_should_use_empty_dir_for_gitsync_without_persistence(self, dags_gitsync_values, expected):
        docs = render_chart(
            values={"dags": {"gitSync": dags_gitsync_values}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert {"name": "dags", **expected} in jmespath.search("spec.volumes", docs[0])

    @pytest.mark.parametrize(
        ("log_values", "expected"),
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
    def test_logs_persistence_changes_volume(self, log_values, expected):
        docs = render_chart(
            values={"logs": log_values},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "logs", **expected} in jmespath.search("spec.volumes", docs[0])

    def test_should_set_a_custom_image_in_pod_template(self):
        docs = render_chart(
            values={
                "images": {
                    "pod_template": {"repository": "dummy_image", "tag": "latest", "pullPolicy": "Always"}
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert jmespath.search("spec.containers[0].image", docs[0]) == "dummy_image:latest"
        assert jmespath.search("spec.containers[0].imagePullPolicy", docs[0]) == "Always"
        assert jmespath.search("spec.containers[0].name", docs[0]) == "base"

    def test_mount_airflow_cfg(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert {"configMap": {"name": "release-name-config"}, "name": "config"} in jmespath.search(
            "spec.volumes", docs[0]
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "subPath": "airflow.cfg",
            "readOnly": True,
        } in jmespath.search("spec.containers[0].volumeMounts", docs[0])

    def test_global_affinity(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
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
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.affinity", docs[0]) == {
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

    def test_global_tolerations(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "tolerations": [
                    {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.tolerations", docs[0]) == [
            {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
        ]

    def test_global_topology_spread_constraints(self):
        expected_topology_spread_constraints = [
            {
                "maxSkew": 1,
                "topologyKey": "foo",
                "whenUnsatisfiable": "ScheduleAnyway",
                "labelSelector": {"matchLabels": {"tier": "airflow"}},
            }
        ]
        docs = render_chart(
            values={"topologySpreadConstraints": expected_topology_spread_constraints},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert expected_topology_spread_constraints == jmespath.search(
            "spec.topologySpreadConstraints", docs[0]
        )

    def test_global_node_selector(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "nodeSelector": {"diskType": "ssd"},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.nodeSelector", docs[0]) == {"diskType": "ssd"}

    def test_workers_affinity(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
                    "kubernetes": {
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
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.affinity", docs[0]) == {
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

    def test_workers_tolerations(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
                    "kubernetes": {
                        "tolerations": [
                            {
                                "key": "dynamic-pods",
                                "operator": "Equal",
                                "value": "true",
                                "effect": "NoSchedule",
                            }
                        ]
                    },
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.tolerations", docs[0]) == [
            {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
        ]

    def test_workers_topology_spread_constraints(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
                    "kubernetes": {
                        "topologySpreadConstraints": [
                            {
                                "maxSkew": 1,
                                "topologyKey": "foo",
                                "whenUnsatisfiable": "ScheduleAnyway",
                                "labelSelector": {"matchLabels": {"tier": "airflow"}},
                            }
                        ],
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.topologySpreadConstraints", docs[0]) == [
            {
                "maxSkew": 1,
                "topologyKey": "foo",
                "whenUnsatisfiable": "ScheduleAnyway",
                "labelSelector": {"matchLabels": {"tier": "airflow"}},
            }
        ]

    def test_workers_node_selector(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {"kubernetes": {"nodeSelector": {"diskType": "ssd"}}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.nodeSelector", docs[0]) == {"diskType": "ssd"}

    def test_affinity_overwrite(self):
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
        docs = render_chart(
            values={
                "workers": {"kubernetes": {"affinity": expected_affinity}},
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
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert expected_affinity == jmespath.search("spec.affinity", docs[0])

    def test_tolerations_overwrite(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "tolerations": [
                            {
                                "key": "dynamic-pods",
                                "operator": "Equal",
                                "value": "true",
                                "effect": "NoSchedule",
                            }
                        ],
                    }
                },
                "tolerations": [
                    {"key": "not-me", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.tolerations", docs[0]) == [
            {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
        ]

    def test_topology_spread_constraints_overwrite(self):
        expected_topology_spread_constraints = {
            "maxSkew": 1,
            "topologyKey": "foo",
            "whenUnsatisfiable": "ScheduleAnyway",
            "labelSelector": {"matchLabels": {"tier": "airflow"}},
        }
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {"topologySpreadConstraints": [expected_topology_spread_constraints]},
                },
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "not-me",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"tier": "airflow"}},
                    }
                ],
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert [expected_topology_spread_constraints] == jmespath.search(
            "spec.topologySpreadConstraints", docs[0]
        )

    def test_node_selector_overwrite(self):
        docs = render_chart(
            values={
                "workers": {"kubernetes": {"nodeSelector": {"diskType": "ssd"}}},
                "nodeSelector": {"type": "not-me"},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert jmespath.search("spec.nodeSelector", docs[0]) == {"diskType": "ssd"}

    @pytest.mark.parametrize(
        ("base_scheduler_name", "worker_values", "expected"),
        [
            ("default-scheduler", {"kubernetes": {"schedulerName": "most-allocated"}}, "most-allocated"),
            ("default-scheduler", {"kubernetes": {"schedulerName": None}}, "default-scheduler"),
            (None, {"kubernetes": {"schedulerName": None}}, None),
        ],
    )
    def test_scheduler_name(self, base_scheduler_name, worker_values, expected):
        docs = render_chart(
            values={
                "schedulerName": base_scheduler_name,
                "workers": worker_values,
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        scheduler_name = jmespath.search("spec.schedulerName", docs[0])

        if expected is not None:
            assert scheduler_name == expected
        else:
            assert scheduler_name is None

    def test_should_not_create_default_affinity(self):
        docs = render_chart(show_only=["templates/pod-template-file.yaml"], chart_dir=self.temp_chart_dir)

        assert jmespath.search("spec.affinity", docs[0]) == {}

    def test_pod_security_context_default(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.securityContext", docs[0]) == {"runAsUser": 50_000, "fsGroup": 0}

    def test_container_security_context_default(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
        }

    @pytest.mark.parametrize(
        "values",
        [
            {"securityContexts": {"pod": {"runAsUser": 10}}},
            {"workers": {"kubernetes": {"securityContexts": {"pod": {"runAsUser": 10}}}}},
        ],
    )
    def test_pod_security_context_set(self, values):
        docs = render_chart(
            values=values,
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.securityContext", docs[0]) == {"runAsUser": 10}

    @pytest.mark.parametrize(
        "values",
        [
            {"securityContexts": {"containers": {"allowPrivilegeEscalation": False}}},
            {
                "workers": {
                    "kubernetes": {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}}
                }
            },
        ],
    )
    def test_container_security_context_set(self, values):
        docs = render_chart(
            values=values,
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False
        }

    def test_pod_security_context_overwrite(self):
        docs = render_chart(
            values={
                "securityContexts": {"pod": {"runAsUser": 5}},
                "workers": {"kubernetes": {"securityContexts": {"pod": {"runAsUser": 10}}}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.securityContext", docs[0]) == {"runAsUser": 10}

    def test_container_security_context_overwrite(self):
        docs = render_chart(
            values={
                "securityContexts": {"containers": {"allowPrivilegeEscalation": True}},
                "workers": {
                    "kubernetes": {"securityContexts": {"container": {"allowPrivilegeEscalation": False}}}
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False
        }

    def test_should_add_gid_to_the_pod_template(self):
        docs = render_chart(
            values={"gid": 1},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.securityContext.fsGroup", docs[0]) == 1

    def test_should_add_uid_to_the_pod_template(self):
        docs = render_chart(
            values={"uid": 1},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.securityContext.runAsUser", docs[0]) == 1

    def test_should_create_valid_volume(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}]
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        # [2:] -> Skipping logs and config volumes
        assert jmespath.search("spec.volumes[2:].name", docs[0]) == ["test-volume-airflow"]

    def test_should_create_valid_volume_mount(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "extraVolumeMounts": [
                            {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                        ],
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        volume_mounts = jmespath.search("spec.containers[0].volumeMounts", docs[0])
        assert {"name": "test-volume-airflow", "mountPath": "/opt/test"} in volume_mounts
        assert {"name": "test", "mountPath": "test"} not in volume_mounts

    def test_should_add_env_for_gitsync(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "env": [{"name": "FOO", "value": "bar"}],
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "FOO", "value": "bar"} in jmespath.search("spec.initContainers[0].env", docs[0])

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": None},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        volume_mounts = jmespath.search("spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.containers[0].volumeMounts", docs[0])

    def test_airflow_pod_annotations(self):
        docs = render_chart(
            values={"airflowPodAnnotations": {"my_annotation": "annotated!"}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert "my_annotation" in annotations
        assert "annotated!" in annotations["my_annotation"]

    @pytest.mark.parametrize(
        ("workers_values", "expected"),
        [
            ({"kubernetes": {"safeToEvict": True}}, "true"),
            ({"kubernetes": {"safeToEvict": False}}, "false"),
        ],
    )
    def test_safe_to_evict_annotation(self, workers_values, expected):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations == {"cluster-autoscaler.kubernetes.io/safe-to-evict": expected}

    def test_safe_to_evict_annotation_other_services(self):
        """Workers' safeToEvict value should not overwrite safeToEvict value of other services."""
        docs = render_chart(
            values={
                "workers": {"kubernetes": {"safeToEvict": False}},
                "scheduler": {"safeToEvict": True},
                "triggerer": {"safeToEvict": True},
                "executor": "KubernetesExecutor",
                "dagProcessor": {"safeToEvict": True},
            },
            show_only=[
                "templates/dag-processor/dag-processor-deployment.yaml",
                "templates/triggerer/triggerer-deployment.yaml",
                "templates/scheduler/scheduler-deployment.yaml",
            ],
            chart_dir=self.temp_chart_dir,
        )
        for doc in docs:
            annotations = jmespath.search("spec.template.metadata.annotations", doc)
            assert annotations.get("cluster-autoscaler.kubernetes.io/safe-to-evict") == "true"

    def test_global_pod_annotations_templated(self):
        docs = render_chart(
            values={"airflowPodAnnotations": {"global": "{{ .Release.Name }}"}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("metadata.annotations", docs[0])["global"] == "release-name"

    def test_workers_pod_annotations_templated(self):
        docs = render_chart(
            values={"workers": {"kubernetes": {"podAnnotations": {"my_annotation": "{{ .Release.Name }}"}}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("metadata.annotations", docs[0])["my_annotation"] == "release-name"

    def test_workers_pod_annotations_override(self):
        # Worker-specific pod annotations should override global airflowPodAnnotations,
        # whether they come from workers.podAnnotations or workers.kubernetes.podAnnotations
        docs = render_chart(
            values={
                "airflowPodAnnotations": {"my_annotation": "airflowPodAnnotations"},
                "workers": {"kubernetes": {"podAnnotations": {"my_annotation": "workerPodAnnotations"}}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("metadata.annotations", docs[0])["my_annotation"] == "workerPodAnnotations"

    def test_pod_annotations_merge(self):
        docs = render_chart(
            values={
                "airflowPodAnnotations": {"global": "airflowPodAnnotations"},
                "workers": {"kubernetes": {"podAnnotations": {"local": "workerPodAnnotations"}}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["global"] == "airflowPodAnnotations"
        assert annotations["local"] == "workerPodAnnotations"

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "extraInitContainers": [
                            {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                        ]
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers", docs[0]) == [
            {
                "name": "test-init-container",
                "image": "test-registry/test-repo:test-tag",
            }
        ]

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "extraInitContainers": [{"name": "{{ .Release.Name }}-test-init-container"}]
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers", docs[0]) == [
            {
                "name": "release-name-test-init-container",
            }
        ]

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "extraContainers": [
                            {"name": "test-container", "image": "test-registry/test-repo:test-tag"}
                        ]
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[1:]", docs[0]) == [
            {
                "name": "test-container",
                "image": "test-registry/test-repo:test-tag",
            }
        ]

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {"extraContainers": [{"name": "{{ .Release.Name }}-test-container"}]}
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[1:]", docs[0]) == [
            {
                "name": "release-name-test-container",
            }
        ]

    def test_should_add_pod_labels(self):
        docs = render_chart(
            values={"labels": {"label1": "value1", "label2": "value2"}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("metadata.labels", docs[0]) == {
            "label1": "value1",
            "label2": "value2",
            "release": "release-name",
            "component": "worker",
            "tier": "airflow",
        }

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
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
                        ]
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.containers[0].env", docs[0]
        )
        assert {"name": "TEST", "value": "test"} not in jmespath.search("spec.containers[0].env", docs[0])
        assert {
            "name": "TEST_ENV_2",
            "valueFrom": {"secretKeyRef": {"name": "my-secret", "key": "my-key"}},
        } in jmespath.search("spec.containers[0].env", docs[0])
        assert {
            "name": "TEST_ENV_3",
            "valueFrom": {"configMapKeyRef": {"name": "my-config-map", "key": "my-key"}},
        } in jmespath.search("spec.containers[0].env", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
                    "kubernetes": {
                        "labels": {"test_label": "test_label_value"},
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        labels = jmespath.search("metadata.labels", docs[0])
        assert labels["test_label"] == "test_label_value"
        assert "key" not in labels

    def test_should_add_resources(self):
        docs = render_chart(
            values={"workers": {"kubernetes": {"resources": {"requests": {"memory": "2Gi", "cpu": "1"}}}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].resources", docs[0]) == {
            "requests": {
                "cpu": "1",
                "memory": "2Gi",
            },
        }

    def test_empty_resources(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert jmespath.search("spec.containers[0].resources", docs[0]) == {}

    def test_workers_host_aliases(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {"hostAliases": [{"ip": "127.0.0.2", "hostnames": ["test.hostname"]}]}
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.hostAliases", docs[0]) == [
            {"ip": "127.0.0.2", "hostnames": ["test.hostname"]}
        ]

    def test_workers_priority_class_name(self):
        docs = render_chart(
            values={"workers": {"kubernetes": {"priorityClassName": "test-priority"}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.priorityClassName", docs[0]) == "test-priority"

    def test_workers_container_lifecycle_webhooks_are_configurable(self):
        docs = render_chart(
            name="test-release",
            values={
                "workers": {
                    "kubernetes": {
                        "containerLifecycleHooks": {
                            "preStop": {"exec": {"command": ["echo", "preStop", "{{ .Release.Name }}"]}}
                        }
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].lifecycle", docs[0]) == {
            "preStop": {"exec": {"command": ["echo", "preStop", "test-release"]}}
        }

    def test_termination_grace_period_seconds(self):
        docs = render_chart(
            values={"workers": {"kubernetes": {"terminationGracePeriodSeconds": 123}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.terminationGracePeriodSeconds", docs[0]) == 123

    def test_runtime_class_name_values_are_configurable(self):
        docs = render_chart(
            values={"workers": {"kubernetes": {"runtimeClassName": "nvidia"}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.runtimeClassName", docs[0]) == "nvidia"

    def test_airflow_local_settings_kerberos_sidecar(self):
        docs = render_chart(
            values={
                "airflowLocalSettings": "# Well hello!",
                "workers": {"kubernetes": {"kerberosSidecar": {"enabled": True}}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert jmespath.search("spec.containers[1].name", docs[0]) == "worker-kerberos"

        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.containers[1].volumeMounts", docs[0])

    def test_kerberos_sidecar_resources(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "kerberosSidecar": {
                            "enabled": True,
                            "resources": {
                                "requests": {"cpu": "1m", "memory": "2Mi"},
                            },
                        }
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[?name=='worker-kerberos'] | [0].resources", docs[0]) == {
            "requests": {
                "cpu": "1m",
                "memory": "2Mi",
            },
        }

    @pytest.mark.parametrize("hook_type", ["preStop", "postStart"])
    def test_kerberos_sidecar_lifecycle(self, hook_type):
        docs = render_chart(
            name="test-release",
            values={
                "workers": {
                    "kubernetes": {
                        "kerberosSidecar": {
                            "enabled": True,
                            "containerLifecycleHooks": {
                                hook_type: {"exec": {"command": ["echo", "{{ .Release.Name }}"]}}
                            },
                        }
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[1].lifecycle", docs[0]) == {
            hook_type: {"exec": {"command": ["echo", "test-release"]}}
        }

    def test_kerberos_sidecar_security_context(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "kerberosSidecar": {
                            "enabled": True,
                            "securityContexts": {"container": {"allowPrivilegeEscalation": False}},
                        }
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[1].securityContext", docs[0]) == {
            "allowPrivilegeEscalation": False
        }

    def test_kerberos_init_container_default(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers[?name=='kerberos-init']", docs[0]) is None

    def test_kerberos_init_container_default_different_versions(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers[?name=='kerberos-init']", docs[0]) is None

    def test_kerberos_init_container_enable(self):
        docs = render_chart(
            values={
                "workers": {"kubernetes": {"kerberosInitContainer": {"enabled": True}}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers[?name=='kerberos-init']", docs[0]) is not None

    def test_kerberos_init_container_name_and_args_default(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {"kerberosInitContainer": {"enabled": True}},
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        initContainers = jmespath.search("spec.initContainers[0]", docs[0])

        assert initContainers["name"] == "kerberos-init"
        assert initContainers["args"] == ["kerberos", "-o"]

    @pytest.mark.parametrize(
        ("workers_values", "expected"),
        [
            ({"kubernetes": {"command": ["test", "command", "to", "run"]}}, ["test", "command", "to", "run"]),
            ({"kubernetes": {"command": ["cmd", "{{ .Release.Name }}"]}}, ["cmd", "release-name"]),
        ],
    )
    def test_should_add_command(self, workers_values, expected):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert expected == jmespath.search("spec.containers[0].command", docs[0])

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"kubernetes": {"command": None}},
            {"kubernetes": {"command": []}},
        ],
    )
    def test_should_not_add_command(self, workers_values):
        docs = render_chart(
            values={"workers": workers_values},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].command", docs[0]) is None

    def test_should_not_add_command_by_default(self):
        docs = render_chart(
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].command", docs[0]) is None

    @pytest.mark.parametrize(
        ("workers_values", "kerberos_init_container", "expected_config_name"),
        [
            ({"kubernetes": {"kerberosSidecar": {"enabled": True}}}, False, "api-server-config"),
            ({"kubernetes": {"kerberosInitContainer": {"enabled": True}}}, True, "api-server-config"),
        ],
    )
    def test_api_server_config_for_kerberos(
        self, workers_values, kerberos_init_container, expected_config_name
    ):
        values = {
            "workers": workers_values,
            "apiServer": {"apiServerConfigConfigMapName": "config"},
        }
        docs = render_chart(
            values=values,
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        kerberos_container = "spec.containers[1].volumeMounts[*].name"
        if kerberos_init_container:
            kerberos_container = "spec.initContainers[0].volumeMounts[*].name"

        volume_mounts_names = jmespath.search(kerberos_container, docs[0])
        print(volume_mounts_names)
        assert expected_config_name in volume_mounts_names
        assert expected_config_name in jmespath.search("spec.volumes[*].name", docs[0])

    @pytest.mark.parametrize(
        "workers_values",
        [
            {"kubernetes": {"kerberosSidecar": {"enabled": True}}},
            {"kubernetes": {"kerberosInitContainer": {"enabled": True}}},
        ],
    )
    def test_base_contains_kerberos_env(self, workers_values):
        docs = render_chart(
            values={
                "workers": workers_values,
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        scheduler_env = jmespath.search("spec.containers[0].env[*].name", docs[0])
        assert set(["KRB5_CONFIG", "KRB5CCNAME"]).issubset(scheduler_env)

    def test_kerberos_init_container_resources(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "kerberosInitContainer": {
                            "enabled": True,
                            "resources": {
                                "requests": {"cpu": "1m", "memory": "2Mi"},
                                "limits": {"cpu": "3m", "memory": "4Mi"},
                            },
                        }
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers[?name=='kerberos-init'] | [0].resources", docs[0]) == {
            "requests": {
                "cpu": "1m",
                "memory": "2Mi",
            },
            "limits": {
                "cpu": "3m",
                "memory": "4Mi",
            },
        }

    def test_kerberos_init_container_security_context(self):
        docs = render_chart(
            values={
                "workers": {
                    "kubernetes": {
                        "kerberosInitContainer": {
                            "enabled": True,
                            "securityContexts": {"container": {"runAsUser": 2000}},
                        }
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search(
            "spec.initContainers[?name=='kerberos-init'] | [0].securityContext", docs[0]
        ) == {"runAsUser": 2000}

    def test_kerberos_init_container_lifecycle_hooks(self):
        docs = render_chart(
            name="test-release",
            values={
                "workers": {
                    "kubernetes": {
                        "kerberosInitContainer": {
                            "enabled": True,
                            "containerLifecycleHooks": {
                                "postStart": {"exec": {"command": ["echo", "{{ .Release.Name }}"]}}
                            },
                        }
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers[?name=='kerberos-init'] | [0].lifecycle", docs[0]) == {
            "postStart": {"exec": {"command": ["echo", "test-release"]}}
        }

    def test_service_account_name_default(self):
        docs = render_chart(
            name="test-release",
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.serviceAccountName", docs[0]) == "test-release-airflow-worker-kubernetes"

    def test_dedicated_service_account_name_default(self):
        docs = render_chart(
            name="test-release",
            values={"workers": {"kubernetes": {"serviceAccount": {"create": True}}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.serviceAccountName", docs[0]) == "test-release-airflow-worker-kubernetes"

    def test_dedicated_service_account_name_when_creation_disabled(self):
        docs = render_chart(
            name="test-release",
            values={
                "workers": {
                    "kubernetes": {
                        "serviceAccount": {
                            "create": False,
                            "name": "airflow",
                        }
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.serviceAccountName", docs[0]) == "airflow"
