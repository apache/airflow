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
import re
import unittest
from pathlib import Path
from shutil import copyfile, copytree
from tempfile import TemporaryDirectory

import jmespath
import pytest
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart

CHART_DIR = Path(__file__).parent / ".." / ".." / "chart"


class PodTemplateFileTest(unittest.TestCase):
    @classmethod
    @pytest.fixture(autouse=True, scope="class")
    def isolate_chart(cls):
        with TemporaryDirectory() as tmp_dir:
            cls.temp_chart_dir = tmp_dir + "/chart"
            copytree(CHART_DIR, cls.temp_chart_dir)
            copyfile(
                cls.temp_chart_dir + "/files/pod-template-file.kubernetes-helm-yaml",
                cls.temp_chart_dir + "/templates/pod-template-file.yaml",
            )
            yield

    def test_should_work(self):
        docs = render_chart(
            values={},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert jmespath.search("spec.containers[0].image", docs[0]) is not None
        assert "base" == jmespath.search("spec.containers[0].name", docs[0])

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
                        "wait": 66,
                        "maxFailures": 70,
                        "subPath": "path1/path2",
                        "rev": "HEAD",
                        "depth": 1,
                        "repo": "https://github.com/apache/airflow.git",
                        "branch": "test-branch",
                        "sshKeySecret": None,
                        "credentialsSecret": None,
                        "knownHosts": None,
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert {
            "name": "git-sync-test-init",
            "securityContext": {"runAsUser": 65533},
            "image": "test-registry/test-repo:test-tag",
            "imagePullPolicy": "Always",
            "env": [
                {"name": "GIT_SYNC_REV", "value": "HEAD"},
                {"name": "GIT_SYNC_BRANCH", "value": "test-branch"},
                {"name": "GIT_SYNC_REPO", "value": "https://github.com/apache/airflow.git"},
                {"name": "GIT_SYNC_DEPTH", "value": "1"},
                {"name": "GIT_SYNC_ROOT", "value": "/git"},
                {"name": "GIT_SYNC_DEST", "value": "repo"},
                {"name": "GIT_SYNC_ADD_USER", "value": "true"},
                {"name": "GIT_SYNC_WAIT", "value": "66"},
                {"name": "GIT_SYNC_MAX_SYNC_FAILURES", "value": "70"},
                {"name": "GIT_SYNC_ONE_TIME", "value": "true"},
            ],
            "volumeMounts": [{"mountPath": "/git", "name": "dags"}],
            "resources": {},
        } == jmespath.search("spec.initContainers[0]", docs[0])

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

    @parameterized.expand(
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
        ]
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
        assert {"name": "GIT_SYNC_SSH", "value": "true"} in jmespath.search(
            "spec.initContainers[0].env", docs[0]
        )
        assert {"name": "GIT_KNOWN_HOSTS", "value": "false"} in jmespath.search(
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

    def test_should_set_the_dags_volume_claim_correctly_when_using_an_existing_claim(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": True, "existingClaim": "test-claim"}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "dags", "persistentVolumeClaim": {"claimName": "test-claim"}} in jmespath.search(
            "spec.volumes", docs[0]
        )

    def test_should_use_empty_dir_for_gitsync_without_persistence(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "dags", "emptyDir": {}} in jmespath.search("spec.volumes", docs[0])

    @parameterized.expand(
        [
            ({"enabled": False}, {"emptyDir": {}}),
            ({"enabled": True}, {"persistentVolumeClaim": {"claimName": "RELEASE-NAME-logs"}}),
            (
                {"enabled": True, "existingClaim": "test-claim"},
                {"persistentVolumeClaim": {"claimName": "test-claim"}},
            ),
        ]
    )
    def test_logs_persistence_changes_volume(self, log_persistence_values, expected):
        docs = render_chart(
            values={"logs": {"persistence": log_persistence_values}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "logs", **expected} in jmespath.search("spec.volumes", docs[0])

    def test_should_set_a_custom_image_in_pod_template(self):
        docs = render_chart(
            values={"images": {"pod_template": {"repository": "dummy_image", "tag": "latest"}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert "dummy_image:latest" == jmespath.search("spec.containers[0].image", docs[0])
        assert "base" == jmespath.search("spec.containers[0].name", docs[0])

    def test_mount_airflow_cfg(self):
        docs = render_chart(
            values={},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert {'configMap': {'name': 'RELEASE-NAME-airflow-config'}, 'name': 'config'} in jmespath.search(
            "spec.volumes", docs[0]
        )
        assert {
            'name': 'config',
            'mountPath': '/opt/airflow/airflow.cfg',
            'subPath': 'airflow.cfg',
            'readOnly': True,
        } in jmespath.search("spec.containers[0].volumeMounts", docs[0])

    def test_should_use_global_affinity_tolerations_and_node_selector(self):
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
                "tolerations": [
                    {"key": "dynamic-pods", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
                "nodeSelector": {"diskType": "ssd"},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert re.search("Pod", docs[0]["kind"])
        assert "foo" == jmespath.search(
            "spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.tolerations[0].key",
            docs[0],
        )

    def test_should_create_valid_affinity_tolerations_topology_spread_constraints_and_node_selector(self):
        docs = render_chart(
            values={
                "executor": "KubernetesExecutor",
                "workers": {
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
                    "topologySpreadConstraints": [
                        {
                            "maxSkew": 1,
                            "topologyKey": "foo",
                            "whenUnsatisfiable": "ScheduleAnyway",
                            "labelSelector": {"matchLabels": {"tier": "airflow"}},
                        }
                    ],
                    "nodeSelector": {"diskType": "ssd"},
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert "Pod" == jmespath.search("kind", docs[0])
        assert "foo" == jmespath.search(
            "spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.tolerations[0].key",
            docs[0],
        )
        assert "foo" == jmespath.search(
            "spec.topologySpreadConstraints[0].topologyKey",
            docs[0],
        )

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(self):
        """When given both global and worker affinity etc, worker affinity etc is used"""
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
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert expected_affinity == jmespath.search("spec.affinity", docs[0])
        assert "ssd" == jmespath.search(
            "spec.nodeSelector.type",
            docs[0],
        )
        tolerations = jmespath.search("spec.tolerations", docs[0])
        assert 1 == len(tolerations)
        assert "dynamic-pods" == tolerations[0]["key"]
        assert expected_topology_spread_constraints == jmespath.search(
            "spec.topologySpreadConstraints[0]", docs[0]
        )

    def test_should_not_create_default_affinity(self):
        docs = render_chart(show_only=["templates/pod-template-file.yaml"], chart_dir=self.temp_chart_dir)

        assert {} == jmespath.search("spec.affinity", docs[0])

    def test_should_add_fsgroup_to_the_pod_template(self):
        docs = render_chart(
            values={"gid": 5000},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        self.assertEqual(5000, jmespath.search("spec.securityContext.fsGroup", docs[0]))

    def test_should_create_valid_volume_mount_and_volume(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraVolumes": [{"name": "test-volume", "emptyDir": {}}],
                    "extraVolumeMounts": [{"name": "test-volume", "mountPath": "/opt/test"}],
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert "test-volume" in jmespath.search(
            "spec.volumes[*].name",
            docs[0],
        )
        assert "test-volume" in jmespath.search(
            "spec.containers[0].volumeMounts[*].name",
            docs[0],
        )

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

    def test_workers_pod_annotations(self):
        docs = render_chart(
            values={"workers": {"podAnnotations": {"my_annotation": "annotated!"}}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert "my_annotation" in annotations
        assert "annotated!" in annotations["my_annotation"]

    def test_airflow_and_workers_pod_annotations(self):
        # should give preference to workers.podAnnotations
        docs = render_chart(
            values={
                "airflowPodAnnotations": {"my_annotation": "airflowPodAnnotations"},
                "workers": {"podAnnotations": {"my_annotation": "workerPodAnnotations"}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert "my_annotation" in annotations
        assert "workerPodAnnotations" in annotations["my_annotation"]

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraInitContainers": [
                        {"name": "test-init-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.initContainers[-1]", docs[0])

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraContainers": [
                        {"name": "test-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {
            "name": "test-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.containers[-1]", docs[0])

    def test_should_add_pod_labels(self):
        docs = render_chart(
            values={"labels": {"label1": "value1", "label2": "value2"}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {
            "label1": "value1",
            "label2": "value2",
            "release": "RELEASE-NAME",
            "component": "worker",
            "tier": "airflow",
        } == jmespath.search("metadata.labels", docs[0])

    def test_should_add_resources(self):
        docs = render_chart(
            values={
                "workers": {
                    "resources": {
                        "requests": {"memory": "2Gi", "cpu": "1"},
                        "limits": {"memory": "3Gi", "cpu": "2"},
                    }
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
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
        } == jmespath.search("spec.containers[0].resources", docs[0])

    def test_empty_resources(self):
        docs = render_chart(
            values={},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert {} == jmespath.search("spec.containers[0].resources", docs[0])
