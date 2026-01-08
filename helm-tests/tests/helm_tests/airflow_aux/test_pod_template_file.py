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
            values={},
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

    @pytest.mark.parametrize(
        ("tag", "expected_prefix"),
        [
            ("v3.6.7", "GIT_SYNC_"),
            ("v4.4.2", "GITSYNC_"),
            ("latest", "GITSYNC_"),
        ],
    )
    def test_should_set_username_and_pass_env_variables(self, tag, expected_prefix):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "credentialsSecret": "user-pass-secret",
                        "sshKeySecret": None,
                    }
                },
                "images": {
                    "gitSync": {
                        "tag": tag,
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        envs = jmespath.search("spec.initContainers[0].env", docs[0])

        assert {
            "name": f"{expected_prefix}USERNAME",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": f"{expected_prefix}USERNAME"}},
        } in envs

        assert {
            "name": f"{expected_prefix}PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": f"{expected_prefix}PASSWORD"}},
        } in envs

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
            values={},
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
        assert (
            jmespath.search(
                "spec.affinity.nodeAffinity."
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
                "spec.nodeSelector.diskType",
                docs[0],
            )
            == "ssd"
        )
        assert (
            jmespath.search(
                "spec.tolerations[0].key",
                docs[0],
            )
            == "dynamic-pods"
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

        assert jmespath.search("kind", docs[0]) == "Pod"
        assert (
            jmespath.search(
                "spec.affinity.nodeAffinity."
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
                "spec.nodeSelector.diskType",
                docs[0],
            )
            == "ssd"
        )
        assert (
            jmespath.search(
                "spec.tolerations[0].key",
                docs[0],
            )
            == "dynamic-pods"
        )
        assert (
            jmespath.search(
                "spec.topologySpreadConstraints[0].topologyKey",
                docs[0],
            )
            == "foo"
        )

    def test_affinity_tolerations_topology_spread_constraints_and_node_selector_precedence(self):
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
        assert (
            jmespath.search(
                "spec.nodeSelector.type",
                docs[0],
            )
            == "ssd"
        )
        tolerations = jmespath.search("spec.tolerations", docs[0])
        assert len(tolerations) == 1
        assert tolerations[0]["key"] == "dynamic-pods"
        assert expected_topology_spread_constraints == jmespath.search(
            "spec.topologySpreadConstraints[0]", docs[0]
        )

    @pytest.mark.parametrize(
        ("base_scheduler_name", "worker_scheduler_name", "expected"),
        [
            ("default-scheduler", "most-allocated", "most-allocated"),
            ("default-scheduler", None, "default-scheduler"),
            (None, None, None),
        ],
    )
    def test_scheduler_name(self, base_scheduler_name, worker_scheduler_name, expected):
        docs = render_chart(
            values={
                "schedulerName": base_scheduler_name,
                "workers": {"schedulerName": worker_scheduler_name},
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

    def test_should_add_fsgroup_to_the_pod_template(self):
        docs = render_chart(
            values={"gid": 5000},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.securityContext.fsGroup", docs[0]) == 5000

    def test_should_create_valid_volume_mount_and_volume(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                    "extraVolumeMounts": [
                        {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                    ],
                }
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert "test-volume-airflow" in jmespath.search(
            "spec.volumes[*].name",
            docs[0],
        )
        assert "test-volume-airflow" in jmespath.search(
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

    @pytest.mark.parametrize("safe_to_evict", [True, False])
    def test_safe_to_evict_annotation(self, safe_to_evict: bool):
        docs = render_chart(
            values={"workers": {"safeToEvict": safe_to_evict}},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations == {
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "true" if safe_to_evict else "false"
        }

    def test_safe_to_evict_annotation_other_services(self):
        """Workers' safeToEvict value should not overwrite safeToEvict value of other services."""
        docs = render_chart(
            values={
                "workers": {"safeToEvict": False},
                "scheduler": {"safeToEvict": True},
                "triggerer": {"safeToEvict": True},
                "executor": "KubernetesExecutor",
                "dagProcessor": {"enabled": True, "safeToEvict": True},
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

        assert jmespath.search("spec.initContainers[-1]", docs[0]) == {
            "name": "test-init-container",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_init_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraInitContainers": [{"name": "{{ .Release.Name }}-test-init-container"}],
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.initContainers[-1]", docs[0]) == {
            "name": "release-name-test-init-container",
        }

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

        assert jmespath.search("spec.containers[-1]", docs[0]) == {
            "name": "test-container",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "workers": {
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[-1]", docs[0]) == {
            "name": "release-name-test-container",
        }

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
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.containers[0].env", docs[0]
        )
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
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

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

        assert jmespath.search("spec.containers[0].resources", docs[0]) == {
            "limits": {
                "cpu": "2",
                "memory": "3Gi",
            },
            "requests": {
                "cpu": "1",
                "memory": "2Gi",
            },
        }

    def test_empty_resources(self):
        docs = render_chart(
            values={},
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        assert jmespath.search("spec.containers[0].resources", docs[0]) == {}

    def test_workers_host_aliases(self):
        docs = render_chart(
            values={
                "workers": {
                    "hostAliases": [{"ip": "127.0.0.2", "hostnames": ["test.hostname"]}],
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.hostAliases[0].ip", docs[0]) == "127.0.0.2"
        assert jmespath.search("spec.hostAliases[0].hostnames[0]", docs[0]) == "test.hostname"

    def test_workers_priority_class_name(self):
        docs = render_chart(
            values={
                "workers": {
                    "priorityClassName": "test-priority",
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.priorityClassName", docs[0]) == "test-priority"

    def test_workers_container_lifecycle_webhooks_are_configurable(self):
        docs = render_chart(
            name="test-release",
            values={
                "workers": {
                    "containerLifecycleHooks": {
                        "preStop": {"exec": {"command": ["echo", "preStop", "{{ .Release.Name }}"]}}
                    }
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.containers[0].lifecycle.preStop", docs[0]) == {
            "exec": {"command": ["echo", "preStop", "test-release"]}
        }

    def test_termination_grace_period_seconds(self):
        docs = render_chart(
            values={
                "workers": {
                    "terminationGracePeriodSeconds": 123,
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.terminationGracePeriodSeconds", docs[0]) == 123

    def test_runtime_class_name_values_are_configurable(self):
        docs = render_chart(
            values={
                "workers": {"runtimeClassName": "nvidia"},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.runtimeClassName", docs[0]) == "nvidia"

    def test_airflow_local_settings_kerberos_sidecar(self):
        docs = render_chart(
            values={
                "airflowLocalSettings": "# Well hello!",
                "workers": {"kerberosSidecar": {"enabled": True}},
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )
        jmespath.search("spec.containers[1].name", docs[0]) == "worker-kerberos"

        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.containers[1].volumeMounts", docs[0])

    @pytest.mark.parametrize(
        ("airflow_version", "init_container_enabled", "expected_init_containers"),
        [
            ("1.10.14", True, 0),
            ("1.10.14", False, 0),
            ("2.0.2", True, 0),
            ("2.0.2", False, 0),
            ("2.1.0", True, 0),
            ("2.1.0", False, 0),
            ("2.8.0", True, 1),
            ("2.8.0", False, 0),
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
                },
            },
            show_only=["templates/pod-template-file.yaml"],
            chart_dir=self.temp_chart_dir,
        )

        initContainers = jmespath.search("spec.initContainers", docs[0])
        if expected_init_containers == 0:
            assert initContainers is None

        if expected_init_containers == 1:
            assert initContainers[0]["name"] == "kerberos-init"
            assert initContainers[0]["args"] == ["kerberos", "-o"]

    @pytest.mark.parametrize(
        ("workers_values", "expected"),
        [
            ({"command": ["test", "command", "to", "run"]}, ["test", "command", "to", "run"]),
            ({"command": ["cmd", "{{ .Release.Name }}"]}, ["cmd", "release-name"]),
            ({"kubernetes": {"command": ["test", "command", "to", "run"]}}, ["test", "command", "to", "run"]),
            ({"kubernetes": {"command": ["cmd", "{{ .Release.Name }}"]}}, ["cmd", "release-name"]),
            (
                {"command": ["test"], "kubernetes": {"command": ["test", "command", "to", "run"]}},
                ["test", "command", "to", "run"],
            ),
            (
                {"command": ["test"], "kubernetes": {"command": ["cmd", "{{ .Release.Name }}"]}},
                ["cmd", "release-name"],
            ),
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
            {"command": None},
            {"command": []},
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
        ("airflow_version", "workers_values", "kerberos_init_container", "expected_config_name"),
        [
            (None, {"kerberosSidecar": {"enabled": True}}, False, "api-server-config"),
            (None, {"kerberosInitContainer": {"enabled": True}}, True, "api-server-config"),
            ("2.10.5", {"kerberosSidecar": {"enabled": True}}, False, "webserver-config"),
            ("2.10.5", {"kerberosInitContainer": {"enabled": True}}, True, "webserver-config"),
        ],
    )
    def test_webserver_config_for_kerberos(
        self, airflow_version, workers_values, kerberos_init_container, expected_config_name
    ):
        values = {
            "airflowVersion": airflow_version,
            "workers": workers_values,
            "apiServer": {"apiServerConfigConfigMapName": "config"},
            "webserver": {"webserverConfigConfigMapName": "config"},
        }
        if airflow_version is None:
            del values["airflowVersion"]
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
        [{"kerberosSidecar": {"enabled": True}}, {"kerberosInitContainer": {"enabled": True}}],
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

    def test_workers_kubernetes_service_account_custom_names_in_objects(self):
        k8s_objects = render_chart(
            "test-rbac",
            values={
                "workers": {
                    "useWorkerDedicatedServiceAccounts": True,
                    "kubernetes": {"serviceAccount": {"name": "TestWorkerKubernetes"}},
                },
            },
            show_only=[
                "templates/pod-template-file.yaml",
            ],
            chart_dir=self.temp_chart_dir,
        )

        assert jmespath.search("spec.serviceAccountName", k8s_objects[0]) == "TestWorkerKubernetes"
