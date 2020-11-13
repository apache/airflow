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

import unittest
from os import remove
from os.path import dirname, realpath
from shutil import copyfile

import jmespath

from tests.helm_template_generator import render_chart

ROOT_FOLDER = realpath(dirname(realpath(__file__)) + "/..")


class PodTemplateFileTest(unittest.TestCase):
    def setUp(self):
        copyfile(
            ROOT_FOLDER + "/files/pod-template-file.kubernetes-helm-yaml",
            ROOT_FOLDER + "/templates/pod-template-file.yaml",
        )

    def tearDown(self):
        remove(ROOT_FOLDER + "/templates/pod-template-file.yaml")

    def test_should_work(self):
        docs = render_chart(
            values={},
            show_only=["templates/pod-template-file.yaml"],
        )

        self.assertRegex(docs[0]["kind"], "Pod")
        self.assertIsNotNone(jmespath.search("spec.containers[0].image", docs[0]))
        self.assertEqual("base", jmespath.search("spec.containers[0].name", docs[0]))

    def test_should_add_an_init_container_if_git_sync_is_true(self):
        docs = render_chart(
            values={
                "images": {
                    "gitSync": {
                        "repository": "test-registry/test-repo",
                        "tag": "test-tag",
                        "pullPolicy": "Allways",
                    }
                },
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "wait": 66,
                        "maxFailures": 70,
                        "subPath": "path1/path2",
                        "dest": "test-dest",
                        "root": "/git-root",
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
        )

        self.assertRegex(docs[0]["kind"], "Pod")
        self.assertEqual(
            {
                "name": "git-sync-test",
                "securityContext": {"runAsUser": 65533},
                "image": "test-registry/test-repo:test-tag",
                "imagePullPolicy": "Allways",
                "env": [
                    {"name": "GIT_SYNC_REV", "value": "HEAD"},
                    {"name": "GIT_SYNC_BRANCH", "value": "test-branch"},
                    {"name": "GIT_SYNC_REPO", "value": "https://github.com/apache/airflow.git"},
                    {"name": "GIT_SYNC_DEPTH", "value": "1"},
                    {"name": "GIT_SYNC_ROOT", "value": "/git-root"},
                    {"name": "GIT_SYNC_DEST", "value": "test-dest"},
                    {"name": "GIT_SYNC_ADD_USER", "value": "true"},
                    {"name": "GIT_SYNC_WAIT", "value": "66"},
                    {"name": "GIT_SYNC_MAX_SYNC_FAILURES", "value": "70"},
                    {"name": "GIT_SYNC_ONE_TIME", "value": "true"},
                ],
                "volumeMounts": [{"mountPath": "/git-root", "name": "dags"}],
            },
            jmespath.search("spec.initContainers[0]", docs[0]),
        )

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
        )

        self.assertIn(
            {"name": "GIT_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"},
            jmespath.search("spec.initContainers[0].env", docs[0]),
        )
        self.assertIn(
            {"name": "GIT_SYNC_SSH", "value": "true"}, jmespath.search("spec.initContainers[0].env", docs[0])
        )
        self.assertIn(
            {"name": "GIT_KNOWN_HOSTS", "value": "false"},
            jmespath.search("spec.initContainers[0].env", docs[0]),
        )
        self.assertIn(
            {"name": "git-sync-ssh-key", "secret": {"secretName": "ssh-secret", "defaultMode": 288}},
            jmespath.search("spec.volumes", docs[0]),
        )

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
        )

        self.assertIn(
            {
                "name": "GIT_SYNC_USERNAME",
                "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_USERNAME"}},
            },
            jmespath.search("spec.initContainers[0].env", docs[0]),
        )
        self.assertIn(
            {
                "name": "GIT_SYNC_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_PASSWORD"}},
            },
            jmespath.search("spec.initContainers[0].env", docs[0]),
        )

    def test_should_set_the_volume_claim_correctly_when_using_an_existing_claim(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": True, "existingClaim": "test-claim"}}},
            show_only=["templates/pod-template-file.yaml"],
        )

        self.assertIn(
            {"name": "dags", "persistentVolumeClaim": {"claimName": "test-claim"}},
            jmespath.search("spec.volumes", docs[0]),
        )

    def test_should_set_a_custom_image_in_pod_template(self):
        docs = render_chart(
            values={"images": {"pod_template": {"repository": "dummy_image", "tag": "latest"}}},
            show_only=["templates/pod-template-file.yaml"],
        )

        self.assertRegex(docs[0]["kind"], "Pod")
        self.assertEqual("dummy_image:latest", jmespath.search("spec.containers[0].image", docs[0]))
        self.assertEqual("base", jmespath.search("spec.containers[0].name", docs[0]))

    def test_mount_airflow_cfg(self):
        docs = render_chart(
            values={},
            show_only=["templates/pod-template-file.yaml"],
        )

        self.assertRegex(docs[0]["kind"], "Pod")
        self.assertDictEqual(
            {'configMap': {'name': 'RELEASE-NAME-airflow-config'}, 'name': 'config'},
            jmespath.search("spec.volumes[1]", docs[0]),
        )
        self.assertDictEqual(
            {
                'name': 'config',
                'mountPath': '/opt/airflow/airflow.cfg',
                'subPath': 'airflow.cfg',
                'readOnly': True,
            },
            jmespath.search("spec.containers[0].volumeMounts[1]", docs[0]),
        )

    def test_should_create_valid_affinity_and_node_selector(self):
        docs = render_chart(
            values={
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
        )

        self.assertRegex(docs[0]["kind"], "Pod")
        self.assertEqual(
            "foo",
            jmespath.search(
                "spec.affinity.nodeAffinity."
                "requiredDuringSchedulingIgnoredDuringExecution."
                "nodeSelectorTerms[0]."
                "matchExpressions[0]."
                "key",
                docs[0],
            ),
        )
        self.assertEqual(
            "ssd",
            jmespath.search(
                "spec.nodeSelector.diskType",
                docs[0],
            ),
        )
        self.assertEqual(
            "dynamic-pods",
            jmespath.search(
                "spec.tolerations[0].key",
                docs[0],
            ),
        )
