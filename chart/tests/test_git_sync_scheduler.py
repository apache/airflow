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

import jmespath

from tests.helm_template_generator import render_chart


class GitSyncSchedulerTest(unittest.TestCase):
    def test_should_add_dags_volume(self):
        docs = render_chart(
            values={"dags": {"gitSync": {"enabled": True}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        self.assertEqual("dags", jmespath.search("spec.template.spec.volumes[1].name", docs[0]))

    def test_validate_the_git_sync_container_spec(self):
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
                    },
                    "persistence": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

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
                ],
                "volumeMounts": [{"mountPath": "/git-root", "name": "dags"}],
            },
            jmespath.search("spec.template.spec.containers[1]", docs[0]),
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        self.assertIn(
            {"name": "GIT_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"},
            jmespath.search("spec.template.spec.containers[1].env", docs[0]),
        )
        self.assertIn(
            {"name": "GIT_SYNC_SSH", "value": "true"},
            jmespath.search("spec.template.spec.containers[1].env", docs[0]),
        )
        self.assertIn(
            {"name": "GIT_KNOWN_HOSTS", "value": "false"},
            jmespath.search("spec.template.spec.containers[1].env", docs[0]),
        )
        self.assertIn(
            {"name": "git-sync-ssh-key", "secret": {"secretName": "ssh-secret", "defaultMode": 288}},
            jmespath.search("spec.template.spec.volumes", docs[0]),
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        self.assertIn(
            {
                "name": "GIT_SYNC_USERNAME",
                "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_USERNAME"}},
            },
            jmespath.search("spec.template.spec.containers[1].env", docs[0]),
        )
        self.assertIn(
            {
                "name": "GIT_SYNC_PASSWORD",
                "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_PASSWORD"}},
            },
            jmespath.search("spec.template.spec.containers[1].env", docs[0]),
        )

    def test_should_set_the_volume_claim_correctly_when_using_an_existing_claim(self):
        docs = render_chart(
            values={"dags": {"persistence": {"enabled": True, "existingClaim": "test-claim"}}},
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        self.assertIn(
            {"name": "dags", "persistentVolumeClaim": {"claimName": "test-claim"}},
            jmespath.search("spec.template.spec.volumes", docs[0]),
        )
