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

import yaml
from kubernetes.client import models as k8s

from .helm_template_generator import render_chart, render_k8s_object

OBJECT_COUNT_IN_BASIC_DEPLOYMENT = 22

git_sync_basic = """
dags:
  gitSync:
    enabled: true
"""

git_sync_existing_claim = """
dags:
 persistence:
  enabled: true
  existingClaim: test-claim
"""

git_sync_ssh_params = """
dags:
 gitSync:
  enabled: true
  containerName: git-sync-test
  sshKeySecret: ssh-secret
  knownHosts: ~
  branch: test-branch
"""

git_sync_username = """
dags:
 gitSync:
  enabled: true
  credentialsSecret: user-pass-secret
  sshKeySecret: ~
"""

git_sync_container_spec = """
images:
  gitSync:
    repository: test-registry/test-repo
    tag: test-tag
dags:
  gitSync:
    enabled: true
    containerName: git-sync-test
    wait: 66
    maxFailures: 70
    subPath: "path1/path2"
    dest: "test-dest"
    root: "/git-root"
    rev: HEAD
    depth: 1
    repo: https://github.com/apache/airflow.git
    branch: test-branch
    sshKeySecret: ~
    credentialsSecret: ~
    knownHosts: ~
  persistence:
    enabled: true
"""


class TestGitSyncScheduler(unittest.TestCase):
    def test_basic(self):
        helm_settings = yaml.safe_load(git_sync_basic)
        res = render_chart(
            'GIT-SYNC', helm_settings, show_only=["templates/scheduler/scheduler-deployment.yaml"]
        )
        dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
        self.assertEqual("dags", dep.spec.template.spec.volumes[1].name)

    def test_git_container_spec(self):
        helm_settings = yaml.safe_load(git_sync_container_spec)
        res = render_chart(
            'GIT-SYNC', helm_settings, show_only=["templates/scheduler/scheduler-deployment.yaml"]
        )
        dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
        git_sync_container = dep.spec.template.spec.containers[1]
        self.assertEqual(git_sync_container.image, "test-registry/test-repo:test-tag")
        self.assertEqual(git_sync_container.name, "git-sync-test")
        self.assertEqual(git_sync_container.security_context.run_as_user, 65533)
        env_dict = [e.to_dict() for e in git_sync_container.env]
        self.assertEqual(
            env_dict,
            [
                {'name': 'GIT_SYNC_REV', 'value': 'HEAD', 'value_from': None},
                {'name': 'GIT_SYNC_BRANCH', 'value': 'test-branch', 'value_from': None},
                {
                    'name': 'GIT_SYNC_REPO',
                    'value': 'https://github.com/apache/airflow.git',
                    'value_from': None,
                },
                {'name': 'GIT_SYNC_DEPTH', 'value': '1', 'value_from': None},
                {'name': 'GIT_SYNC_ROOT', 'value': '/git-root', 'value_from': None},
                {'name': 'GIT_SYNC_DEST', 'value': 'test-dest', 'value_from': None},
                {'name': 'GIT_SYNC_ADD_USER', 'value': 'true', 'value_from': None},
                {'name': 'GIT_SYNC_WAIT', 'value': '66', 'value_from': None},
                {'name': 'GIT_SYNC_MAX_SYNC_FAILURES', 'value': '70', 'value_from': None},
            ],
        )

        self.assertEqual(
            git_sync_container.volume_mounts, [k8s.V1VolumeMount(name="dags", mount_path="/git-root")]
        )

    def test_ssh_params_added(self):
        helm_settings = yaml.safe_load(git_sync_ssh_params)
        res = render_chart(
            'GIT-SYNC', helm_settings, show_only=["templates/scheduler/scheduler-deployment.yaml"]
        )
        dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
        git_sync_container = dep.spec.template.spec.containers[1]
        env_dict = [e.to_dict() for e in git_sync_container.env]
        self.assertEqual(
            env_dict,
            [
                {'name': 'GIT_SSH_KEY_FILE', 'value': '/etc/git-secret/ssh', 'value_from': None},
                {'name': 'GIT_SYNC_SSH', 'value': 'true', 'value_from': None},
                {'name': 'GIT_KNOWN_HOSTS', 'value': 'false', 'value_from': None},
                {'name': 'GIT_SYNC_REV', 'value': 'HEAD', 'value_from': None},
                {'name': 'GIT_SYNC_BRANCH', 'value': 'test-branch', 'value_from': None},
                {
                    'name': 'GIT_SYNC_REPO',
                    'value': 'https://github.com/apache/airflow.git',
                    'value_from': None,
                },
                {'name': 'GIT_SYNC_DEPTH', 'value': '1', 'value_from': None},
                {'name': 'GIT_SYNC_ROOT', 'value': '/git', 'value_from': None},
                {'name': 'GIT_SYNC_DEST', 'value': 'repo', 'value_from': None},
                {'name': 'GIT_SYNC_ADD_USER', 'value': 'true', 'value_from': None},
                {'name': 'GIT_SYNC_WAIT', 'value': '60', 'value_from': None},
                {'name': 'GIT_SYNC_MAX_SYNC_FAILURES', 'value': '0', 'value_from': None},
            ],
        )

    def test_adds_git_username(self):
        helm_settings = yaml.safe_load(git_sync_username)
        res = render_chart(
            'GIT-SYNC', helm_settings, show_only=["templates/scheduler/scheduler-deployment.yaml"]
        )
        dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
        git_sync_container = dep.spec.template.spec.containers[1]
        env_dict = [e.to_dict() for e in git_sync_container.env]
        self.assertEqual(
            env_dict,
            [
                {
                    'name': 'GIT_SYNC_USERNAME',
                    'value': None,
                    'value_from': {
                        'config_map_key_ref': None,
                        'field_ref': None,
                        'resource_field_ref': None,
                        'secret_key_ref': {
                            'key': 'GIT_SYNC_USERNAME',
                            'name': 'user-pass-secret',
                            'optional': None,
                        },
                    },
                },
                {
                    'name': 'GIT_SYNC_PASSWORD',
                    'value': None,
                    'value_from': {
                        'config_map_key_ref': None,
                        'field_ref': None,
                        'resource_field_ref': None,
                        'secret_key_ref': {
                            'key': 'GIT_SYNC_PASSWORD',
                            'name': 'user-pass-secret',
                            'optional': None,
                        },
                    },
                },
                {'name': 'GIT_SYNC_REV', 'value': 'HEAD', 'value_from': None},
                {'name': 'GIT_SYNC_BRANCH', 'value': 'v1-10-stable', 'value_from': None},
                {
                    'name': 'GIT_SYNC_REPO',
                    'value': 'https://github.com/apache/airflow.git',
                    'value_from': None,
                },
                {'name': 'GIT_SYNC_DEPTH', 'value': '1', 'value_from': None},
                {'name': 'GIT_SYNC_ROOT', 'value': '/git', 'value_from': None},
                {'name': 'GIT_SYNC_DEST', 'value': 'repo', 'value_from': None},
                {'name': 'GIT_SYNC_ADD_USER', 'value': 'true', 'value_from': None},
                {'name': 'GIT_SYNC_WAIT', 'value': '60', 'value_from': None},
                {'name': 'GIT_SYNC_MAX_SYNC_FAILURES', 'value': '0', 'value_from': None},
            ],
        )

    def test_set_volume_claim_to_existing_claim(self):
        helm_settings = yaml.safe_load(git_sync_existing_claim)
        res = render_chart(
            'GIT-SYNC', helm_settings, show_only=["templates/scheduler/scheduler-deployment.yaml"]
        )
        dep: k8s.V1Deployment = render_k8s_object(res[0], k8s.V1Deployment)
        volume_map = {vol.name: vol for vol in dep.spec.template.spec.volumes}
        dag_volume = volume_map['dags']
        self.assertEqual(
            dag_volume,
            k8s.V1Volume(
                name="dags",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-claim'),
            ),
        )
