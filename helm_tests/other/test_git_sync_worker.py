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

from tests.charts.helm_template_generator import render_chart


class TestGitSyncWorker:
    """Test git sync worker."""

    def test_should_add_dags_volume_to_the_worker_if_git_sync_and_persistence_is_enabled(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "dags": {"persistence": {"enabled": True}, "gitSync": {"enabled": True}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "config" == jmespath.search("spec.template.spec.volumes[0].name", docs[0])
        assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])

    def test_should_add_dags_volume_to_the_worker_if_git_sync_is_enabled_and_persistence_is_disabled(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "dags": {"gitSync": {"enabled": True}, "persistence": {"enabled": False}},
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "config" == jmespath.search("spec.template.spec.volumes[0].name", docs[0])
        assert "dags" == jmespath.search("spec.template.spec.volumes[1].name", docs[0])

    def test_should_add_git_sync_container_to_worker_if_persistence_is_not_enabled_but_git_sync_is(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "dags": {
                    "gitSync": {"enabled": True, "containerName": "git-sync"},
                    "persistence": {"enabled": False},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "git-sync" == jmespath.search("spec.template.spec.containers[1].name", docs[0])

    def test_should_not_add_sync_container_to_worker_if_git_sync_and_persistence_are_enabled(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "dags": {
                    "gitSync": {"enabled": True, "containerName": "git-sync"},
                    "persistence": {"enabled": True},
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "git-sync" != jmespath.search("spec.template.spec.containers[1].name", docs[0])

    def test_should_add_env(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "env": [{"name": "FOO", "value": "bar"}],
                    }
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "FOO", "value": "bar"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )

    def test_resources_are_configurable(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "resources": {
                            "limits": {"cpu": "200m", "memory": "128Mi"},
                            "requests": {"cpu": "300m", "memory": "169Mi"},
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert "128Mi" == jmespath.search("spec.template.spec.containers[1].resources.limits.memory", docs[0])
        assert "169Mi" == jmespath.search(
            "spec.template.spec.containers[1].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search("spec.template.spec.containers[1].resources.requests.cpu", docs[0])

    def test_validate_sshkeysecret_not_added_when_persistence_is_enabled(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "sshKeySecret": "ssh-secret",
                        "knownHosts": None,
                        "ref": "test-branch",
                    },
                    "persistence": {"enabled": True},
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert "git-sync-ssh-key" not in jmespath.search("spec.template.spec.volumes[].name", docs[0])

    def test_validate_if_ssh_params_are_added_with_git_ssh_key(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "sshKey": "dummy-ssh-key",
                    }
                }
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )

        assert {"name": "GITSYNC_SSH_KEY_FILE", "value": "/etc/git-secret/ssh"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {"name": "GITSYNC_SSH_KNOWN_HOSTS", "value": "false"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )
        assert {
            "name": "git-sync-ssh-key",
            "secret": {"secretName": "release-name-ssh-secret", "defaultMode": 288},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

    def test_container_lifecycle_hooks(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerLifecycleHooks": {
                            "postStart": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-c",
                                        "echo postStart handler > /git/message_start",
                                    ]
                                }
                            },
                            "preStop": {
                                "exec": {
                                    "command": ["/bin/sh", "-c", "echo preStop handler > /git/message_start"]
                                }
                            },
                        },
                    },
                },
            },
            show_only=["templates/workers/worker-deployment.yaml"],
        )
        assert {
            "postStart": {
                "exec": {"command": ["/bin/sh", "-c", "echo postStart handler > /git/message_start"]}
            },
            "preStop": {"exec": {"command": ["/bin/sh", "-c", "echo preStop handler > /git/message_start"]}},
        } == jmespath.search("spec.template.spec.containers[1].lifecycle", docs[0])
