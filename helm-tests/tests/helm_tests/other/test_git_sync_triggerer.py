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
from chart_utils.helm_template_generator import render_chart


class TestGitSyncTriggerer:
    """Test git sync triggerer."""

    # def test_validate_sshkeysecret_not_added_when_persistence_is_enabled(self):
    #     docs = render_chart(
    #         values={
    #             "dags": {
    #                 "gitSync": {
    #                     "enabled": True,
    #                     "components": {"triggerer": True},
    #                     "containerName": "git-sync-test",
    #                     "sshKeySecret": "ssh-secret",
    #                     "knownHosts": None,
    #                     "branch": "test-branch",
    #                 },
    #                 "persistence": {"enabled": True},
    #             }
    #         },
    #         show_only=["templates/triggerer/triggerer-deployment.yaml"],
    #     )
    #     assert "git-sync-ssh-key" not in jmespath.search("spec.template.spec.volumes[].name", docs[0])

    def test_validate_if_ssh_params_are_added_with_git_ssh_key(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "components": {"triggerer": True},
                        "sshKey": "dummy-ssh-key",
                    }
                }
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
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

    def test_liveliness_and_readiness_probes_are_configurable(self):
        livenessProbe = {
            "failureThreshold": 10,
            "exec": {"command": ["/bin/true"]},
            "initialDelaySeconds": 0,
            "periodSeconds": 1,
            "successThreshold": 1,
            "timeoutSeconds": 5,
        }
        readinessProbe = {
            "failureThreshold": 10,
            "exec": {"command": ["/bin/true"]},
            "initialDelaySeconds": 0,
            "periodSeconds": 1,
            "successThreshold": 1,
            "timeoutSeconds": 5,
        }
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "livenessProbe": livenessProbe,
                        "readinessProbe": readinessProbe,
                    },
                }
            },
            show_only=["templates/triggerer/triggerer-deployment.yaml"],
        )
        container_search_result = jmespath.search(
            "spec.template.spec.containers[?name == 'git-sync']", docs[0]
        )
        init_container_search_result = jmespath.search(
            "spec.template.spec.initContainers[?name == 'git-sync-init']", docs[0]
        )
        assert "livenessProbe" in container_search_result[0]
        assert "readinessProbe" in container_search_result[0]
        assert "readinessProbe" not in init_container_search_result[0]
        assert "readinessProbe" not in init_container_search_result[0]
        assert livenessProbe == container_search_result[0]["livenessProbe"]
        assert readinessProbe == container_search_result[0]["readinessProbe"]
