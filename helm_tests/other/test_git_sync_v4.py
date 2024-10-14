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
import pytest

from tests.charts.helm_template_generator import HelmFailedError, render_chart


class TestGitSyncV4:
    """
    Test git sync v4.1+

    See changes in: https://github.com/kubernetes/git-sync/blob/v4.1.0/v3-to-v4.md
    """

    @pytest.mark.parametrize(
        "invalid_config,value",
        [
            ("rev", "HEAD"),
            ("branch", "test-branch"),
            ("wait", True),
        ],
    )
    def test_git_sync_v3_configs_not_present(self, invalid_config, value):
        with pytest.raises(HelmFailedError, match=f"Additional property {invalid_config} is not allowed"):
            render_chart(
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
                            invalid_config: value,
                            "enabled": True,
                            "containerName": "git-sync-test",
                            "period": "66s",
                            "maxFailures": 70,
                            "subPath": "path1/path2",
                            "ref": "test-branch",
                            "depth": 1,
                            "repo": "https://github.com/apache/airflow.git",
                            "sshKeySecret": None,
                            "credentialsSecret": None,
                            "knownHosts": None,
                            "envFrom": "- secretRef:\n    name: 'proxy-config'\n",
                        },
                        "persistence": {"enabled": True},
                    },
                },
                show_only=["templates/scheduler/scheduler-deployment.yaml"],
            )

    def test_env_vars_for_v4(self):
        """
        Ensures all env vars start with GITSYNC_ and not GIT_SYNC,
        see https://github.com/kubernetes/git-sync/blob/v4.1.0/v3-to-v4.md#env-vars
        """
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
                        "period": "66s",
                        "maxFailures": 70,
                        "subPath": "path1/path2",
                        "ref": "test-branch",
                        "depth": 1,
                        "repo": "https://github.com/apache/airflow.git",
                        "sshKeySecret": "ssh-secret",
                        "credentialsSecret": "user-pass-secret",
                        "knownHosts": "my-known-hosts",
                        "envFrom": "- secretRef:\n    name: 'proxy-config'\n",
                    },
                    "persistence": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert all(
            e["name"].startswith("GITSYNC_")
            for e in jmespath.search("spec.template.spec.containers[1].env[]", docs[0])
        )
        assert all(
            not e["name"].startswith("GIT_SYNC_")
            for e in jmespath.search("spec.template.spec.containers[1].env[]", docs[0])
        )

    def test_ssh_flag_not_present(self):
        """
        In v4, `--ssh` has no effect, it is determined by the repo.
        In v4.0 it is required but has no effect
        In v4.1+ it is not required.

        We support v4.1+, so ensuring it is not present from old versions

        See: https://github.com/kubernetes/git-sync/blob/v4.1.0/v3-to-v4.md#ssh---ssh-is-optional-after-v400
        """
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
                        "period": "66s",
                        "maxFailures": 70,
                        "subPath": "path1/path2",
                        "ref": "test-branch",
                        "depth": 1,
                        "repo": "https://github.com/apache/airflow.git",
                        "sshKeySecret": "ssh-secret",
                        "credentialsSecret": None,
                        "knownHosts": "my-known-hosts",
                        "envFrom": "- secretRef:\n    name: 'proxy-config'\n",
                    },
                    "persistence": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert all(
            e["name"] != "GITSYNC_SSH"
            for e in jmespath.search("spec.template.spec.containers[1].env[]", docs[0])
        )
        assert all(
            e["name"] != "GIT_SYNC_SSH"
            for e in jmespath.search("spec.template.spec.containers[1].env[]", docs[0])
        )
