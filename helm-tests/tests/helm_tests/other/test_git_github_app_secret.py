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


class TestGitHubAppSecret:
    """Tests GitHub App secret functionality for git-sync authentication."""

    def test_github_app_secret_template_helper(self):
        """Test that the GitHub App secret helper template function works correctly."""
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "githubAppSecret": "my-github-app-secret",
                        "githubAppId": 123456,
                        "githubAppInstallationId": 789012,
                    },
                    "persistence": {"enabled": True},
                }
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # Verify that the GitHub App secret volume is created with correct name
        volumes = jmespath.search("spec.template.spec.volumes", docs[0])
        github_app_volume = None
        for volume in volumes:
            if volume.get("name") == "github-app-secret":
                github_app_volume = volume
                break
        
        assert github_app_volume is not None
        assert github_app_volume["secret"]["secretName"] == "my-github-app-secret"
        assert github_app_volume["secret"]["defaultMode"] == 288

    def test_github_app_volume_mount(self):
        """Test that GitHub App secret is mounted correctly in git-sync container."""
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "githubAppSecret": "my-github-app-secret",
                        "githubAppId": 123456,
                        "githubAppInstallationId": 789012,
                    },
                    "persistence": {"enabled": True},
                }
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # Check that the GitHub App secret is mounted in the git-sync container
        git_sync_container = None
        containers = jmespath.search("spec.template.spec.containers", docs[0])
        for container in containers:
            if container.get("name") == "git-sync":
                git_sync_container = container
                break
        
        assert git_sync_container is not None
        volume_mounts = git_sync_container.get("volumeMounts", [])
        github_app_mount = None
        for mount in volume_mounts:
            if mount.get("name") == "github-app-secret":
                github_app_mount = mount
                break
        
        assert github_app_mount is not None
        assert github_app_mount["mountPath"] == "/etc/git-secret/github-app"
        assert github_app_mount["readOnly"] is True
        assert github_app_mount["subPath"] == "githubAppPrivateKey"

    def test_github_app_secret_not_created_when_disabled(self):
        """Test that GitHub App secret volume is not created when GitHub App authentication is disabled."""
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        # No GitHub App configuration
                    },
                    "persistence": {"enabled": True},
                }
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # Verify that no GitHub App secret volume is created
        volumes = jmespath.search("spec.template.spec.volumes", docs[0]) or []
        github_app_volumes = [v for v in volumes if v.get("name") == "github-app-secret"]
        assert len(github_app_volumes) == 0 