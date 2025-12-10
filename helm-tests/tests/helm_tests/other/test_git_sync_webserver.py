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
from chart_utils.helm_template_generator import render_chart


class TestGitSyncWebserver:
    """Test git sync webserver."""

    def test_should_add_dags_volume_to_the_webserver_if_git_sync_and_persistence_is_enabled(self):
        docs = render_chart(
            values={
                "airflowVersion": "1.10.14",
                "dags": {"gitSync": {"enabled": True, "components": {"webserver": True}}, "persistence": {"enabled": True}},
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1].name", docs[0]) == "dags"

    def test_should_add_dags_volume_to_the_webserver_if_git_sync_is_enabled_and_persistence_is_disabled(self):
        docs = render_chart(
            values={
                "airflowVersion": "1.10.14",
                "dags": {"gitSync": {"enabled": True, "components": {"webserver": True}}, "persistence": {"enabled": False}},
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[1].name", docs[0]) == "dags"

    def test_should_have_service_account_defined(self):
        docs = render_chart(
            values={
                "airflowVersion": "2.10.0",
                "dags": {
                    "gitSync": {"enabled": True, "components": {"webserver": True}},
                    "persistence": {"enabled": True}
                }
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[1].name", docs[0]) == "git-sync"
        assert (
            jmespath.search("spec.template.spec.serviceAccountName", docs[0])
            == "release-name-airflow-webserver"
        )

    # @pytest.mark.parametrize(
    #     ("airflow_version", "exclude_webserver"),
    #     [
    #         ("2.0.0", True),
    #         ("2.0.2", True),
    #         ("1.10.14", False),
    #         ("1.9.0", False),
    #         ("2.1.0", True),
    #     ],
    # )
    # def test_git_sync_with_different_airflow_versions(self, airflow_version, exclude_webserver):
    #     """If Airflow >= 2.0.0 - git sync related containers, volume mounts & volumes are not created."""
    #     docs = render_chart(
    #         values={
    #             "airflowVersion": airflow_version,
    #             "dags": {
    #                 "gitSync": {
    #                     "enabled": True,
    #                     "components": {"webserver": True},
    #                 },
    #                 "persistence": {"enabled": False},
    #             },
    #         },
    #         show_only=["templates/webserver/webserver-deployment.yaml"],
    #     )
    #
    #     containers_names = [
    #         container["name"] for container in jmespath.search("spec.template.spec.containers", docs[0])
    #     ]
    #
    #     volume_mount_names = [
    #         vm["name"] for vm in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
    #     ]
    #
    #     volume_names = [volume["name"] for volume in jmespath.search("spec.template.spec.volumes", docs[0])]
    #
    #     if exclude_webserver:
    #         assert "git-sync" not in containers_names
    #         assert "dags" not in volume_mount_names
    #         assert "dags" not in volume_names
    #     else:
    #         assert "git-sync" in containers_names
    #         assert "dags" in volume_mount_names
    #         assert "dags" in volume_names

    def test_should_add_env(self):
        docs = render_chart(
            values={
                "airflowVersion": "1.10.14",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "components": {"webserver": True},
                        "env": [{"name": "FOO", "value": "bar"}],
                    }
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {"name": "FOO", "value": "bar"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )

    def test_resources_are_configurable(self):
        docs = render_chart(
            values={
                "airflowVersion": "1.10.14",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "components": {"webserver": True},
                        "resources": {
                            "limits": {"cpu": "200m", "memory": "128Mi"},
                            "requests": {"cpu": "300m", "memory": "169Mi"},
                        },
                    },
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[1].resources.limits.memory", docs[0]) == "128Mi"
        assert (
            jmespath.search("spec.template.spec.containers[1].resources.requests.memory", docs[0]) == "169Mi"
        )
        assert jmespath.search("spec.template.spec.containers[1].resources.requests.cpu", docs[0]) == "300m"

    # def test_validate_sshkeysecret_not_added_when_persistence_is_enabled(self):
    #     docs = render_chart(
    #         values={
    #             "airflowVersion": "2.10.4",
    #             "dags": {
    #                 "gitSync": {
    #                     "enabled": True,
    #                     "components": {"webserver": True},
    #                     "containerName": "git-sync-test",
    #                     "sshKeySecret": "ssh-secret",
    #                     "knownHosts": None,
    #                     "branch": "test-branch",
    #                 },
    #                 "persistence": {"enabled": True},
    #             }
    #         },
    #         show_only=["templates/webserver/webserver-deployment.yaml"],
    #     )
    #     assert "git-sync-ssh-key" not in jmespath.search("spec.template.spec.volumes[].name", docs[0])

    def test_validate_if_ssh_params_are_added_with_git_ssh_key(self):
        docs = render_chart(
            values={
                "airflowVersion": "1.10.14",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "components": {"webserver": True},
                        "sshKey": "dummy-ssh-key",
                    },
                    "persistence": {"enabled": False},
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
        )

        assert {
            "name": "git-sync-ssh-key",
            "secret": {"secretName": "release-name-ssh-secret", "defaultMode": 288},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

    def test_liveliness_and_readiness_probes_are_configurable(self):
        """If Airflow < 2.0.0 - test git sync related containers, volume mounts & volumes are created."""
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
                "airflowVersion": "1.10.14",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "livenessProbe": livenessProbe,
                        "readinessProbe": readinessProbe,
                    },
                },
            },
            show_only=["templates/webserver/webserver-deployment.yaml"],
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
