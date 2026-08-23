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


class TestGitSyncSchedulerTest:
    """Test git sync scheduler. This is ignored when Airflow >=3 or a separate dag processor is used."""

    def test_should_add_dags_volume(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {"gitSync": {"enabled": True}},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        # check that there is a volume and git-sync and scheduler container mount it
        assert len(jmespath.search("spec.template.spec.volumes[?name=='dags']", docs[0])) > 0
        assert (
            len(
                jmespath.search(
                    "(spec.template.spec.containers[?name=='scheduler'].volumeMounts[])[?name=='dags']",
                    docs[0],
                )
            )
            > 0
        )
        assert (
            len(
                jmespath.search(
                    "(spec.template.spec.containers[?name=='git-sync'].volumeMounts[])[?name=='dags']",
                    docs[0],
                )
            )
            > 0
        )

    def test_validate_the_git_sync_container_spec(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
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
                    },
                    "persistence": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[1]", docs[0]) == {
            "name": "git-sync-test",
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
                {"name": "GIT_SYNC_HTTP_BIND", "value": ":1234"},
                {"name": "GITSYNC_HTTP_BIND", "value": ":1234"},
            ],
            "volumeMounts": [{"mountPath": "/git", "name": "dags"}],
            "resources": {},
            "startupProbe": {
                "failureThreshold": 10,
                "httpGet": {"path": "/", "port": 1234},
                "initialDelaySeconds": 0,
                "periodSeconds": 5,
                "timeoutSeconds": 1,
            },
        }

    def test_validate_the_git_sync_container_spec_if_wait_specified(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
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
                    },
                    "persistence": {"enabled": True},
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[1]", docs[0]) == {
            "name": "git-sync-test",
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
                {"name": "GIT_SYNC_WAIT", "value": "66"},
                {"name": "GIT_SYNC_HTTP_BIND", "value": ":1234"},
                {"name": "GITSYNC_HTTP_BIND", "value": ":1234"},
            ],
            "volumeMounts": [{"mountPath": "/git", "name": "dags"}],
            "resources": {},
            "startupProbe": {
                "failureThreshold": 10,
                "httpGet": {"path": "/", "port": 1234},
                "initialDelaySeconds": 0,
                "periodSeconds": 5,
                "timeoutSeconds": 1,
            },
        }

    def test_validate_if_ssh_params_are_added(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "sshKeySecret": "ssh-secret",
                        "knownHosts": None,
                        "branch": "test-branch",
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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
            "secret": {"secretName": "ssh-secret", "defaultMode": 288},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

    def test_validate_if_ssh_params_are_added_with_git_ssh_key(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "sshKey": "dummy-ssh-key",
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
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
        assert {
            "name": "git-sync-ssh-key",
            "secret": {"secretName": "release-name-ssh-secret", "defaultMode": 288},
        } in jmespath.search("spec.template.spec.volumes", docs[0])

    def test_validate_sshkeysecret_not_added_when_persistence_is_enabled(self):
        docs = render_chart(
            values={
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "containerName": "git-sync-test",
                        "sshKeySecret": "ssh-secret",
                        "knownHosts": None,
                        "branch": "test-branch",
                    },
                    "persistence": {"enabled": True},
                }
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert "git-sync-ssh-key" not in jmespath.search("spec.template.spec.volumes[].name", docs[0])

    def test_should_set_username_and_pass_env_variables(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "credentialsSecret": "user-pass-secret",
                        "sshKeySecret": None,
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {
            "name": "GIT_SYNC_USERNAME",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_USERNAME"}},
        } in jmespath.search("spec.template.spec.containers[1].env", docs[0])
        assert {
            "name": "GIT_SYNC_PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GIT_SYNC_PASSWORD"}},
        } in jmespath.search("spec.template.spec.containers[1].env", docs[0])

        # Testing git-sync v4
        assert {
            "name": "GITSYNC_USERNAME",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GITSYNC_USERNAME"}},
        } in jmespath.search("spec.template.spec.containers[1].env", docs[0])
        assert {
            "name": "GITSYNC_PASSWORD",
            "valueFrom": {"secretKeyRef": {"name": "user-pass-secret", "key": "GITSYNC_PASSWORD"}},
        } in jmespath.search("spec.template.spec.containers[1].env", docs[0])

    def test_should_set_the_volume_claim_correctly_when_using_an_existing_claim(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {"persistence": {"enabled": True, "existingClaim": "test-claim"}},
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "dags", "persistentVolumeClaim": {"claimName": "test-claim"}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

    def test_should_add_extra_volume_and_extra_volume_mount(self):
        docs = render_chart(
            values={
                "executor": "CeleryExecutor",
                "scheduler": {
                    "extraVolumes": [{"name": "test-volume-{{ .Chart.Name }}", "emptyDir": {}}],
                    "extraVolumeMounts": [
                        {"name": "test-volume-{{ .Chart.Name }}", "mountPath": "/opt/test"}
                    ],
                },
                "dags": {
                    "gitSync": {
                        "enabled": True,
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "test-volume-airflow", "emptyDir": {}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )
        assert {"name": "test-volume-airflow", "mountPath": "/opt/test"} in jmespath.search(
            "spec.template.spec.containers[0].volumeMounts", docs[0]
        )

    @pytest.mark.parametrize(
        ("executor", "git_sync_mount"), [("CeleryExecutor", False), ("LocalExecutor", True)]
    )
    def test_extra_volume_and_git_sync_extra_volume_mount(self, executor, git_sync_mount):
        docs = render_chart(
            values={
                "executor": executor,
                "scheduler": {
                    "extraVolumes": [{"name": "test-volume-{{ .Values.executor }}", "emptyDir": {}}],
                },
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "extraVolumeMounts": [
                            {"mountPath": "/opt/test", "name": "test-volume-{{ .Values.executor }}"}
                        ],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": f"test-volume-{executor}", "emptyDir": {}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )
        # Git sync is not needed on the scheduler when using CeleryExecutor,
        # so the extra volume mount should not be added to the git-sync container
        if git_sync_mount:
            assert {"mountPath": "/git", "name": "dags"} in jmespath.search(
                "spec.template.spec.containers[1].volumeMounts", docs[0]
            )
            assert {"name": f"test-volume-{executor}", "mountPath": "/opt/test"} in jmespath.search(
                "spec.template.spec.containers[1].volumeMounts", docs[0]
            )
        else:
            assert {"mountPath": "/git", "name": "dags"} not in jmespath.search(
                "spec.template.spec.containers[1].volumeMounts", docs[0]
            )

    def test_should_add_env(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "env": [{"name": "FOO", "value": "bar"}],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert {"name": "FOO", "value": "bar"} in jmespath.search(
            "spec.template.spec.containers[1].env", docs[0]
        )

    def test_env_override_replaces_hardcoded_value(self):
        """User-provided env should replace the hardcoded default without duplicates."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "repo": "https://default.example.com/repo.git",
                        "env": [
                            {"name": "GIT_SYNC_REPO", "value": "https://override.example.com/repo.git"},
                        ],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        git_sync_repo_entries = [e for e in env if e["name"] == "GIT_SYNC_REPO"]
        assert len(git_sync_repo_entries) == 1, "Expected exactly one GIT_SYNC_REPO entry, no duplicates"
        assert git_sync_repo_entries[0]["value"] == "https://override.example.com/repo.git"

    def test_env_override_with_secret_ref(self):
        """User-provided env with valueFrom.secretKeyRef should replace the hardcoded default."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "repo": "https://default.example.com/repo.git",
                        "env": [
                            {
                                "name": "GIT_SYNC_REPO",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": "my-git-secret",
                                        "key": "repo-url",
                                    }
                                },
                            },
                        ],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        git_sync_repo_entries = [e for e in env if e["name"] == "GIT_SYNC_REPO"]
        assert len(git_sync_repo_entries) == 1, "Expected exactly one GIT_SYNC_REPO entry, no duplicates"
        assert git_sync_repo_entries[0]["valueFrom"]["secretKeyRef"]["name"] == "my-git-secret"
        assert git_sync_repo_entries[0]["valueFrom"]["secretKeyRef"]["key"] == "repo-url"

    def test_env_override_multiple_vars_no_duplicates(self):
        """Multiple user overrides should each replace their hardcoded counterpart."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "repo": "https://default.example.com/repo.git",
                        "depth": 1,
                        "env": [
                            {"name": "GIT_SYNC_REPO", "value": "https://override.example.com/repo.git"},
                            {"name": "GITSYNC_REPO", "value": "https://override.example.com/repo.git"},
                            {"name": "GIT_SYNC_DEPTH", "value": "0"},
                        ],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        for var_name in ["GIT_SYNC_REPO", "GITSYNC_REPO", "GIT_SYNC_DEPTH"]:
            entries = [e for e in env if e["name"] == var_name]
            assert len(entries) == 1, f"Expected exactly one {var_name} entry, got {len(entries)}"

    def test_env_override_does_not_affect_other_vars(self):
        """Overriding one env var should not remove other hardcoded env vars."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "env": [
                            {"name": "GIT_SYNC_REPO", "value": "https://override.example.com/repo.git"},
                        ],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        env_names = [e["name"] for e in env]
        # These should still be present since they were not overridden
        assert "GITSYNC_REPO" in env_names
        assert "GIT_SYNC_REV" in env_names
        assert "GIT_SYNC_DEPTH" in env_names
        assert "GIT_SYNC_ROOT" in env_names

    def test_env_vars_secret_loads_from_secret_key_ref(self):
        """When envVarsSecret is set, main env vars should use valueFrom.secretKeyRef."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "envVarsSecret": "git-sync-env-secret",
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        repo_entries = [e for e in env if e["name"] == "GIT_SYNC_REPO"]
        assert len(repo_entries) == 1
        assert repo_entries[0]["valueFrom"]["secretKeyRef"]["name"] == "git-sync-env-secret"
        assert repo_entries[0]["valueFrom"]["secretKeyRef"]["key"] == "GIT_SYNC_REPO"
        assert repo_entries[0]["valueFrom"]["secretKeyRef"]["optional"] is True

        # GITSYNC_REPO should also use the secret
        gitsync_repo = [e for e in env if e["name"] == "GITSYNC_REPO"]
        assert len(gitsync_repo) == 1
        assert gitsync_repo[0]["valueFrom"]["secretKeyRef"]["name"] == "git-sync-env-secret"

        # GIT_SYNC_REV should also use the secret
        rev_entries = [e for e in env if e["name"] == "GIT_SYNC_REV"]
        assert len(rev_entries) == 1
        assert rev_entries[0]["valueFrom"]["secretKeyRef"]["name"] == "git-sync-env-secret"

    def test_env_vars_secret_does_not_affect_constants(self):
        """Constants like GIT_SYNC_ROOT should keep their hardcoded values even with envVarsSecret."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "envVarsSecret": "git-sync-env-secret",
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        root_entries = [e for e in env if e["name"] == "GIT_SYNC_ROOT"]
        assert len(root_entries) == 1
        assert root_entries[0]["value"] == "/git"

        dest_entries = [e for e in env if e["name"] == "GIT_SYNC_DEST"]
        assert len(dest_entries) == 1
        assert dest_entries[0]["value"] == "repo"

    def test_env_override_takes_precedence_over_env_vars_secret(self):
        """dags.gitSync.env should override envVarsSecret (no duplicates)."""
        docs = render_chart(
            values={
                "airflowVersion": "2.11.0",
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "envVarsSecret": "git-sync-env-secret",
                        "env": [
                            {"name": "GIT_SYNC_REPO", "value": "https://explicit.example.com/repo.git"},
                        ],
                    }
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        env = jmespath.search("spec.template.spec.containers[1].env", docs[0])
        repo_entries = [e for e in env if e["name"] == "GIT_SYNC_REPO"]
        assert len(repo_entries) == 1, "Expected exactly one GIT_SYNC_REPO entry"
        assert repo_entries[0]["value"] == "https://explicit.example.com/repo.git"

    def test_resources_are_configurable(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
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
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[1].resources.limits.memory", docs[0]) == "128Mi"
        assert (
            jmespath.search("spec.template.spec.containers[1].resources.requests.memory", docs[0]) == "169Mi"
        )
        assert jmespath.search("spec.template.spec.containers[1].resources.requests.cpu", docs[0]) == "300m"

    def test_liveness_probe_configuration(self):
        livenessProbe = {
            "failureThreshold": 10,
            "exec": {"command": ["/bin/true"]},
            "initialDelaySeconds": 0,
            "periodSeconds": 1,
            "successThreshold": 1,
            "timeoutSeconds": 5,
        }

        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "livenessProbe": livenessProbe,
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert livenessProbe == jmespath.search(
            "spec.template.spec.containers[?name=='git-sync'] | [0].livenessProbe", docs[0]
        )

    def test_readiness_probe_configuration(self):
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
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "readinessProbe": readinessProbe,
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'] | [0].readinessProbe", docs[0]
            )
            is None
        )

        assert (
            jmespath.search("spec.template.spec.containers[?name=='git-sync'] | [0].readinessProbe", docs[0])
            == readinessProbe
        )

    def test_readiness_probe_configuration_recommended(self):
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
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "recommendedProbeSetting": True,
                        "readinessProbe": readinessProbe,
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'] | [0].readinessProbe", docs[0]
            )
            is None
        )

        assert (
            jmespath.search("spec.template.spec.containers[?name=='git-sync'] | [0].readinessProbe", docs[0])
            is None
        )

    def test_liveness_probe_configuration_recommended(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "httpPort": 10,
                        "recommendedProbeSetting": True,
                        "livenessProbe": {
                            "enabled": True,
                            "timeoutSeconds": 11,
                            "initialDelaySeconds": 12,
                            "periodSeconds": 13,
                            "failureThreshold": 14,
                        },
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'] | [0].livenessProbe", docs[0]
            )
            is None
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='git-sync'] | [0].livenessProbe", docs[0]
        ) == {
            "httpGet": {"path": "/", "port": 10},
            "timeoutSeconds": 11,
            "initialDelaySeconds": 12,
            "periodSeconds": 13,
            "failureThreshold": 14,
        }

    def test_startup_probe_configuration(self):
        docs = render_chart(
            values={
                "executor": "LocalExecutor",  # needed to have git sync added to the scheduler
                "dags": {
                    "gitSync": {
                        "enabled": True,
                        "httpPort": 10,
                        "startupProbe": {
                            "enabled": True,
                            "timeoutSeconds": 11,
                            "initialDelaySeconds": 12,
                            "periodSeconds": 13,
                            "failureThreshold": 14,
                        },
                    },
                },
            },
            show_only=["templates/scheduler/scheduler-deployment.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.initContainers[?name=='git-sync-init'] | [0].startupProbe", docs[0]
            )
            is None
        )

        assert jmespath.search(
            "spec.template.spec.containers[?name=='git-sync'] | [0].startupProbe", docs[0]
        ) == {
            "httpGet": {"path": "/", "port": 10},
            "timeoutSeconds": 11,
            "initialDelaySeconds": 12,
            "periodSeconds": 13,
            "failureThreshold": 14,
        }
