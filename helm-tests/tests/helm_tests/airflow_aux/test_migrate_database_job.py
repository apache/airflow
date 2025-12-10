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
from helm_tests.utils import (
    _get_enabled_git_sync_test_params,
    _get_git_sync_test_params_for_no_containers,
    _test_git_sync_presence,
)


class TestMigrateDatabaseJob:
    """Tests migrate DB job."""

    def test_should_run_by_default(self):
        docs = render_chart(show_only=["templates/jobs/migrate-database-job.yaml"])
        assert docs[0]["kind"] == "Job"
        assert jmespath.search("spec.template.spec.containers[0].name", docs[0]) == "run-airflow-migrations"
        assert jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0]) == 50000

    @pytest.mark.parametrize(
        ("migrate_database_job_enabled", "created"),
        [
            (False, False),
            (True, True),
        ],
    )
    def test_enable_migrate_database_job(self, migrate_database_job_enabled, created):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {"enabled": migrate_database_job_enabled},
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert bool(docs) is created

    def test_should_support_annotations(self):
        docs = render_chart(
            values={"migrateDatabaseJob": {"annotations": {"foo": "bar"}, "jobAnnotations": {"fiz": "fuz"}}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        annotations = jmespath.search("spec.template.metadata.annotations", docs[0])
        assert "foo" in annotations
        assert annotations["foo"] == "bar"
        job_annotations = jmespath.search("metadata.annotations", docs[0])
        assert "fiz" in job_annotations
        assert job_annotations["fiz"] == "fuz"

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert jmespath.search("spec.template.metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_should_merge_common_labels_and_component_specific_labels(self):
        docs = render_chart(
            values={
                "labels": {"test_common_label": "test_common_label_value"},
                "migrateDatabaseJob": {
                    "labels": {"test_specific_label": "test_specific_label_value"},
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        assert "test_common_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_common_label"]
            == "test_common_label_value"
        )
        assert "test_specific_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.template.metadata.labels", docs[0])["test_specific_label"]
            == "test_specific_label_value"
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
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
                }
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "Job"
        assert (
            jmespath.search(
                "spec.template.spec.affinity.nodeAffinity."
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
                "spec.template.spec.nodeSelector.diskType",
                docs[0],
            )
            == "ssd"
        )
        assert (
            jmespath.search(
                "spec.template.spec.tolerations[0].key",
                docs[0],
            )
            == "dynamic-pods"
        )

    def test_scheduler_name(self):
        docs = render_chart(
            values={"schedulerName": "airflow-scheduler"},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert (
            jmespath.search(
                "spec.template.spec.schedulerName",
                docs[0],
            )
            == "airflow-scheduler"
        )

    @pytest.mark.parametrize(
        ("use_default_image", "expected_image"),
        [
            (True, "apache/airflow:2.1.0"),
            (False, "apache/airflow:user-image"),
        ],
    )
    def test_should_use_correct_image(self, use_default_image, expected_image):
        docs = render_chart(
            values={
                "defaultAirflowRepository": "apache/airflow",
                "defaultAirflowTag": "2.1.0",
                "images": {
                    "airflow": {
                        "repository": "apache/airflow",
                        "tag": "user-image",
                    },
                    "useDefaultImageForMigration": use_default_image,
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert expected_image == jmespath.search("spec.template.spec.containers[0].image", docs[0])

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "extraContainers": [
                        {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_add_extra_init_containers(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "extraInitContainers": [
                        {"name": "{{ .Chart.Name }}", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.initContainers[0]", docs[0]) == {
            "name": "airflow",
            "image": "test-registry/test-repo:test-tag",
        }

    def test_should_template_extra_containers(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "extraContainers": [{"name": "{{ .Release.Name }}-test-container"}],
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[-1]", docs[0]) == {
            "name": "release-name-test-container"
        }

    def test_set_resources(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "resources": {
                        "requests": {
                            "cpu": "1000mi",
                            "memory": "512Mi",
                        },
                        "limits": {
                            "cpu": "1000mi",
                            "memory": "512Mi",
                        },
                    },
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {
            "requests": {
                "cpu": "1000mi",
                "memory": "512Mi",
            },
            "limits": {
                "cpu": "1000mi",
                "memory": "512Mi",
            },
        }

    def test_should_disable_default_helm_hooks(self):
        docs = render_chart(
            values={"migrateDatabaseJob": {"useHelmHooks": False}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations is None

    def test_should_set_correct_helm_hooks_weight(self):
        docs = render_chart(
            show_only=[
                "templates/jobs/migrate-database-job.yaml",
            ],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["helm.sh/hook-weight"] == "1"

    def test_should_add_extra_volumes(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "extraVolumes": [{"name": "myvolume-{{ .Chart.Name }}", "emptyDir": {}}],
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[-1]", docs[0]) == {
            "name": "myvolume-airflow",
            "emptyDir": {},
        }

    def test_should_add_extra_volume_mounts(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "extraVolumeMounts": [{"name": "foobar-{{ .Chart.Name }}", "mountPath": "foo/bar"}],
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].volumeMounts[-1]", docs[0]) == {
            "name": "foobar-airflow",
            "mountPath": "foo/bar",
        }

    def test_should_add_global_volume_and_global_volume_mount(self):
        docs = render_chart(
            values={
                "volumes": [{"name": "myvolume", "emptyDir": {}}],
                "volumeMounts": [{"name": "foobar", "mountPath": "foo/bar"}],
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.volumes[-1]", docs[0]) == {
            "name": "myvolume",
            "emptyDir": {},
        }
        assert jmespath.search("spec.template.spec.containers[0].volumeMounts[-1]", docs[0]) == {
            "name": "foobar",
            "mountPath": "foo/bar",
        }

    def test_job_ttl_after_finished(self):
        docs = render_chart(
            values={"migrateDatabaseJob": {"ttlSecondsAfterFinished": 1}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        ttl = jmespath.search("spec.ttlSecondsAfterFinished", docs[0])
        assert ttl == 1

    def test_job_ttl_after_finished_zero(self):
        docs = render_chart(
            values={"migrateDatabaseJob": {"ttlSecondsAfterFinished": 0}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        ttl = jmespath.search("spec.ttlSecondsAfterFinished", docs[0])
        assert ttl == 0

    def test_job_ttl_after_finished_nil(self):
        docs = render_chart(
            values={"migrateDatabaseJob": {"ttlSecondsAfterFinished": None}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        spec = jmespath.search("spec", docs[0])
        assert "ttlSecondsAfterFinished" not in spec

    @pytest.mark.parametrize(
        ("airflow_version", "expected_arg"),
        [
            ("1.10.14", "airflow upgradedb"),
            ("2.0.2", "airflow db upgrade"),
            ("2.7.1", "airflow db migrate"),
        ],
    )
    def test_default_command_and_args_airflow_version(self, airflow_version, expected_arg):
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert [
            "bash",
            "-c",
            f"exec \\\n{expected_arg}",
        ] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"migrateDatabaseJob": {"command": command, "args": args}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {"command": ["{{ .Release.Name }}"], "args": ["{{ .Release.Service }}"]}
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) == ["release-name"]
        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": None}, show_only=["templates/jobs/migrate-database-job.yaml"]
        )
        volume_mounts = jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "airflow_local_settings.py" not in str(volume_mounts)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={"airflowLocalSettings": "# Well hello!"},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    @pytest.mark.parametrize(
        ("git_sync_values", "dags_persistence_enabled"),
        _get_git_sync_test_params_for_no_containers("migrateDatabaseJob"),
    )
    def test_git_sync_not_added_when_disabled_or_persistent_dags(
        self, git_sync_values, dags_persistence_enabled
    ):
        _test_git_sync_presence(
            git_sync_values=git_sync_values,
            dags_persistence_enabled=dags_persistence_enabled,
            template_path="templates/jobs/migrate-database-job.yaml",
            in_init_containers=False,
            in_containers=False
        )

    @pytest.mark.parametrize(
        ("git_sync_values", "dags_persistence_enabled"),
        _get_enabled_git_sync_test_params("migrateDatabaseJob"),
    )
    def test_git_sync_added_when_enabled(self, git_sync_values, dags_persistence_enabled):
        _test_git_sync_presence(
            git_sync_values=git_sync_values,
            dags_persistence_enabled=dags_persistence_enabled,
            template_path="templates/jobs/migrate-database-job.yaml",
            in_init_containers=True,
            in_containers=False
        )

    @pytest.mark.parametrize(
        "restart_policy",
        [
            "OnFailure",
            "Never",
        ],
    )
    def test_restart_policy(self, restart_policy):
        docs = render_chart(
            values={"migrateDatabaseJob": {"restartPolicy": restart_policy}},
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        assert restart_policy == jmespath.search("spec.template.spec.restartPolicy", docs[0])


class TestMigrateDatabaseJobServiceAccount:
    """Tests migrate database job service account."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/jobs/migrate-database-job-serviceaccount.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_should_merge_common_labels_and_component_specific_labels(self):
        docs = render_chart(
            values={
                "labels": {"test_common_label": "test_common_label_value"},
                "migrateDatabaseJob": {
                    "labels": {"test_specific_label": "test_specific_label_value"},
                },
            },
            show_only=["templates/jobs/migrate-database-job-serviceaccount.yaml"],
        )
        assert "test_common_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_common_label"] == "test_common_label_value"
        assert "test_specific_label" in jmespath.search("metadata.labels", docs[0])
        assert (
            jmespath.search("metadata.labels", docs[0])["test_specific_label"] == "test_specific_label_value"
        )

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/jobs/migrate-database-job-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "serviceAccount": {"create": True, "automountServiceAccountToken": False},
                },
            },
            show_only=["templates/jobs/migrate-database-job-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False

    def test_should_add_component_specific_env(self):
        env = {"name": "test_env_key", "value": "test_env_value"}
        docs = render_chart(
            values={
                "migrateDatabaseJob": {
                    "env": [env],
                },
            },
            show_only=["templates/jobs/migrate-database-job.yaml"],
        )
        assert env in jmespath.search("spec.template.spec.containers[0].env", docs[0])
