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


class TestDatabaseCleanupDeployment:
    """Tests database cleanup deployments."""

    def test_should_have_a_schedule_with_defaults(self):
        doc = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )[0]

        assert doc["spec"]["schedule"] == "0 0 * * 0"

    cron_tests = [
        ("release-name", "*/5 * * * *", "*/5 * * * *"),
        ("something-else", "@hourly", "@hourly"),
        (
            "custom-name",
            '{{- add 3 (regexFind ".$" (adler32sum .Release.Name)) -}}-59/15 * * * *',
            "7-59/15 * * * *",
        ),
        (
            "airflow-rules",
            '{{- add 3 (regexFind ".$" (adler32sum .Release.Name)) -}}-59/15 * * * *',
            "10-59/15 * * * *",
        ),
    ]

    @pytest.mark.parametrize(
        ("release_name", "schedule_value", "schedule_result"),
        cron_tests,
        ids=[x[0] for x in cron_tests],
    )
    def test_should_work_with_custom_schedule_string(self, release_name, schedule_value, schedule_result):
        doc = render_chart(
            name=release_name,
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "schedule": schedule_value,
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )[0]

        assert doc["spec"]["schedule"] == schedule_result


class TestDatabaseCleanup:
    """Tests database cleanup."""

    def test_should_create_cronjob_for_enabled_cleanup(self):
        docs = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert (
            jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].name", docs[0])
            == "database-cleanup"
        )
        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].image", docs[0]).startswith(
            "apache/airflow"
        )
        assert {"name": "config", "configMap": {"name": "release-name-config"}} in jmespath.search(
            "spec.jobTemplate.spec.template.spec.volumes", docs[0]
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "subPath": "airflow.cfg",
            "readOnly": True,
        } in jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].volumeMounts", docs[0])
        assert "successfulJobsHistoryLimit" in docs[0]["spec"]
        assert "failedJobsHistoryLimit" in docs[0]["spec"]

    def test_should_pass_validation_with_v1beta1_api(self):
        render_chart(
            values={"databaseCleanup": {"enabled": True}},
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
            kubernetes_version="1.16.0",
        )  # checks that no validation exception is raised

    def test_should_change_image_when_set_airflow_image(self):
        docs = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
                "images": {"airflow": {"repository": "airflow", "tag": "test"}},
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert (
            jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].image", docs[0])
            == "airflow:test"
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
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
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert jmespath.search("kind", docs[0]) == "CronJob"
        assert (
            jmespath.search(
                "spec.jobTemplate.spec.template.spec.affinity.nodeAffinity."
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
                "spec.jobTemplate.spec.template.spec.nodeSelector.diskType",
                docs[0],
            )
            == "ssd"
        )
        assert (
            jmespath.search(
                "spec.jobTemplate.spec.template.spec.tolerations[0].key",
                docs[0],
            )
            == "dynamic-pods"
        )

    def test_scheduler_name(self):
        docs = render_chart(
            values={"databaseCleanup": {"enabled": True}, "schedulerName": "airflow-scheduler"},
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert (
            jmespath.search(
                "spec.jobTemplate.spec.template.spec.schedulerName",
                docs[0],
            )
            == "airflow-scheduler"
        )

    def test_retention_days_changed(self):
        docs = render_chart(
            values={"databaseCleanup": {"enabled": True, "retentionDays": 10}},
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0]) == [
            "-c",
            'CLEAN_TS=$(date -d "-10 days" +"%Y-%m-%dT%H:%M:%S"); echo "Cleaning up metadata DB entries older than ${CLEAN_TS}"; exec airflow db clean --clean-before-timestamp "${CLEAN_TS}" --yes --verbose',
        ]

    def test_default_command_and_args(self):
        docs = render_chart(
            values={"databaseCleanup": {"enabled": True}},
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]) == [
            "bash"
        ]
        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0]) == [
            "-c",
            'CLEAN_TS=$(date -d "-90 days" +"%Y-%m-%dT%H:%M:%S"); echo "Cleaning up metadata DB entries older than ${CLEAN_TS}"; exec airflow db clean --clean-before-timestamp "${CLEAN_TS}" --yes --verbose',
        ]

    @pytest.mark.parametrize(
        ("retention", "skip_archive", "verbose", "batch_size", "tables", "command_args"),
        [
            (90, False, False, None, None, ""),
            (91, True, False, None, None, " --skip-archive"),
            (92, False, True, None, None, " --verbose"),
            (93, False, False, 200, None, " --batch-size 200"),
            (94, False, False, None, ["xcom"], " --tables xcom"),
            (
                95,
                True,
                True,
                500,
                ["task_instance", "log"],
                " --skip-archive --verbose --batch-size 500 --tables task_instance,log",
            ),
        ],
    )
    def test_cleanup_command_options(
        self, retention, skip_archive, verbose, batch_size, tables, command_args
    ):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "retentionDays": retention,
                    "skipArchive": skip_archive,
                    "verbose": verbose,
                    "batchSize": batch_size,
                    "tables": tables,
                }
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]) == [
            "bash"
        ]
        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0]) == [
            "-c",
            f'CLEAN_TS=$(date -d "-{retention} days" +"%Y-%m-%dT%H:%M:%S"); echo "Cleaning up metadata DB entries older than ${{CLEAN_TS}}"; exec airflow db clean --clean-before-timestamp "${{CLEAN_TS}}" --yes{command_args}',
        ]

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].env", docs[0]
        )

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"databaseCleanup": {"enabled": True, "command": command, "args": args}},
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        if not command and not args:
            assert not docs, (
                "The CronJob should not be created if command and args are null even if enabled is true"
            )
        else:
            assert command == jmespath.search(
                "spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]
            )
            assert args == jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                }
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]) == [
            "release-name"
        ]
        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0]) == ["Helm"]

    def test_should_set_labels_to_jobs_from_cronjob(self):
        docs = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
                "labels": {"project": "airflow"},
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert jmespath.search("spec.jobTemplate.spec.template.metadata.labels", docs[0]) == {
            "tier": "airflow",
            "component": "database-cleanup",
            "release": "release-name",
            "project": "airflow",
        }

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert "test_label" in jmespath.search("spec.jobTemplate.spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.jobTemplate.spec.template.metadata.labels", docs[0])["test_label"]
            == "test_label_value"
        )

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "jobAnnotations": {"test_cronjob_annotation": "test_cronjob_annotation_value"},
                    "podAnnotations": {"test_pod_annotation": "test_pod_annotation_value"},
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert "test_cronjob_annotation" in jmespath.search("metadata.annotations", docs[0])
        assert (
            jmespath.search("metadata.annotations", docs[0])["test_cronjob_annotation"]
            == "test_cronjob_annotation_value"
        )
        assert "test_pod_annotation" in jmespath.search(
            "spec.jobTemplate.spec.template.metadata.annotations", docs[0]
        )
        assert (
            jmespath.search("spec.jobTemplate.spec.template.metadata.annotations", docs[0])[
                "test_pod_annotation"
            ]
            == "test_pod_annotation_value"
        )

    def test_cleanup_resources_are_configurable(self):
        resources = {
            "requests": {
                "cpu": "128m",
                "memory": "256Mi",
            },
            "limits": {
                "cpu": "256m",
                "memory": "512Mi",
            },
        }
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "resources": resources,
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )

        assert (
            jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].resources", docs[0])
            == resources
        )

    def test_should_set_job_history_limits(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "failedJobsHistoryLimit": 2,
                    "successfulJobsHistoryLimit": 4,
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )
        assert jmespath.search("spec.failedJobsHistoryLimit", docs[0]) == 2
        assert jmespath.search("spec.successfulJobsHistoryLimit", docs[0]) == 4

    def test_should_set_zero_job_history_limits(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "failedJobsHistoryLimit": 0,
                    "successfulJobsHistoryLimit": 0,
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )
        assert jmespath.search("spec.failedJobsHistoryLimit", docs[0]) == 0
        assert jmespath.search("spec.successfulJobsHistoryLimit", docs[0]) == 0

    def test_no_airflow_local_settings(self):
        docs = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
                "airflowLocalSettings": None,
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )
        volume_mounts = jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].volumeMounts", docs[0]
        )
        assert "airflow_local_settings.py" not in str(volume_mounts)

    def test_airflow_local_settings(self):
        docs = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
                "airflowLocalSettings": "# Well hello!",
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/config/airflow_local_settings.py",
            "subPath": "airflow_local_settings.py",
            "readOnly": True,
        } in jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_global_volumes_and_volume_mounts(self):
        docs = render_chart(
            values={
                "databaseCleanup": {"enabled": True},
                "volumes": [{"name": "test-volume", "emptyDir": {}}],
                "volumeMounts": [{"name": "test-volume", "mountPath": "/test"}],
            },
            show_only=["templates/database-cleanup/database-cleanup-cronjob.yaml"],
        )
        assert {
            "name": "test-volume",
            "mountPath": "/test",
        } in jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].volumeMounts", docs[0])
        assert {
            "name": "test-volume",
            "emptyDir": {},
        } in jmespath.search("spec.jobTemplate.spec.template.spec.volumes", docs[0])


class TestDatabaseCleanupServiceAccount:
    """Tests cleanup of service accounts."""

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-serviceaccount.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "databaseCleanup": {
                    "enabled": True,
                    "serviceAccount": {"automountServiceAccountToken": False},
                },
            },
            show_only=["templates/database-cleanup/database-cleanup-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False
