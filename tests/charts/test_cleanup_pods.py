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

import unittest

import jmespath
from parameterized import parameterized

from tests.charts.helm_template_generator import render_chart


class CleanupPodsTest(unittest.TestCase):
    def test_should_create_cronjob_for_enabled_cleanup(self):
        docs = render_chart(
            values={
                "cleanup": {"enabled": True},
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "airflow-cleanup-pods" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].name", docs[0]
        )
        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].image", docs[0]).startswith(
            'apache/airflow'
        )
        assert {"name": "config", "configMap": {"name": "release-name-airflow-config"}} in jmespath.search(
            "spec.jobTemplate.spec.template.spec.volumes", docs[0]
        )
        assert {
            "name": "config",
            "mountPath": "/opt/airflow/airflow.cfg",
            "subPath": "airflow.cfg",
            "readOnly": True,
        } in jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_should_pass_validation_with_v1beta1_api(self):
        render_chart(
            values={"cleanup": {"enabled": True}},
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
            kubernetes_version='1.16.0',
        )  # checks that no validation exception is raised

    def test_should_change_image_when_set_airflow_image(self):
        docs = render_chart(
            values={
                "cleanup": {"enabled": True},
                "images": {"airflow": {"repository": "airflow", "tag": "test"}},
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "airflow:test" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].image", docs[0]
        )

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "cleanup": {
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
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "CronJob" == jmespath.search("kind", docs[0])
        assert "foo" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.jobTemplate.spec.template.spec.tolerations[0].key",
            docs[0],
        )

    def test_default_command_and_args(self):
        docs = render_chart(
            values={"cleanup": {"enabled": True}}, show_only=["templates/cleanup/cleanup-cronjob.yaml"]
        )

        assert jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]) is None
        assert ["bash", "-c", "exec airflow kubernetes cleanup-pods --namespace=default"] == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].args", docs[0]
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "cleanup": {
                    "enabled": True,
                    "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                },
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert {'name': 'TEST_ENV_1', 'value': 'test_env_1'} in jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].env", docs[0]
        )

    @parameterized.expand(
        [
            (None, None),
            (None, ["custom", "args"]),
            (["custom", "command"], None),
            (["custom", "command"], ["custom", "args"]),
        ]
    )
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"cleanup": {"enabled": True, "command": command, "args": args}},
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert command == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]
        )
        assert args == jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "cleanup": {
                    "enabled": True,
                    "command": ["{{ .Release.Name }}"],
                    "args": ["{{ .Release.Service }}"],
                }
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert ["release-name"] == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].command", docs[0]
        )
        assert ["Helm"] == jmespath.search("spec.jobTemplate.spec.template.spec.containers[0].args", docs[0])

    def test_should_set_labels_to_jobs_from_cronjob(self):
        docs = render_chart(
            values={
                "cleanup": {"enabled": True},
                "labels": {"project": "airflow"},
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert {
            "tier": "airflow",
            "component": "airflow-cleanup-pods",
            "release": "release-name",
            "project": "airflow",
        } == jmespath.search("spec.jobTemplate.spec.template.metadata.labels", docs[0])

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "cleanup": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert "test_label" in jmespath.search("spec.jobTemplate.spec.template.metadata.labels", docs[0])
        assert (
            jmespath.search("spec.jobTemplate.spec.template.metadata.labels", docs[0])["test_label"]
            == "test_label_value"
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
                "cleanup": {
                    "enabled": True,
                    "resources": resources,
                },
            },
            show_only=["templates/cleanup/cleanup-cronjob.yaml"],
        )

        assert resources == jmespath.search(
            "spec.jobTemplate.spec.template.spec.containers[0].resources", docs[0]
        )


class CleanupServiceAccountTest(unittest.TestCase):
    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "cleanup": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/cleanup/cleanup-serviceaccount.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"
