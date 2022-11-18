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

from tests.charts.helm_template_generator import render_chart


class TestCreateUserJob:
    def test_should_run_by_default(self):
        docs = render_chart(show_only=["templates/jobs/create-user-job.yaml"])
        assert "Job" == docs[0]["kind"]
        assert "create-user" == jmespath.search("spec.template.spec.containers[0].name", docs[0])
        assert 50000 == jmespath.search("spec.template.spec.securityContext.runAsUser", docs[0])

    def test_should_support_annotations(self):
        docs = render_chart(
            values={"createUserJob": {"annotations": {"foo": "bar"}, "jobAnnotations": {"fiz": "fuz"}}},
            show_only=["templates/jobs/create-user-job.yaml"],
        )
        annotations = jmespath.search("spec.template.metadata.annotations", docs[0])
        assert "foo" in annotations
        assert "bar" == annotations["foo"]
        job_annotations = jmespath.search("metadata.annotations", docs[0])
        assert "fiz" in job_annotations
        assert "fuz" == job_annotations["fiz"]

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "createUserJob": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )
        assert "test_label" in jmespath.search("spec.template.metadata.labels", docs[0])
        assert jmespath.search("spec.template.metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "createUserJob": {
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
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert "Job" == jmespath.search("kind", docs[0])
        assert "foo" == jmespath.search(
            "spec.template.spec.affinity.nodeAffinity."
            "requiredDuringSchedulingIgnoredDuringExecution."
            "nodeSelectorTerms[0]."
            "matchExpressions[0]."
            "key",
            docs[0],
        )
        assert "ssd" == jmespath.search(
            "spec.template.spec.nodeSelector.diskType",
            docs[0],
        )
        assert "dynamic-pods" == jmespath.search(
            "spec.template.spec.tolerations[0].key",
            docs[0],
        )

    def test_create_user_job_resources_are_configurable(self):
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
                "createUserJob": {
                    "resources": resources,
                },
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert resources == jmespath.search("spec.template.spec.containers[0].resources", docs[0])

    def test_should_disable_default_helm_hooks(self):
        docs = render_chart(
            values={"createUserJob": {"useHelmHooks": False}},
            show_only=["templates/jobs/create-user-job.yaml"],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations is None

    def test_should_set_correct_helm_hooks_weight(self):
        docs = render_chart(
            show_only=[
                "templates/jobs/create-user-job.yaml",
            ],
        )
        annotations = jmespath.search("metadata.annotations", docs[0])
        assert annotations["helm.sh/hook-weight"] == "2"

    def test_should_add_extra_containers(self):
        docs = render_chart(
            values={
                "createUserJob": {
                    "extraContainers": [
                        {"name": "test-container", "image": "test-registry/test-repo:test-tag"}
                    ],
                },
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert {
            "name": "test-container",
            "image": "test-registry/test-repo:test-tag",
        } == jmespath.search("spec.template.spec.containers[-1]", docs[0])

    def test_should_add_extra_volumes(self):
        docs = render_chart(
            values={
                "createUserJob": {
                    "extraVolumes": [{"name": "myvolume", "emptyDir": {}}],
                },
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert {"name": "myvolume", "emptyDir": {}} == jmespath.search(
            "spec.template.spec.volumes[-1]", docs[0]
        )

    def test_should_add_extra_volume_mounts(self):
        docs = render_chart(
            values={
                "createUserJob": {
                    "extraVolumeMounts": [{"name": "foobar", "mountPath": "foo/bar"}],
                },
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert {"name": "foobar", "mountPath": "foo/bar"} == jmespath.search(
            "spec.template.spec.containers[0].volumeMounts[-1]", docs[0]
        )

    def test_should_add_extraEnvs(self):
        docs = render_chart(
            values={
                "createUserJob": {
                    "env": [{"name": "TEST_ENV_1", "value": "test_env_1"}],
                },
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert {"name": "TEST_ENV_1", "value": "test_env_1"} in jmespath.search(
            "spec.template.spec.containers[0].env", docs[0]
        )

    def test_should_enable_custom_env(self):
        docs = render_chart(
            values={
                "env": [
                    {"name": "foo", "value": "bar"},
                ],
                "extraEnv": "- name: extraFoo\n  value: extraBar\n",
                "createUserJob": {"applyCustomEnv": True},
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )
        envs = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        assert {"name": "foo", "value": "bar"} in envs
        assert {"name": "extraFoo", "value": "extraBar"} in envs

    def test_should_disable_custom_env(self):
        docs = render_chart(
            values={
                "env": [
                    {"name": "foo", "value": "bar"},
                ],
                "extraEnv": "- name: extraFoo\n  value: extraBar\n",
                "createUserJob": {"applyCustomEnv": False},
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )
        envs = jmespath.search("spec.template.spec.containers[0].env", docs[0])
        assert {"name": "foo", "value": "bar"} not in envs
        assert {"name": "extraFoo", "value": "extraBar"} not in envs

    @pytest.mark.parametrize(
        "airflow_version, expected_arg",
        [
            ("1.10.14", "airflow create_user"),
            ("2.0.2", "airflow users create"),
        ],
    )
    def test_default_command_and_args_airflow_version(self, airflow_version, expected_arg):
        docs = render_chart(
            values={
                "airflowVersion": airflow_version,
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert [
            "bash",
            "-c",
            f'exec \\\n{expected_arg} "$@"',
            "--",
            "-r",
            "Admin",
            "-u",
            "admin",
            "-e",
            "admin@example.com",
            "-f",
            "admin",
            "-l",
            "user",
            "-p",
            "admin",
        ] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    @pytest.mark.parametrize("command", [None, ["custom", "command"]])
    @pytest.mark.parametrize("args", [None, ["custom", "args"]])
    def test_command_and_args_overrides(self, command, args):
        docs = render_chart(
            values={"createUserJob": {"command": command, "args": args}},
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert command == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_command_and_args_overrides_are_templated(self):
        docs = render_chart(
            values={
                "createUserJob": {"command": ["{{ .Release.Name }}"], "args": ["{{ .Release.Service }}"]}
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert ["release-name"] == jmespath.search("spec.template.spec.containers[0].command", docs[0])
        assert ["Helm"] == jmespath.search("spec.template.spec.containers[0].args", docs[0])

    def test_default_user_overrides(self):
        docs = render_chart(
            values={
                "webserver": {
                    "defaultUser": {
                        "role": "SomeRole",
                        "username": "jdoe",
                        "email": "jdoe@example.com",
                        "firstName": "John",
                        "lastName": "Doe",
                        "password": "whereisjane?",
                    }
                }
            },
            show_only=["templates/jobs/create-user-job.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].command", docs[0]) is None
        assert [
            "bash",
            "-c",
            'exec \\\nairflow users create "$@"',
            "--",
            "-r",
            "SomeRole",
            "-u",
            "jdoe",
            "-e",
            "jdoe@example.com",
            "-f",
            "John",
            "-l",
            "Doe",
            "-p",
            "whereisjane?",
        ] == jmespath.search("spec.template.spec.containers[0].args", docs[0])


class TestCreateUserJobServiceAccount:
    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "createUserJob": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/jobs/create-user-job-serviceaccount.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"
