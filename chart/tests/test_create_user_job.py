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

import unittest

import jmespath

from chart.tests.helm_template_generator import render_chart


class CreateUserJobTest(unittest.TestCase):
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
        annotations = jmespath.search("spec.template.metadata.annotations", docs[0])
        assert annotations is None
