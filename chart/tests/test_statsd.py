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

from tests.helm_template_generator import render_chart


class StatsdTest(unittest.TestCase):
    def test_should_create_statsd_default(self):
        docs = render_chart(show_only=["templates/statsd/statsd-deployment.yaml"])

        assert "RELEASE-NAME-statsd" == jmespath.search("metadata.name", docs[0])

        assert "statsd" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

    def test_should_add_volume_and_volume_mount_when_exist_extra_mappings(self):
        extra_mapping = {
            "match": "airflow.pool.queued_slots.*",
            "name": "airflow_pool_queued_slots",
            "labels": {"pool": "$1"},
        }
        docs = render_chart(
            values={"statsd": {"enabled": True, "extraMappings": [extra_mapping]}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert {"name": "config", "configMap": {"name": "RELEASE-NAME-statsd"}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

        assert {
            "name": "config",
            "mountPath": "/etc/statsd-exporter/mappings.yml",
            "subPath": "mappings.yml",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_should_create_valid_affinity_tolerations_and_node_selector(self):
        docs = render_chart(
            values={
                "statsd": {
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
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert "Deployment" == jmespath.search("kind", docs[0])
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

    def test_stastd_resources_are_configurable(self):
        docs = render_chart(
            values={
                "statsd": {
                    "resources": {
                        "limits": {"cpu": "200m", 'memory': "128Mi"},
                        "requests": {"cpu": "300m", 'memory': "169Mi"},
                    }
                },
            },
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        assert "128Mi" == jmespath.search("spec.template.spec.containers[0].resources.limits.memory", docs[0])
        assert "169Mi" == jmespath.search(
            "spec.template.spec.containers[0].resources.requests.memory", docs[0]
        )
        assert "300m" == jmespath.search("spec.template.spec.containers[0].resources.requests.cpu", docs[0])

    def test_statsd_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}
