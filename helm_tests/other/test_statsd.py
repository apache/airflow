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
import yaml

from tests.charts.helm_template_generator import render_chart


class TestStatsd:
    """Tests statsd."""

    def test_should_create_statsd_default(self):
        docs = render_chart(show_only=["templates/statsd/statsd-deployment.yaml"])

        assert "release-name-statsd" == jmespath.search("metadata.name", docs[0])

        assert "statsd" == jmespath.search("spec.template.spec.containers[0].name", docs[0])

        assert {"name": "config", "configMap": {"name": "release-name-statsd"}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

        assert {
            "name": "config",
            "mountPath": "/etc/statsd-exporter/mappings.yml",
            "subPath": "mappings.yml",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

        default_args = ["--statsd.mapping-config=/etc/statsd-exporter/mappings.yml"]
        assert default_args == jmespath.search("spec.template.spec.containers[0].args", docs[0])

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

        assert {"name": "config", "configMap": {"name": "release-name-statsd"}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

        assert {
            "name": "config",
            "mountPath": "/etc/statsd-exporter/mappings.yml",
            "subPath": "mappings.yml",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    def test_should_add_volume_and_volume_mount_when_exist_override_mappings(self):
        override_mapping = {
            "match": "airflow.pool.queued_slots.*",
            "name": "airflow_pool_queued_slots",
            "labels": {"pool": "$1"},
        }
        docs = render_chart(
            values={"statsd": {"enabled": True, "overrideMappings": [override_mapping]}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert {"name": "config", "configMap": {"name": "release-name-statsd"}} in jmespath.search(
            "spec.template.spec.volumes", docs[0]
        )

        assert {
            "name": "config",
            "mountPath": "/etc/statsd-exporter/mappings.yml",
            "subPath": "mappings.yml",
        } in jmespath.search("spec.template.spec.containers[0].volumeMounts", docs[0])

    @pytest.mark.parametrize(
        "revision_history_limit, global_revision_history_limit",
        [(8, 10), (10, 8), (8, None), (None, 10), (None, None)],
    )
    def test_revision_history_limit(self, revision_history_limit, global_revision_history_limit):
        values = {"statsd": {"enabled": True}}
        if revision_history_limit:
            values["statsd"]["revisionHistoryLimit"] = revision_history_limit
        if global_revision_history_limit:
            values["revisionHistoryLimit"] = global_revision_history_limit
        docs = render_chart(
            values=values,
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        expected_result = revision_history_limit or global_revision_history_limit
        assert jmespath.search("spec.revisionHistoryLimit", docs[0]) == expected_result

    def test_scheduler_name(self):
        docs = render_chart(
            values={"schedulerName": "airflow-scheduler"},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert "airflow-scheduler" == jmespath.search(
            "spec.template.spec.schedulerName",
            docs[0],
        )

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
                        "limits": {"cpu": "200m", "memory": "128Mi"},
                        "requests": {"cpu": "300m", "memory": "169Mi"},
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

    def test_statsd_security_contexts_are_configurable(self):
        docs = render_chart(
            values={
                "statsd": {
                    "securityContexts": {
                        "pod": {
                            "fsGroup": 1000,
                            "runAsGroup": 1001,
                            "runAsNonRoot": True,
                            "runAsUser": 2000,
                        },
                        "container": {
                            "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True,
                        },
                    }
                },
            },
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        assert {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True} == jmespath.search(
            "spec.template.spec.containers[0].securityContext", docs[0]
        )

        assert {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        } == jmespath.search("spec.template.spec.securityContext", docs[0])

    def test_statsd_security_context_legacy(self):
        docs = render_chart(
            values={
                "statsd": {
                    "securityContext": {
                        "fsGroup": 1000,
                        "runAsGroup": 1001,
                        "runAsNonRoot": True,
                        "runAsUser": 2000,
                    }
                },
            },
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert {
            "runAsUser": 2000,
            "runAsGroup": 1001,
            "fsGroup": 1000,
            "runAsNonRoot": True,
        } == jmespath.search("spec.template.spec.securityContext", docs[0])

    def test_statsd_resources_are_not_added_by_default(self):
        docs = render_chart(
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        assert jmespath.search("spec.template.spec.containers[0].resources", docs[0]) == {}

    def test_statsd_configmap_by_default(self):
        docs = render_chart(show_only=["templates/configmaps/statsd-configmap.yaml"])

        mappings_yml = jmespath.search('data."mappings.yml"', docs[0])
        mappings_yml_obj = yaml.safe_load(mappings_yml)

        names = [mapping["name"] for mapping in mappings_yml_obj["mappings"]]
        assert "airflow_dagrun_dependency_check" in names
        assert "airflow_pool_starving_tasks" in names

    def test_statsd_configmap_when_exist_extra_mappings(self):
        extra_mapping = {
            "match": "airflow.pool.queued_slots.*",
            "name": "airflow_pool_queued_slots",
            "labels": {"pool": "$1"},
        }
        docs = render_chart(
            values={"statsd": {"enabled": True, "extraMappings": [extra_mapping]}},
            show_only=["templates/configmaps/statsd-configmap.yaml"],
        )

        mappings_yml = jmespath.search('data."mappings.yml"', docs[0])
        mappings_yml_obj = yaml.safe_load(mappings_yml)

        assert "airflow_dagrun_dependency_check" == mappings_yml_obj["mappings"][0]["name"]
        assert "airflow_pool_queued_slots" == mappings_yml_obj["mappings"][-1]["name"]

    def test_statsd_configmap_when_exist_override_mappings(self):
        override_mapping = {
            "match": "airflow.pool.queued_slots.*",
            "name": "airflow_pool_queued_slots",
            "labels": {"pool": "$1"},
        }
        docs = render_chart(
            values={"statsd": {"enabled": True, "overrideMappings": [override_mapping]}},
            show_only=["templates/configmaps/statsd-configmap.yaml"],
        )

        mappings_yml = jmespath.search('data."mappings.yml"', docs[0])
        mappings_yml_obj = yaml.safe_load(mappings_yml)

        assert 1 == len(mappings_yml_obj["mappings"])
        assert "airflow_pool_queued_slots" == mappings_yml_obj["mappings"][0]["name"]

    def test_statsd_args_can_be_overridden(self):
        args = ["--some-arg=foo"]
        docs = render_chart(
            values={"statsd": {"enabled": True, "args": args}},
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )

        assert jmespath.search("spec.template.spec.containers[0].args", docs[0]) == args

    def test_should_add_component_specific_annotations(self):
        docs = render_chart(
            values={
                "statsd": {
                    "annotations": {"test_annotation": "test_annotation_value"},
                    "podAnnotations": {"test_pod_annotation": "test_pod_annotation_value"},
                },
            },
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        assert "annotations" in jmespath.search("metadata", docs[0])
        assert jmespath.search("metadata.annotations", docs[0])["test_annotation"] == "test_annotation_value"
        assert "test_pod_annotation" in jmespath.search("spec.template.metadata.annotations", docs[0])
        assert (
            jmespath.search("spec.template.metadata.annotations", docs[0])["test_pod_annotation"]
            == "test_pod_annotation_value"
        )

    def test_should_add_custom_env_variables(self):
        env1 = {"name": "TEST_ENV_1", "value": "test_env_1"}

        docs = render_chart(
            values={
                "statsd": {
                    "enabled": True,
                    "env": [env1],
                },
            },
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )[0]

        assert jmespath.search("spec.template.spec.containers[0].env", docs) == [env1]

    def test_should_add_annotations_to_statsd_configmap(self):
        docs = render_chart(
            values={
                "statsd": {
                    "enabled": True,
                    "configMapAnnotations": {"test_annotation": "test_annotation_value"},
                },
            },
            show_only=["templates/configmaps/statsd-configmap.yaml"],
        )[0]

        assert "annotations" in jmespath.search("metadata", docs)
        assert jmespath.search("metadata.annotations", docs)["test_annotation"] == "test_annotation_value"

    @pytest.mark.parametrize(
        "statsd_values, expected",
        [
            ({}, 30),
            ({"statsd": {"terminationGracePeriodSeconds": 1200}}, 1200),
        ],
    )
    def test_statsd_termination_grace_period_seconds(self, statsd_values, expected):
        docs = render_chart(
            values=statsd_values,
            show_only=["templates/statsd/statsd-deployment.yaml"],
        )
        assert expected == jmespath.search("spec.template.spec.terminationGracePeriodSeconds", docs[0])


class TestStatsdServiceAccount:
    """Tests statsd service account."""

    def test_default_automount_service_account_token(self):
        docs = render_chart(
            values={
                "statsd": {
                    "serviceAccount": {"create": True},
                },
            },
            show_only=["templates/statsd/statsd-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is True

    def test_overridden_automount_service_account_token(self):
        docs = render_chart(
            values={
                "statsd": {
                    "serviceAccount": {"create": True, "automountServiceAccountToken": False},
                },
            },
            show_only=["templates/statsd/statsd-serviceaccount.yaml"],
        )
        assert jmespath.search("automountServiceAccountToken", docs[0]) is False


class TestStatsdIngress:
    """Tests Statsd Ingress."""

    def test_statsd_ingress(self):
        docs = render_chart(
            values={
                "statsd": {"enabled": True},
                "ingress": {
                    "statsd": {
                        "enabled": True,
                        "hosts": [
                            {"name": "some-host", "tls": {"enabled": True, "secretName": "some-secret"}}
                        ],
                        "ingressClassName": "ingress-class",
                    }
                },
            },
            show_only=["templates/statsd/statsd-ingress.yaml"],
        )

        assert {"name": "release-name-statsd", "port": {"name": "statsd-metrics"}} == jmespath.search(
            "spec.rules[0].http.paths[0].backend.service", docs[0]
        )
        assert "/metrics" == jmespath.search("spec.rules[0].http.paths[0].path", docs[0])
        assert "some-host" == jmespath.search("spec.rules[0].host", docs[0])
        assert {"hosts": ["some-host"], "secretName": "some-secret"} == jmespath.search(
            "spec.tls[0]", docs[0]
        )
        assert "ingress-class" == jmespath.search("spec.ingressClassName", docs[0])
