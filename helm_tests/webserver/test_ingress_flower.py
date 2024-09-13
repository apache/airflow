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

import itertools

import jmespath
import pytest

from tests.charts.helm_template_generator import render_chart


class TestIngressFlower:
    """Tests ingress flower."""

    def test_should_pass_validation_with_just_ingress_enabled_v1(self):
        render_chart(
            values={"flower": {"enabled": True}, "ingress": {"flower": {"enabled": True}}},
            show_only=["templates/flower/flower-ingress.yaml"],
        )  # checks that no validation exception is raised

    def test_should_pass_validation_with_just_ingress_enabled_v1beta1(self):
        render_chart(
            values={"flower": {"enabled": True}, "ingress": {"flower": {"enabled": True}}},
            show_only=["templates/flower/flower-ingress.yaml"],
            kubernetes_version="1.16.0",
        )  # checks that no validation exception is raised

    def test_should_allow_more_than_one_annotation(self):
        docs = render_chart(
            values={
                "ingress": {"flower": {"enabled": True, "annotations": {"aa": "bb", "cc": "dd"}}},
                "flower": {"enabled": True},
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )
        assert jmespath.search("metadata.annotations", docs[0]) == {"aa": "bb", "cc": "dd"}

    def test_should_set_ingress_class_name(self):
        docs = render_chart(
            values={
                "ingress": {"enabled": True, "flower": {"ingressClassName": "foo"}},
                "flower": {"enabled": True},
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )
        assert "foo" == jmespath.search("spec.ingressClassName", docs[0])

    def test_should_ingress_hosts_objs_have_priority_over_host(self):
        docs = render_chart(
            values={
                "flower": {"enabled": True},
                "ingress": {
                    "flower": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "oldsecret"},
                        "hosts": [
                            {"name": "*.a-host", "tls": {"enabled": True, "secretName": "newsecret1"}},
                            {"name": "b-host", "tls": {"enabled": True, "secretName": "newsecret2"}},
                            {"name": "c-host", "tls": {"enabled": True, "secretName": "newsecret1"}},
                            {"name": "d-host", "tls": {"enabled": False, "secretName": ""}},
                            {"name": "e-host"},
                        ],
                        "host": "old-host",
                    },
                },
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )
        assert ["*.a-host", "b-host", "c-host", "d-host", "e-host"] == jmespath.search(
            "spec.rules[*].host", docs[0]
        )
        assert [
            {"hosts": ["*.a-host"], "secretName": "newsecret1"},
            {"hosts": ["b-host"], "secretName": "newsecret2"},
            {"hosts": ["c-host"], "secretName": "newsecret1"},
        ] == jmespath.search("spec.tls[*]", docs[0])

    def test_should_ingress_hosts_strs_have_priority_over_host(self):
        docs = render_chart(
            values={
                "flower": {"enabled": True},
                "ingress": {
                    "flower": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "secret"},
                        "hosts": ["*.a-host", "b-host", "c-host", "d-host"],
                        "host": "old-host",
                    },
                },
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )

        assert ["*.a-host", "b-host", "c-host", "d-host"] == jmespath.search("spec.rules[*].host", docs[0])
        assert [
            {"hosts": ["*.a-host", "b-host", "c-host", "d-host"], "secretName": "secret"}
        ] == jmespath.search("spec.tls[*]", docs[0])

    def test_should_ingress_deprecated_host_and_top_level_tls_still_work(self):
        docs = render_chart(
            values={
                "flower": {"enabled": True},
                "ingress": {
                    "flower": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "supersecret"},
                        "host": "old-host",
                    },
                },
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )
        assert (
            ["old-host"]
            == jmespath.search("spec.rules[*].host", docs[0])
            == list(itertools.chain.from_iterable(jmespath.search("spec.tls[*].hosts", docs[0])))
        )

    def test_should_ingress_host_entry_not_exist(self):
        docs = render_chart(
            values={"flower": {"enabled": True}, "ingress": {"flower": {"enabled": True}}},
            show_only=["templates/flower/flower-ingress.yaml"],
        )
        assert not jmespath.search("spec.rules[*].host", docs[0])

    @pytest.mark.parametrize(
        "global_value, flower_value, expected",
        [
            (None, None, False),
            (None, False, False),
            (None, True, True),
            (False, None, False),
            (True, None, True),
            (False, True, True),  # We will deploy it if _either_ are true
            (True, False, True),
        ],
    )
    def test_ingress_created(self, global_value, flower_value, expected):
        values = {"flower": {"enabled": True}, "ingress": {}}
        if global_value is not None:
            values["ingress"]["enabled"] = global_value
        if flower_value is not None:
            values["ingress"]["flower"] = {"enabled": flower_value}
        if values["ingress"] == {}:
            del values["ingress"]
        docs = render_chart(values=values, show_only=["templates/flower/flower-ingress.yaml"])
        assert expected == (1 == len(docs))

    def test_ingress_not_created_flower_disabled(self):
        docs = render_chart(
            values={
                "ingress": {
                    "flower": {"enabled": True},
                }
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )
        assert 0 == len(docs)

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "ingress": {"enabled": True},
                "flower": {
                    "enabled": True,
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/flower/flower-ingress.yaml"],
        )

        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_can_ingress_hosts_be_templated(self):
        docs = render_chart(
            values={
                "testValues": {
                    "scalar": "aa",
                    "list": ["bb", "cc"],
                    "dict": {
                        "key": "dd",
                    },
                },
                "flower": {"enabled": True},
                "ingress": {
                    "flower": {
                        "enabled": True,
                        "hosts": [
                            {"name": "*.{{ .Release.Namespace }}.example.com"},
                            {"name": "{{ .Values.testValues.scalar }}.example.com"},
                            {"name": "{{ index .Values.testValues.list 1 }}.example.com"},
                            {"name": "{{ .Values.testValues.dict.key }}.example.com"},
                        ],
                    },
                },
            },
            show_only=["templates/flower/flower-ingress.yaml"],
            namespace="airflow",
        )

        assert [
            "*.airflow.example.com",
            "aa.example.com",
            "cc.example.com",
            "dd.example.com",
        ] == jmespath.search("spec.rules[*].host", docs[0])

    def test_backend_service_name(self):
        docs = render_chart(
            values={"ingress": {"enabled": True}, "flower": {"enabled": True}},
            show_only=["templates/flower/flower-ingress.yaml"],
        )

        assert "release-name-flower" == jmespath.search("spec.rules[0].http.paths[0].backend.service.name", docs[0])

    def test_backend_service_name_with_fullname_override(self):
        docs = render_chart(
            values={"fullnameOverride": "test-basic",
                    "useStandardNaming": True,
                    "ingress": {"enabled": True}, "flower": {"enabled": True}},
            show_only=["templates/flower/flower-ingress.yaml"],

        )

        assert "test-basic-flower" == jmespath.search("spec.rules[0].http.paths[0].backend.service.name", docs[0])
