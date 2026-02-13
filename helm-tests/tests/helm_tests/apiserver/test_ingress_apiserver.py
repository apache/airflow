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


class TestIngressAPIServer:
    """Tests ingress API Server."""

    def test_should_pass_validation_with_just_ingress_enabled(self):
        render_chart(
            values={"ingress": {"apiServer": {"enabled": True}}, "airflowVersion": "3.0.0"},
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )

    def test_should_allow_more_than_one_annotation(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {"apiServer": {"enabled": True, "annotations": {"aa": "bb", "cc": "dd"}}},
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert jmespath.search("metadata.annotations", docs[0]) == {"aa": "bb", "cc": "dd"}

    def test_should_set_ingress_class_name(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {"apiServer": {"enabled": True, "ingressClassName": "foo"}},
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert jmespath.search("spec.ingressClassName", docs[0]) == "foo"

    def test_should_ingress_hosts_objs_have_priority_over_host(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {
                    "apiServer": {
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
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert jmespath.search("spec.rules[*].host", docs[0]) == [
            "*.a-host",
            "b-host",
            "c-host",
            "d-host",
            "e-host",
        ]
        assert jmespath.search("spec.tls[*]", docs[0]) == [
            {"hosts": ["*.a-host"], "secretName": "newsecret1"},
            {"hosts": ["b-host"], "secretName": "newsecret2"},
            {"hosts": ["c-host"], "secretName": "newsecret1"},
        ]

    def test_should_ingress_hosts_strs_have_priority_over_host(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {
                    "apiServer": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "secret"},
                        "hosts": ["*.a-host", "b-host", "c-host", "d-host"],
                        "host": "old-host",
                    },
                },
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert jmespath.search("spec.rules[*].host", docs[0]) == ["*.a-host", "b-host", "c-host", "d-host"]
        assert jmespath.search("spec.tls[*]", docs[0]) == [
            {"hosts": ["*.a-host", "b-host", "c-host", "d-host"], "secretName": "secret"}
        ]

    def test_should_ingress_deprecated_host_and_top_level_tls_still_work(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {
                    "apiServer": {
                        "enabled": True,
                        "tls": {"enabled": True, "secretName": "supersecret"},
                        "host": "old-host",
                    },
                },
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert (
            ["old-host"]
            == jmespath.search("spec.rules[*].host", docs[0])
            == jmespath.search("spec.tls[0].hosts", docs[0])
        )

    def test_should_ingress_host_entry_not_exist(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {
                    "apiServer": {
                        "enabled": True,
                    }
                },
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert not jmespath.search("spec.rules[*].host", docs[0])

    @pytest.mark.parametrize(
        ("global_value", "api_server_value", "expected"),
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
    def test_ingress_created(self, global_value, api_server_value, expected):
        values = {"airflowVersion": "3.0.0", "ingress": {}}
        if global_value is not None:
            values["ingress"]["enabled"] = global_value
        if api_server_value is not None:
            values["ingress"]["apiServer"] = {"enabled": api_server_value}
        if values["ingress"] == {}:
            del values["ingress"]
        docs = render_chart(values=values, show_only=["templates/api-server/api-server-ingress.yaml"])
        assert expected == (len(docs) == 1)

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "ingress": {"enabled": True},
                "apiServer": {
                    "labels": {"test_label": "test_label_value"},
                },
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )
        assert "test_label" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search("metadata.labels", docs[0])["test_label"] == "test_label_value"

    def test_can_ingress_hosts_be_templated(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "testValues": {
                    "scalar": "aa",
                    "list": ["bb", "cc"],
                    "dict": {
                        "key": "dd",
                    },
                },
                "ingress": {
                    "apiServer": {
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
            show_only=["templates/api-server/api-server-ingress.yaml"],
            namespace="airflow",
        )

        assert jmespath.search("spec.rules[*].host", docs[0]) == [
            "*.airflow.example.com",
            "aa.example.com",
            "cc.example.com",
            "dd.example.com",
        ]

    def test_backend_service_name(self):
        docs = render_chart(
            values={"airflowVersion": "3.0.0", "ingress": {"apiServer": {"enabled": True}}},
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )

        assert (
            jmespath.search("spec.rules[0].http.paths[0].backend.service.name", docs[0])
            == "release-name-api-server"
        )

    def test_backend_service_name_with_fullname_override(self):
        docs = render_chart(
            values={
                "airflowVersion": "3.0.0",
                "fullnameOverride": "test-basic",
                "useStandardNaming": True,
                "ingress": {"apiServer": {"enabled": True}},
            },
            show_only=["templates/api-server/api-server-ingress.yaml"],
        )

        assert (
            jmespath.search("spec.rules[0].http.paths[0].backend.service.name", docs[0])
            == "test-basic-api-server"
        )
