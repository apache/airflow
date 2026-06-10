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

SHOW_ONLY = ["templates/api-server/api-server-httproute.yaml"]


class TestHTTPRouteAPIServer:
    """Tests HTTPRoute API Server (Kubernetes Gateway API)."""

    def test_should_pass_validation_with_just_httproute_enabled(self):
        render_chart(
            values={"httpRoute": {"apiServer": {"enabled": True}}},
            show_only=SHOW_ONLY,
        )

    def test_should_set_api_version_and_kind(self):
        docs = render_chart(
            values={"httpRoute": {"apiServer": {"enabled": True}}},
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("apiVersion", docs[0]) == "gateway.networking.k8s.io/v1"
        assert jmespath.search("kind", docs[0]) == "HTTPRoute"

    def test_should_allow_more_than_one_annotation(self):
        docs = render_chart(
            values={
                "httpRoute": {
                    "apiServer": {"enabled": True, "annotations": {"aa": "bb", "cc": "dd"}},
                },
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("metadata.annotations", docs[0]) == {"aa": "bb", "cc": "dd"}

    def test_should_add_extra_labels(self):
        docs = render_chart(
            values={
                "httpRoute": {"apiServer": {"enabled": True, "labels": {"custom": "value"}}},
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search('metadata.labels."custom"', docs[0]) == "value"

    def test_should_pass_parent_refs_through(self):
        docs = render_chart(
            values={
                "httpRoute": {
                    "apiServer": {
                        "enabled": True,
                        "parentRefs": [
                            {"name": "main-gateway", "namespace": "gateway-system"},
                            {"name": "main-gateway", "namespace": "gateway-system", "sectionName": "https"},
                        ],
                    },
                },
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("spec.parentRefs[*].name", docs[0]) == ["main-gateway", "main-gateway"]
        assert jmespath.search("spec.parentRefs[1].sectionName", docs[0]) == "https"

    def test_should_set_hostnames(self):
        docs = render_chart(
            values={
                "httpRoute": {
                    "apiServer": {
                        "enabled": True,
                        "hostnames": ["airflow.example.com", "airflow2.example.com"],
                    },
                },
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("spec.hostnames", docs[0]) == [
            "airflow.example.com",
            "airflow2.example.com",
        ]

    def test_hostnames_should_be_templated(self):
        docs = render_chart(
            name="airflow",
            values={
                "httpRoute": {
                    "apiServer": {
                        "enabled": True,
                        "hostnames": ["{{ .Release.Name }}.example.com"],
                    },
                },
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("spec.hostnames", docs[0]) == ["airflow.example.com"]

    def test_should_default_to_path_prefix_and_api_server_backend(self):
        docs = render_chart(
            name="my-release",
            values={"httpRoute": {"apiServer": {"enabled": True}}},
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("spec.rules[0].matches[0].path.type", docs[0]) == "PathPrefix"
        assert jmespath.search("spec.rules[0].matches[0].path.value", docs[0]) == "/"
        assert jmespath.search("spec.rules[0].backendRefs[0].name", docs[0]) == "my-release-api-server"
        assert jmespath.search("spec.rules[0].backendRefs[0].port", docs[0]) == 8080

    def test_custom_path_and_path_type_should_apply(self):
        docs = render_chart(
            values={
                "httpRoute": {
                    "apiServer": {"enabled": True, "path": "/api", "pathType": "Exact"},
                },
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("spec.rules[0].matches[0].path.type", docs[0]) == "Exact"
        assert jmespath.search("spec.rules[0].matches[0].path.value", docs[0]) == "/api"

    def test_custom_rules_override_default_rule(self):
        custom_rules = [
            {
                "matches": [{"path": {"type": "PathPrefix", "value": "/admin"}}],
                "backendRefs": [{"name": "external-admin", "port": 8443}],
            },
        ]
        docs = render_chart(
            values={
                "httpRoute": {
                    "apiServer": {"enabled": True, "rules": custom_rules},
                },
            },
            show_only=SHOW_ONLY,
        )
        assert jmespath.search("spec.rules[0].matches[0].path.value", docs[0]) == "/admin"
        assert jmespath.search("spec.rules[0].backendRefs[0].name", docs[0]) == "external-admin"
        assert jmespath.search("spec.rules[0].backendRefs[0].port", docs[0]) == 8443

    @pytest.mark.parametrize(
        ("api_server_value", "expected"),
        [
            (None, False),
            (False, False),
            (True, True),
        ],
    )
    def test_httproute_created(self, api_server_value, expected):
        values = {"httpRoute": {}}
        if api_server_value is not None:
            values["httpRoute"]["apiServer"] = {"enabled": api_server_value}
        docs = render_chart(values=values, show_only=SHOW_ONLY)
        assert bool(docs) is expected

    def test_should_not_render_when_api_server_disabled(self):
        docs = render_chart(
            values={
                "apiServer": {"enabled": False},
                "httpRoute": {"apiServer": {"enabled": True}},
            },
            show_only=SHOW_ONLY,
        )
        assert docs == []

    def test_backend_service_name_with_fullname_override(self):
        docs = render_chart(
            values={
                "fullnameOverride": "airflow-fullname-override",
                "useStandardNaming": True,
                "httpRoute": {"apiServer": {"enabled": True}},
            },
            show_only=SHOW_ONLY,
        )
        assert (
            jmespath.search("spec.rules[0].backendRefs[0].name", docs[0])
            == "airflow-fullname-override-api-server"
        )

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "labels": {"label1": "value1", "label2": "value2"},
                "apiServer": {"labels": {"test_label": "test_label_value"}},
                "httpRoute": {"apiServer": {"enabled": True, "labels": {"route_label": "route_value"}}},
            },
            show_only=SHOW_ONLY,
        )
        assert "tier" in jmespath.search("metadata.labels", docs[0])
        assert "release" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search('metadata.labels."label1"', docs[0]) == "value1"
        assert jmespath.search('metadata.labels."test_label"', docs[0]) == "test_label_value"
        assert jmespath.search('metadata.labels."route_label"', docs[0]) == "route_value"
