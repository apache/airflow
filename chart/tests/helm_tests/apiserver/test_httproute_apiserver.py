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
from chart_utils.helm_template_generator import HelmFailedError, render_chart

SHOW_ONLY = ["templates/api-server/api-server-httproute.yaml"]

# The template is gated on the Gateway API CRD being installed
# (`.Capabilities.APIVersions.Has`). Pass it explicitly so `helm template`
# can render the resource offline.
GATEWAY_API_VERSIONS = ["gateway.networking.k8s.io/v1"]

# A minimal valid `parentRefs`; the HTTPRoute now requires at least one parent
# Gateway reference when enabled, so rendering tests must supply it.
MINIMAL_PARENT_REFS = [{"name": "main-gateway"}]


class TestHTTPRouteAPIServer:
    """Tests HTTPRoute API Server (Kubernetes Gateway API)."""

    def test_should_pass_validation_with_minimal_config(self):
        docs = render_chart(
            values={"apiServer": {"httpRoute": {"enabled": True, "parentRefs": MINIMAL_PARENT_REFS}}},
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert len(docs) == 1
        assert jmespath.search("kind", docs[0]) == "HTTPRoute"
        assert jmespath.search("metadata.name", docs[0]) == "release-name-api-server-httproute"

    def test_should_set_api_version_and_kind(self):
        docs = render_chart(
            values={"apiServer": {"httpRoute": {"enabled": True, "parentRefs": MINIMAL_PARENT_REFS}}},
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("apiVersion", docs[0]) == "gateway.networking.k8s.io/v1"
        assert jmespath.search("kind", docs[0]) == "HTTPRoute"

    def test_should_allow_more_than_one_annotation(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "annotations": {"aa": "bb", "cc": "dd"},
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("metadata.annotations", docs[0]) == {"aa": "bb", "cc": "dd"}

    def test_should_add_extra_labels(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "labels": {"custom": "value"},
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search('metadata.labels."custom"', docs[0]) == "value"

    def test_should_pass_parent_refs_through(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": [
                            {"name": "main-gateway", "namespace": "gateway-system"},
                            {"name": "main-gateway", "namespace": "gateway-system", "sectionName": "https"},
                        ],
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("spec.parentRefs", docs[0]) == [
            {"name": "main-gateway", "namespace": "gateway-system"},
            {"name": "main-gateway", "namespace": "gateway-system", "sectionName": "https"},
        ]

    def test_should_set_hostnames(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "hostnames": ["airflow.example.com", "airflow2.example.com"],
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("spec.hostnames", docs[0]) == [
            "airflow.example.com",
            "airflow2.example.com",
        ]

    def test_hostnames_should_be_templated(self):
        docs = render_chart(
            name="airflow",
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "hostnames": ["{{ .Release.Name }}.example.com"],
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("spec.hostnames", docs[0]) == ["airflow.example.com"]

    def test_should_default_to_path_prefix_and_api_server_backend(self):
        docs = render_chart(
            name="my-release",
            values={"apiServer": {"httpRoute": {"enabled": True, "parentRefs": MINIMAL_PARENT_REFS}}},
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("spec.rules", docs[0]) == [
            {
                "matches": [{"path": {"type": "PathPrefix", "value": "/"}}],
                "backendRefs": [{"name": "my-release-api-server", "port": 8080}],
            },
        ]

    def test_custom_path_and_path_type_should_apply(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "path": "/api",
                        "pathType": "Exact",
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("spec.rules[0].matches[0].path.type", docs[0]) == "Exact"
        assert jmespath.search("spec.rules[0].matches[0].path.value", docs[0]) == "/api"

    def test_custom_rules_override_default_rule(self):
        docs = render_chart(
            values={
                "apiServer": {
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "rules": [
                            {
                                "matches": [{"path": {"type": "PathPrefix", "value": "/admin"}}],
                                "backendRefs": [{"name": "external-admin", "port": 8443}],
                            },
                        ],
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert jmespath.search("spec.rules", docs[0]) == [
            {
                "matches": [{"path": {"type": "PathPrefix", "value": "/admin"}}],
                "backendRefs": [{"name": "external-admin", "port": 8443}],
            },
        ]

    def test_httproute_not_created_when_unset(self):
        docs = render_chart(
            values={"apiServer": {"httpRoute": {}}},
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert docs == []

    def test_httproute_not_created_when_disabled(self):
        docs = render_chart(
            values={"apiServer": {"httpRoute": {"enabled": False}}},
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert docs == []

    def test_httproute_created_when_enabled(self):
        docs = render_chart(
            values={"apiServer": {"httpRoute": {"enabled": True, "parentRefs": MINIMAL_PARENT_REFS}}},
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert len(docs) == 1

    def test_should_not_render_when_api_server_disabled(self):
        docs = render_chart(
            values={
                "apiServer": {"enabled": False, "httpRoute": {"enabled": True}},
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert docs == []

    def test_should_fail_when_gateway_api_crd_missing(self):
        # No api_versions -> the Gateway API CRD is treated as not installed.
        with pytest.raises(HelmFailedError) as exc_info:
            render_chart(
                values={"apiServer": {"httpRoute": {"enabled": True}}},
                show_only=SHOW_ONLY,
            )
        assert "Gateway API HTTPRoute CRD" in exc_info.value.stderr.decode()

    def test_should_fail_when_parent_refs_empty(self):
        # Gateway API CRD present, but parentRefs left at its `~` default -> the template
        # guard catches the still-null value while enabled.
        with pytest.raises(HelmFailedError) as exc_info:
            render_chart(
                values={"apiServer": {"httpRoute": {"enabled": True}}},
                show_only=SHOW_ONLY,
                api_versions=GATEWAY_API_VERSIONS,
            )
        assert "parentRefs" in exc_info.value.stderr.decode()

    def test_should_fail_schema_when_parent_refs_explicitly_empty(self):
        # An explicitly empty `parentRefs: []` violates the schema's `minItems: 1` and
        # fails fast during values validation, before any template guard runs.
        with pytest.raises(HelmFailedError) as exc_info:
            render_chart(
                values={"apiServer": {"httpRoute": {"enabled": True, "parentRefs": []}}},
                show_only=SHOW_ONLY,
                api_versions=GATEWAY_API_VERSIONS,
            )
        assert "parentRefs" in exc_info.value.stderr.decode()

    def test_should_fail_when_ingress_and_httproute_both_enabled(self):
        with pytest.raises(HelmFailedError) as exc_info:
            render_chart(
                values={
                    "ingress": {"apiServer": {"enabled": True}},
                    "apiServer": {"httpRoute": {"enabled": True}},
                },
                show_only=SHOW_ONLY,
                api_versions=GATEWAY_API_VERSIONS,
            )
        # Either reciprocal guard (httproute-side or ingress-side) may fire first;
        # both share this closing phrase.
        assert "enable only one of them" in exc_info.value.stderr.decode()

    def test_backend_service_name_with_fullname_override(self):
        docs = render_chart(
            values={
                "fullnameOverride": "airflow-fullname-override",
                "useStandardNaming": True,
                "apiServer": {"httpRoute": {"enabled": True, "parentRefs": MINIMAL_PARENT_REFS}},
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert (
            jmespath.search("spec.rules[0].backendRefs[0].name", docs[0])
            == "airflow-fullname-override-api-server"
        )

    def test_should_add_component_specific_labels(self):
        docs = render_chart(
            values={
                "labels": {"label1": "value1", "label2": "value2"},
                "apiServer": {
                    "labels": {"test_label": "test_label_value"},
                    "httpRoute": {
                        "enabled": True,
                        "parentRefs": MINIMAL_PARENT_REFS,
                        "labels": {"route_label": "route_value"},
                    },
                },
            },
            show_only=SHOW_ONLY,
            api_versions=GATEWAY_API_VERSIONS,
        )
        assert "tier" in jmespath.search("metadata.labels", docs[0])
        assert "release" in jmespath.search("metadata.labels", docs[0])
        assert jmespath.search('metadata.labels."label1"', docs[0]) == "value1"
        assert jmespath.search('metadata.labels."test_label"', docs[0]) == "test_label_value"
        assert jmespath.search('metadata.labels."route_label"', docs[0]) == "route_value"
