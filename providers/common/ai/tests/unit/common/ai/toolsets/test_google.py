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

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic_ai.exceptions import ModelRetry

from airflow.providers.common.ai.toolsets.google import GoogleCloudToolset
from airflow.providers.common.ai.utils.tool_definition import _SUPPORTS_RETURN_SCHEMA


def _make_toolset(**kwargs):
    kwargs.setdefault("allowed_methods", ["storage/v1:objects.list", "storage/v1:buckets.*"])
    return GoogleCloudToolset("gcp_test", **kwargs)


def _call(ts, name, args):
    return asyncio.run(ts.call_tool(name, args, ctx=MagicMock(), tool=MagicMock()))


def _install_mock_service(ts, api, version, resource_path, method, response=None):
    """Wire a MagicMock service into the toolset and return (node, bound_method, request)."""
    service = MagicMock()
    node = service
    for resource in resource_path:
        node = getattr(node, resource).return_value
    bound = getattr(node, method)
    request = bound.return_value
    request.execute.return_value = response if response is not None else {}
    getattr(node, f"{method}_next").return_value = None
    ts._services[(api, version)] = service
    return node, bound, request


def _install_mock_hook(ts, project_id="my-proj"):
    hook = MagicMock()
    hook.project_id = project_id
    ts._hook = hook
    return hook


class TestGoogleCloudToolsetInit:
    def test_id_includes_conn_id(self):
        ts = _make_toolset()
        assert ts.id == "gcp-gcp_test"

    def test_rejects_empty_allowed_methods(self):
        with pytest.raises(ValueError, match="non-empty"):
            GoogleCloudToolset("gcp_test", allowed_methods=[])

    @pytest.mark.parametrize(
        ("entry", "match"),
        [
            ("storage:objects.list", "expected"),
            ("storage/v1", "expected"),
            (":objects.list", "expected"),
            ("*/v1:foo", "wildcards are only allowed in the method part"),
            ("storage/*:foo", "wildcards are only allowed in the method part"),
            ("nosuchapi/v1:foo", "not in the bundled discovery documents"),
            ("storage/v1:no.such.method", "Unknown method"),
            ("storage/v1:no.such.*", "matches no methods"),
            ("secretmanager/v1:projects.secrets.versions.acc*", "matches no methods"),
        ],
    )
    def test_rejects_invalid_entry(self, entry, match):
        with pytest.raises(ValueError, match=match):
            GoogleCloudToolset("gcp_test", allowed_methods=[entry])

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"max_pages": 0}, "max_pages"),
            ({"max_output_bytes": 0}, "max_output_bytes"),
        ],
    )
    def test_rejects_non_positive_bounds(self, kwargs, match):
        with pytest.raises(ValueError, match=match):
            _make_toolset(**kwargs)

    def test_accepts_case_and_doc_style_variants(self):
        GoogleCloudToolset(
            "gcp_test",
            allowed_methods=["storage/v1:OBJECTS.LIST", "storage/v1:storage.buckets.list"],
        )

    def test_unbundled_api_allowed_with_remote_discovery(self):
        GoogleCloudToolset("gcp_test", allowed_methods=["nosuchapi/v1:foo.bar"], allow_remote_discovery=True)


class TestGoogleCloudToolsetGetTools:
    def test_returns_three_tools(self):
        ts = _make_toolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        assert set(tools.keys()) == {"list_gcp_methods", "describe_gcp_method", "call_gcp"}

    def test_tool_definitions_have_descriptions(self):
        ts = _make_toolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        for tool in tools.values():
            assert tool.tool_def.description

    @pytest.mark.skipif(
        not _SUPPORTS_RETURN_SCHEMA, reason="pydantic-ai too old for ToolDefinition.return_schema"
    )
    def test_tools_declare_string_return_schema(self):
        ts = _make_toolset()
        tools = asyncio.run(ts.get_tools(ctx=MagicMock()))
        for tool in tools.values():
            assert tool.tool_def.return_schema == {"type": "string"}


class TestGoogleCloudToolsetMethodMatching:
    @pytest.mark.parametrize(
        ("methods", "api", "version", "path", "allowed"),
        [
            (["storage/v1:objects.list"], "storage", "v1", "objects.list", True),
            (["storage/v1:storage.objects.list"], "storage", "v1", "objects.list", True),
            (["storage/v1:buckets.get_iam_policy"], "storage", "v1", "buckets.getIamPolicy", True),
            (["storage/v1:buckets.*"], "storage", "v1", "buckets.getIamPolicy", True),
            (["storage/v1:buckets.*"], "storage", "v1", "objects.list", False),
            (["storage/v1:*"], "bigquery", "v2", "jobs.query", False),
            (["compute/v1:instances.*"], "compute", "beta", "instances.list", False),
            (["secretmanager/v1:*"], "secretmanager", "v1", "projects.secrets.versions.access", False),
            (
                ["secretmanager/v1:projects.secrets.versions.access"],
                "secretmanager",
                "v1",
                "projects.secrets.versions.access",
                True,
            ),
            (
                ["secretmanager/v1beta1:*"],
                "secretmanager",
                "v1beta1",
                "projects.secrets.versions.access",
                False,
            ),
        ],
    )
    def test_allow_list_matching(self, methods, api, version, path, allowed):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=methods)
        assert ts._is_method_allowed(api=api, version=version, method_path=path) is allowed


class TestGoogleCloudToolsetListMethods:
    def test_lists_only_allowed_methods(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:buckets.*"])
        listing = json.loads(_call(ts, "list_gcp_methods", {}))
        assert "buckets.list" in listing["storage/v1"]
        assert "objects.list" not in listing["storage/v1"]

    def test_listing_has_no_duplicates_from_aliases(self):
        ts = GoogleCloudToolset(
            "gcp_test",
            allowed_methods=["storage/v1:storage.buckets.list", "storage/v1:objects.list"],
        )
        listing = json.loads(_call(ts, "list_gcp_methods", {}))
        assert listing["storage/v1"] == ["buckets.list", "objects.list"]

    def test_sensitive_methods_hidden_behind_wildcard(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["secretmanager/v1:*"])
        listing = json.loads(_call(ts, "list_gcp_methods", {}))
        assert "projects.secrets.list" in listing["secretmanager/v1"]
        assert "projects.secrets.versions.access" not in listing["secretmanager/v1"]

    def test_verbatim_sensitive_method_is_listed(self):
        ts = GoogleCloudToolset(
            "gcp_test",
            allowed_methods=["secretmanager/v1:*", "secretmanager/v1:projects.secrets.versions.access"],
        )
        listing = json.loads(_call(ts, "list_gcp_methods", {}))
        assert "projects.secrets.versions.access" in listing["secretmanager/v1"]

    def test_api_outside_allow_list_raises_model_retry(self):
        ts = _make_toolset()
        with pytest.raises(ModelRetry, match="not in this toolset's allowed methods"):
            _call(ts, "list_gcp_methods", {"api": "bigquery/v2"})


class TestGoogleCloudToolsetDescribeMethod:
    def test_returns_parameters_with_required_members(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        described = json.loads(
            _call(ts, "describe_gcp_method", {"api": "storage/v1", "method": "objects.list"})
        )
        assert described["method"] == "objects.list"
        assert described["http_method"] == "GET"
        assert described["parameters"]["bucket"]["required"] is True
        assert described["parameters"]["prefix"]["location"] == "query"
        assert described["scopes"]

    def test_request_body_schema_for_insert_method(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["bigquery/v2:jobs.insert"])
        described = json.loads(
            _call(ts, "describe_gcp_method", {"api": "bigquery/v2", "method": "jobs.insert"})
        )
        assert described["request_body"] is not None
        assert described["response"] is not None

    def test_resolves_doc_style_method_id(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        described = json.loads(
            _call(ts, "describe_gcp_method", {"api": "storage/v1", "method": "storage.objects.list"})
        )
        assert described["method"] == "objects.list"

    def test_disallowed_method_raises_model_retry(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        with pytest.raises(ModelRetry, match="not in this toolset's allowed methods"):
            _call(ts, "describe_gcp_method", {"api": "storage/v1", "method": "buckets.delete"})


class TestGoogleCloudToolsetCallGcp:
    def test_executes_method_and_returns_json(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        _install_mock_hook(ts, project_id=None)
        _, bound, request = _install_mock_service(
            ts,
            "storage",
            "v1",
            ["objects"],
            "list",
            response={"kind": "storage#objects", "items": [{"name": "a.parquet"}]},
        )

        result = json.loads(
            _call(
                ts,
                "call_gcp",
                {"api": "storage/v1", "method": "objects.list", "parameters": {"bucket": "b"}},
            )
        )
        bound.assert_called_once_with(bucket="b")
        request.execute.assert_called_once_with()
        assert result["items"] == [{"name": "a.parquet"}]

    def test_body_is_passed_as_keyword(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["bigquery/v2:jobs.insert"])
        _install_mock_hook(ts, project_id=None)
        _, bound, _ = _install_mock_service(
            ts, "bigquery", "v2", ["jobs"], "insert", response={"jobReference": {}}
        )

        _call(
            ts,
            "call_gcp",
            {
                "api": "bigquery/v2",
                "method": "jobs.insert",
                "parameters": {"projectId": "p"},
                "body": {"configuration": {"query": {}}},
            },
        )
        bound.assert_called_once_with(projectId="p", body={"configuration": {"query": {}}})

    def test_traverses_nested_resource_path(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["secretmanager/v1:projects.secrets.list"])
        _install_mock_hook(ts, project_id=None)
        _, bound, _ = _install_mock_service(
            ts, "secretmanager", "v1", ["projects", "secrets"], "list", response={"secrets": []}
        )

        result = json.loads(
            _call(
                ts,
                "call_gcp",
                {
                    "api": "secretmanager/v1",
                    "method": "projects.secrets.list",
                    "parameters": {"parent": "projects/p"},
                },
            )
        )
        bound.assert_called_once_with(parent="projects/p")
        assert result == {"secrets": []}

    def test_dotted_query_parameters_are_passed_as_client_keyword_names(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["monitoring/v3:projects.timeSeries.list"])
        _install_mock_hook(ts, project_id=None)
        _, bound, _ = _install_mock_service(
            ts, "monitoring", "v3", ["projects", "timeSeries"], "list", response={"timeSeries": []}
        )

        _call(
            ts,
            "call_gcp",
            {
                "api": "monitoring/v3",
                "method": "projects.timeSeries.list",
                "parameters": {
                    "name": "projects/p",
                    "filter": 'metric.type = "bigquery.googleapis.com/query/count"',
                    "interval.startTime": "2026-07-20T00:00:00Z",
                    "interval.endTime": "2026-07-20T01:00:00Z",
                },
            },
        )

        bound.assert_called_once_with(
            name="projects/p",
            filter='metric.type = "bigquery.googleapis.com/query/count"',
            interval_startTime="2026-07-20T00:00:00Z",
            interval_endTime="2026-07-20T01:00:00Z",
        )

    def test_leading_underscore_parameters_are_passed_as_client_keyword_names(self):
        ts = GoogleCloudToolset(
            "gcp_test", allowed_methods=["healthcare/v1:projects.locations.datasets.fhirStores.fhir.history"]
        )
        _install_mock_hook(ts, project_id=None)
        _, bound, _ = _install_mock_service(
            ts,
            "healthcare",
            "v1",
            ["projects", "locations", "datasets", "fhirStores", "fhir"],
            "history",
            response={"entry": []},
        )

        _call(
            ts,
            "call_gcp",
            {
                "api": "healthcare/v1",
                "method": "projects.locations.datasets.fhirStores.fhir.history",
                "parameters": {
                    "name": "projects/p/locations/us/datasets/d/fhirStores/s/fhir/Patient/123",
                    "_count": 10,
                    "_page_token": "next",
                },
            },
        )

        bound.assert_called_once_with(
            name="projects/p/locations/us/datasets/d/fhirStores/s/fhir/Patient/123",
            x_count=10,
            x_page_token="next",
        )

    def test_rejects_conflicting_rest_and_client_parameter_names(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["monitoring/v3:projects.timeSeries.list"])
        _install_mock_hook(ts, project_id=None)
        _, bound, _ = _install_mock_service(
            ts, "monitoring", "v3", ["projects", "timeSeries"], "list", response={"timeSeries": []}
        )

        with pytest.raises(ModelRetry, match="conflicts with another parameter"):
            _call(
                ts,
                "call_gcp",
                {
                    "api": "monitoring/v3",
                    "method": "projects.timeSeries.list",
                    "parameters": {
                        "name": "projects/p",
                        "interval.startTime": "2026-07-20T00:00:00Z",
                        "interval_startTime": "2026-07-20T01:00:00Z",
                    },
                },
            )

        bound.assert_not_called()

    def test_follows_pagination_up_to_max_pages(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"], max_pages=2)
        _install_mock_hook(ts, project_id=None)
        node, _, _ = _install_mock_service(
            ts,
            "storage",
            "v1",
            ["objects"],
            "list",
            response={"items": [{"name": "page1"}], "nextPageToken": "t1"},
        )
        page2_request = MagicMock()
        page2_request.execute.return_value = {"items": [{"name": "page2"}], "nextPageToken": "t2"}
        node.list_next.side_effect = [page2_request, MagicMock()]

        result = json.loads(
            _call(
                ts,
                "call_gcp",
                {"api": "storage/v1", "method": "objects.list", "parameters": {"bucket": "b"}},
            )
        )
        assert result["page_count"] == 2
        assert result["more_pages_available"] is True
        assert [p["items"][0]["name"] for p in result["pages"]] == ["page1", "page2"]
        assert node.list_next.call_count == 1

    @pytest.mark.parametrize("media_param", ["media_body", "media_mime_type"])
    def test_rejects_media_upload_kwargs(self, media_param):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.insert"])
        with pytest.raises(ModelRetry, match="Media upload"):
            _call(
                ts,
                "call_gcp",
                {
                    "api": "storage/v1",
                    "method": "objects.insert",
                    "parameters": {"bucket": "b", media_param: "/etc/passwd"},
                },
            )
        assert ts._services == {}

    @pytest.mark.parametrize(
        ("args", "match"),
        [
            ({"method": "objects.list"}, "'api' argument is required"),
            ({"api": "storage/v1"}, "'method' argument is required"),
            ({"api": 42, "method": "objects.list"}, "'api' argument is required"),
            (
                {"api": "storage/v1", "method": "objects.list", "parameters": ["bucket"]},
                "'parameters' argument must be a JSON object",
            ),
            (
                {"api": "storage/v1", "method": "objects.list", "body": "not-a-dict"},
                "'body' argument must be a JSON object",
            ),
        ],
    )
    def test_invalid_tool_args_raise_clear_model_retry(self, args, match):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        with pytest.raises(ModelRetry, match=match):
            _call(ts, "call_gcp", args)

    def test_rejects_media_download(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.get"])
        with pytest.raises(ModelRetry, match="alt=media"):
            _call(
                ts,
                "call_gcp",
                {
                    "api": "storage/v1",
                    "method": "objects.get",
                    "parameters": {"bucket": "b", "object": "o", "alt": "media"},
                },
            )
        assert ts._services == {}

    def test_disallowed_method_raises_model_retry_without_client(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        with pytest.raises(ModelRetry, match="not in this toolset's allowed methods"):
            _call(ts, "call_gcp", {"api": "storage/v1", "method": "buckets.delete"})
        assert ts._services == {}

    def test_api_error_raises_model_retry(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"])
        _install_mock_hook(ts, project_id=None)
        _, _, request = _install_mock_service(ts, "storage", "v1", ["objects"], "list")
        request.execute.side_effect = Exception("403 storage.objects.list access denied")

        with pytest.raises(ModelRetry, match="access denied"):
            _call(
                ts,
                "call_gcp",
                {"api": "storage/v1", "method": "objects.list", "parameters": {"bucket": "b"}},
            )

    def test_large_response_is_truncated(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["storage/v1:objects.list"], max_output_bytes=200)
        _install_mock_hook(ts, project_id=None)
        _install_mock_service(
            ts, "storage", "v1", ["objects"], "list", response={"items": [{"name": "x" * 50}] * 50}
        )

        result = json.loads(
            _call(
                ts,
                "call_gcp",
                {"api": "storage/v1", "method": "objects.list", "parameters": {"bucket": "b"}},
            )
        )
        assert result["truncated"] is True
        assert len(result["data"]) == 200

    def test_unknown_tool_raises_value_error(self):
        ts = _make_toolset()
        with pytest.raises(ValueError, match="Unknown tool"):
            _call(ts, "use_gcp", {})

    @patch("airflow.providers.common.ai.toolsets.google.build", autospec=True)
    @patch("airflow.providers.common.ai.toolsets.google._get_google_base_hook_class", autospec=True)
    def test_service_built_with_hook_credentials(self, mock_get_hook_cls, mock_build):
        mock_hook_cls = mock_get_hook_cls.return_value
        hook = mock_hook_cls.return_value
        hook.project_id = None
        service = MagicMock()
        node = service.objects.return_value
        node.list.return_value.execute.return_value = {"items": []}
        node.list_next.return_value = None
        mock_build.return_value = service

        ts = GoogleCloudToolset(
            "gcp_prod",
            allowed_methods=["storage/v1:objects.list"],
            impersonation_chain="sa@proj.iam.gserviceaccount.com",
        )
        _call(
            ts,
            "call_gcp",
            {"api": "storage/v1", "method": "objects.list", "parameters": {"bucket": "b"}},
        )
        mock_hook_cls.assert_called_once_with(
            gcp_conn_id="gcp_prod", impersonation_chain="sa@proj.iam.gserviceaccount.com"
        )
        mock_build.assert_called_once_with(
            "storage",
            "v1",
            credentials=hook.get_credentials.return_value,
            cache_discovery=False,
            static_discovery=True,
        )


class TestGoogleCloudToolsetProjectGuard:
    def _make_compute_toolset(self, **kwargs):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["compute/v1:instances.list"], **kwargs)
        _install_mock_hook(ts, project_id="my-proj")
        _install_mock_service(ts, "compute", "v1", ["instances"], "list", response={"items": []})
        return ts

    def _make_pubsub_toolset(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["pubsub/v1:projects.topics.list"])
        _install_mock_hook(ts, project_id="my-proj")
        _install_mock_service(ts, "pubsub", "v1", ["projects", "topics"], "list", response={"topics": []})
        return ts

    def test_fills_missing_project_from_connection(self):
        ts = self._make_compute_toolset()
        _call(
            ts,
            "call_gcp",
            {"api": "compute/v1", "method": "instances.list", "parameters": {"zone": "z"}},
        )
        node = ts._services[("compute", "v1")].instances.return_value
        node.list.assert_called_once_with(zone="z", project="my-proj")

    def test_rejects_mismatched_project(self):
        ts = self._make_compute_toolset()
        with pytest.raises(ModelRetry, match="must be 'my-proj'"):
            _call(
                ts,
                "call_gcp",
                {
                    "api": "compute/v1",
                    "method": "instances.list",
                    "parameters": {"zone": "z", "project": "other-proj"},
                },
            )

    def test_enforce_project_false_allows_mismatch(self):
        ts = self._make_compute_toolset(enforce_project=False)
        _call(
            ts,
            "call_gcp",
            {
                "api": "compute/v1",
                "method": "instances.list",
                "parameters": {"zone": "z", "project": "other-proj"},
            },
        )
        node = ts._services[("compute", "v1")].instances.return_value
        node.list.assert_called_once_with(zone="z", project="other-proj")

    def test_fills_project_path_parameter_from_connection(self):
        ts = self._make_pubsub_toolset()
        _call(ts, "call_gcp", {"api": "pubsub/v1", "method": "projects.topics.list"})
        node = ts._services[("pubsub", "v1")].projects.return_value.topics.return_value
        node.list.assert_called_once_with(project="projects/my-proj")

    def test_normalizes_bare_project_path_parameter(self):
        ts = self._make_pubsub_toolset()
        _call(
            ts,
            "call_gcp",
            {
                "api": "pubsub/v1",
                "method": "projects.topics.list",
                "parameters": {"project": "my-proj"},
            },
        )
        node = ts._services[("pubsub", "v1")].projects.return_value.topics.return_value
        node.list.assert_called_once_with(project="projects/my-proj")

    def test_rejects_mismatched_project_path_parameter(self):
        ts = self._make_pubsub_toolset()
        with pytest.raises(ModelRetry, match="must be 'projects/my-proj'"):
            _call(
                ts,
                "call_gcp",
                {
                    "api": "pubsub/v1",
                    "method": "projects.topics.list",
                    "parameters": {"project": "projects/other-proj"},
                },
            )

    def test_rejects_mismatched_project_in_path_parameter(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["secretmanager/v1:projects.secrets.list"])
        _install_mock_hook(ts, project_id="my-proj")
        _install_mock_service(ts, "secretmanager", "v1", ["projects", "secrets"], "list")

        with pytest.raises(ModelRetry, match="pinned to 'my-proj'"):
            _call(
                ts,
                "call_gcp",
                {
                    "api": "secretmanager/v1",
                    "method": "projects.secrets.list",
                    "parameters": {"parent": "projects/other-proj"},
                },
            )

    def test_no_project_on_connection_skips_guard(self):
        ts = GoogleCloudToolset("gcp_test", allowed_methods=["compute/v1:instances.list"])
        _install_mock_hook(ts, project_id=None)
        _install_mock_service(ts, "compute", "v1", ["instances"], "list", response={"items": []})
        _call(
            ts,
            "call_gcp",
            {
                "api": "compute/v1",
                "method": "instances.list",
                "parameters": {"zone": "z", "project": "any-proj"},
            },
        )
        node = ts._services[("compute", "v1")].instances.return_value
        node.list.assert_called_once_with(zone="z", project="any-proj")


class TestGetAvailableMethods:
    def test_returns_ready_to_use_entries(self):
        methods = GoogleCloudToolset.get_available_methods("storage/v1")
        assert "storage/v1:objects.list" in methods
        assert "storage/v1:buckets.list" in methods
        assert methods == sorted(methods)

    @pytest.mark.parametrize("bad", ["storage", "nosuchapi/v1"])
    def test_rejects_invalid_api(self, bad):
        with pytest.raises(ValueError, match="not in the bundled discovery documents"):
            GoogleCloudToolset.get_available_methods(bad)
