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

import datetime

import pytest

from airflowctl.api.client import Client, ClientKind
from airflowctl.api.datamodels.generated import (
    AssetEventResponse,
    AssetResponse,
    CreateAssetBody,
    CreateAssetEventsBody,
)


def make_api_client(base_url: str = "http://test-server", token: str = "test-token") -> Client:
    """Get a client for testing"""
    return Client(base_url=base_url, token=token, kind=ClientKind.CLI)


class TestAssetsOperationsMinimal:
    """Minimal tests for AssetsOperations without httpx dependencies."""

    def test_create_hits_assets_endpoint(self, monkeypatch):
        """Test that create() posts to /assets with correct payload."""
        called = {}

        def mock_post(self, url, json=None, **kwargs):
            called['url'] = url
            called['json'] = json
            called['method'] = 'POST'

            # Verify the endpoint and payload
            assert url.endswith("assets")
            assert json == {"name": "test_asset", "uri": "s3://bucket/test", "group": None, "extra": None}

            # Return mock response
            mock_response = AssetResponse(
                id=1,
                name="test_asset",
                uri="s3://bucket/test",
                group="default",
                extra=None,
                created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                scheduled_dags=[],
                producing_tasks=[],
                consuming_tasks=[],
                aliases=[],
            )

    asset_event_response = AssetEventResponse(
        id=asset_id,
        asset_id=asset_id,
        uri="uri",
        name="asset",
        group="group",
        extra=None,
        source_task_id="task_id",
        source_dag_id=dag_id,
        source_run_id="manual__2025-01-01T00:00:00+00:00",
        source_map_index=1,
        created_dagruns=[assets_dag_reference],
        timestamp=datetime.datetime(2025, 1, 1, 0, 0, 0),
    )

    def test_get_asset(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/assets/{self.asset_id}"
            return httpx.Response(200, json=json.loads(self.asset_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get(self.asset_id)
        assert response == self.asset_response

    def test_get_by_alias(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/assets/aliases/{self.asset_id}"
            return httpx.Response(200, json=json.loads(self.asset_alias_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get_by_alias(self.asset_id)
        assert response == self.asset_alias_response

    def test_list(self):
        assets_collection_response = AssetCollectionResponse(
            assets=[self.asset_response],
            total_entries=1,
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            return httpx.Response(200, json=json.loads(assets_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.list()
        assert response == assets_collection_response

    def test_list_by_alias(self):
        assets_collection_response = AssetAliasCollectionResponse(
            asset_aliases=[self.asset_alias_response],
            total_entries=1,
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets/aliases"
            return httpx.Response(200, json=json.loads(assets_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.list_by_alias()
        assert response == assets_collection_response

    def test_create(self):
        asset_create_body = CreateAssetBody(
            name="test_asset",
            uri="test_uri",
            group="test_group",
            extra={"test": "extra"}
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            return httpx.Response(200, json=json.loads(self.asset_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.create(asset_body=asset_create_body)
        assert response == self.asset_response

    def test_create_event(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets/events"
            return httpx.Response(200, json=json.loads(self.asset_event_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.create_event(asset_event_body=self.asset_create_event_body)
        assert response == self.asset_event_response

    def test_materialize(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/assets/{self.asset_id}/materialize"
            return httpx.Response(200, json=json.loads(self.dag_run_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.materialize(asset_id=self.asset_id)
        assert response == self.dag_run_response

    def test_get_queued_events(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/assets/{self.asset_id}/queuedEvents"
            return httpx.Response(
                200, json=json.loads(self.asset_queued_event_collection_response.model_dump_json())
            )

            class MockResponse:
                def __init__(self, data):
                    self.status_code = 201
                    self._data = data
                    self.content = data.model_dump_json().encode('utf-8')

                def json(self):
                    return json.loads(self._data.model_dump_json())

            return MockResponse(mock_response)

        # Patch the client's post method
        monkeypatch.setattr(Client, "post", mock_post)

        client = make_api_client()
        response = client.assets.create(asset_body=CreateAssetBody(name="test_asset", uri="s3://bucket/test"))

        assert isinstance(response, AssetResponse)
        assert response.name == "test_asset"
        assert response.uri == "s3://bucket/test"
        assert called['method'] == 'POST'
        assert called['url'].endswith('assets')

    def test_create_event_hits_assets_events_endpoint(self, monkeypatch):
        """Test that create_event() posts to /assets/events with correct payload."""
        called = {}

        def mock_post(self, url, json=None, **kwargs):
            called['url'] = url
            called['json'] = json
            called['method'] = 'POST'

            # Verify the endpoint and payload
            assert url.endswith("assets/events")
            assert json == {"asset_id": 1, "extra": {}}

            # Return mock response
            mock_response = AssetEventResponse(
                id=1,
                asset_id=1,
                extra={"from_rest_api": True},
                timestamp=datetime.datetime(2025, 1, 1, 0, 0, 0),
                source_task_id=None,
                source_dag_id=None,
                source_run_id=None,
                source_map_index=-1,
                created_dagruns=[],
            )

            class MockResponse:
                def __init__(self, data):
                    self.status_code = 201
                    self._data = data
                    self.content = data.model_dump_json().encode('utf-8')

                def json(self):
                    return json.loads(self._data.model_dump_json())

            return MockResponse(mock_response)

        # Patch the client's post method
        monkeypatch.setattr(Client, "post", mock_post)

        client = make_api_client()
        response = client.assets.create_event(asset_event_body=CreateAssetEventsBody(asset_id=1, extra=None))

        assert isinstance(response, AssetEventResponse)
        assert response.asset_id == 1
        assert response.extra == {"from_rest_api": True}
        assert called['method'] == 'POST'
        assert called['url'].endswith('assets/events')

    def test_create_event_with_extra_preserves_structure(self, monkeypatch):
        """Test that create_event() preserves extra structure when provided."""
        called = {}

        def mock_post(self, url, json=None, **kwargs):
            called['url'] = url
            called['json'] = json
            called['method'] = 'POST'

            # Verify the endpoint and payload
            assert url.endswith("assets/events")
            assert json == {"asset_id": 42, "extra": {"test": "data", "nested": {"key": "value"}}}

            # Return mock response
            mock_response = AssetEventResponse(
                id=42,
                asset_id=42,
                extra={"test": "data", "nested": {"key": "value"}, "from_rest_api": True},
                timestamp=datetime.datetime(2025, 1, 1, 0, 0, 0),
                source_task_id=None,
                source_dag_id=None,
                source_run_id=None,
                source_map_index=-1,
                created_dagruns=[],
            )

            class MockResponse:
                def __init__(self, data):
                    self.status_code = 201
                    self._data = data
                    self.content = data.model_dump_json().encode('utf-8')

                def json(self):
                    return json.loads(self._data.model_dump_json())

            return MockResponse(mock_response)

        # Patch the client's post method
        monkeypatch.setattr(Client, "post", mock_post)

        client = make_api_client()
        response = client.assets.create_event(asset_event_body=CreateAssetEventsBody(asset_id=42, extra={"test": "data", "nested": {"key": "value"}}))

        assert isinstance(response, AssetEventResponse)
        assert response.asset_id == 42
        expected_extra = {"test": "data", "nested": {"key": "value"}, "from_rest_api": True}
        assert response.extra == expected_extra
        assert called['method'] == 'POST'
        assert called['url'].endswith('assets/events')

    @pytest.mark.parametrize("extra", [None, {}, {"k": "v"}])
    def test_create_event_includes_extra(self, monkeypatch, extra):
        """Test that create_event() always includes extra field."""
        called = {}

        def mock_post(self, url, json=None, **kwargs):
            called['url'] = url
            called['json'] = json
            called['method'] = 'POST'

            # Verify the endpoint and payload
            assert url.endswith("assets/events")
            expected_extra = {} if extra in (None, {}) else extra
            assert json == {"asset_id": 1, "extra": expected_extra}

            # Return mock response
            mock_response = AssetEventResponse(
                id=1,
                asset_id=1,
                extra={**expected_extra, "from_rest_api": True},
                timestamp=datetime.datetime(2025, 1, 1, 0, 0, 0),
                source_task_id=None,
                source_dag_id=None,
                source_run_id=None,
                source_map_index=-1,
                created_dagruns=[],
            )

            class MockResponse:
                def __init__(self, data):
                    self.status_code = 201
                    self._data = data
                    self.content = data.model_dump_json().encode('utf-8')

                def json(self):
                    return json.loads(self._data.model_dump_json())

            return MockResponse(mock_response)

        # Patch the client's post method
        monkeypatch.setattr(Client, "post", mock_post)

        client = make_api_client()
        response = client.assets.create_event(asset_event_body=CreateAssetEventsBody(asset_id=1, extra=extra))

        assert isinstance(response, AssetEventResponse)
        assert response.asset_id == 1
        expected_extra = {} if extra in (None, {}) else extra
        expected_extra["from_rest_api"] = True
        assert response.extra == expected_extra
        assert called['method'] == 'POST'
        assert called['url'].endswith('assets/events')
# Clean version - no conflict markers
