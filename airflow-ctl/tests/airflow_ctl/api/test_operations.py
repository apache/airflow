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
import json
import uuid
from math import ceil
from typing import TYPE_CHECKING
from unittest.mock import Mock

import httpx
import pytest
from pydantic import BaseModel

from airflowctl.api.client import Client, ClientKind
from airflowctl.api.datamodels.auth_generated import LoginBody, LoginResponse
from airflowctl.api.datamodels.generated import (
    AssetAliasCollectionResponse,
    AssetAliasResponse,
    AssetCollectionResponse,
    AssetEventResponse,
    AssetResponse,
    BackfillCollectionResponse,
    BackfillPostBody,
    BackfillResponse,
    BulkActionOnExistence,
    BulkActionResponse,
    BulkBodyConnectionBody,
    BulkBodyPoolBody,
    BulkBodyVariableBody,
    BulkCreateActionConnectionBody,
    BulkCreateActionPoolBody,
    BulkCreateActionVariableBody,
    BulkResponse,
    Config,
    ConfigOption,
    ConfigSection,
    ConnectionBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestResponse,
    CreateAssetEventsBody,
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
    DagRunAssetReference,
    DAGRunCollectionResponse,
    DAGRunResponse,
    DagRunState,
    DagRunTriggeredByType,
    DagRunType,
    DagStatsCollectionResponse,
    DagStatsResponse,
    DagStatsStateResponse,
    DAGTagCollectionResponse,
    DAGVersionCollectionResponse,
    DagVersionResponse,
    DAGWarningCollectionResponse,
    DAGWarningResponse,
    DagWarningType,
    ImportErrorCollectionResponse,
    ImportErrorResponse,
    JobCollectionResponse,
    JobResponse,
    PoolBody,
    PoolCollectionResponse,
    PoolResponse,
    ProviderCollectionResponse,
    ProviderResponse,
    QueuedEventCollectionResponse,
    QueuedEventResponse,
    ReprocessBehavior,
    TriggerDAGRunPostBody,
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
    VersionInfo,
)
from airflowctl.api.operations import BaseOperations
from airflowctl.exceptions import AirflowCtlConnectionException

if TYPE_CHECKING:
    from pydantic import NonNegativeInt


def make_api_client(
    transport: httpx.MockTransport | None = None,
    base_url: str = "test://server",
    token: str = "",
    kind: ClientKind = ClientKind.CLI,
) -> Client:
    """Get a client with a custom transport"""
    return Client(base_url=base_url, transport=transport, token=token, kind=kind)


class HelloResponse(BaseModel):
    name: str


class HelloCollectionResponse(BaseModel):
    hellos: list[HelloResponse]
    total_entries: int


class TestBaseOperations:
    def test_server_connection_refused(self):
        client = make_api_client(base_url="http://localhost")
        with pytest.raises(
            AirflowCtlConnectionException, match="Connection refused. Is the API server running?"
        ):
            client.connections.get("1")

    @pytest.mark.parametrize(
        "total_entries, limit, expected_response",
        [
            (1, 50, (HelloCollectionResponse(hellos=[HelloResponse(name="hello")], total_entries=1))),
            (
                150,
                50,
                (
                    HelloCollectionResponse(
                        hellos=[
                            HelloResponse(name="hello"),
                        ]
                        * 150,
                        total_entries=150,
                    )
                ),
            ),
            (
                90,
                50,
                (HelloCollectionResponse(hellos=[HelloResponse(name="hello")] * 90, total_entries=90)),
            ),
        ],
    )
    def test_execute_list(self, total_entries, limit, expected_response):
        get_response_mock = []

        mock_client = Mock()
        mock_client.get.side_effect = get_response_mock
        base_operation = BaseOperations(client=mock_client)

        nb_of_pages = ceil(total_entries / limit)
        for page in range(nb_of_pages):
            if page == nb_of_pages - 1 and (remaining_entries := total_entries % limit) > 0:
                # partial page
                get_response_mock.append(
                    Mock(
                        content=json.dumps(
                            {
                                "hellos": [{"name": "hello"}] * remaining_entries,
                                "total_entries": total_entries,
                            }
                        )
                    )
                )
                continue
            # page is full
            get_response_mock.append(
                Mock(
                    content=json.dumps(
                        {
                            "hellos": [{"name": "hello"}] * limit,
                            "total_entries": total_entries,
                        }
                    )
                )
            )

        response = base_operation.execute_list(
            path="some_fake_path", data_model=HelloCollectionResponse, limit=limit
        )

        assert expected_response == response


class TestAssetsOperations:
    asset_id: int = 1
    dag_id: str = "dag_id"
    before: str = "2024-12-31T23:59:59+00:00"
    asset_response = AssetResponse(
        id=asset_id,
        name="asset",
        uri="asset_uri",
        extra={"extra": "extra"},
        created_at=datetime.datetime(2024, 12, 31, 23, 59, 59),
        updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        scheduled_dags=[],
        producing_tasks=[],
        consuming_tasks=[],
        aliases=[],
        group="group",
    )
    asset_alias_response = AssetAliasResponse(
        id=asset_id,
        name="asset",
        group="group",
    )

    asset_queued_event_response = QueuedEventResponse(
        dag_id=dag_id,
        asset_id=asset_id,
        created_at=datetime.datetime(2024, 12, 31, 23, 59, 59),
        dag_display_name=dag_id,
    )

    asset_queued_event_collection_response = QueuedEventCollectionResponse(
        queued_events=[asset_queued_event_response],
        total_entries=1,
    )

    dag_run_response = DAGRunResponse(
        dag_display_name=dag_id,
        dag_run_id=dag_id,
        dag_id=dag_id,
        logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        queued_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        start_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        last_scheduling_decision=datetime.datetime(2025, 1, 1, 0, 0, 0),
        run_type=DagRunType.MANUAL,
        run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        state=DagRunState.RUNNING,
        triggered_by=DagRunTriggeredByType.UI,
        conf=None,
        note=None,
        dag_versions=[
            DagVersionResponse(
                id=uuid.uuid4(),
                version_number=1,
                dag_id=dag_id,
                bundle_name="bundle_name",
                bundle_version="1",
                created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                dag_display_name=dag_id,
            )
        ],
    )

    asset_create_event_body = CreateAssetEventsBody(asset_id=asset_id, extra=None)

    assets_dag_reference = DagRunAssetReference(
        run_id="manual__2025-01-01T00:00:00+00:00",
        dag_id=dag_id,
        logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        start_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        state="RUNNING",
        data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
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

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get_queued_events(asset_id=self.asset_id)
        assert response == self.asset_queued_event_collection_response

    def test_get_dag_queued_events(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/dags/{self.dag_id}/assets/queuedEvents"
            return httpx.Response(
                200, json=json.loads(self.asset_queued_event_collection_response.model_dump_json())
            )

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get_dag_queued_events(dag_id=self.dag_id, before=self.before)
        assert response == self.asset_queued_event_collection_response

    def test_get_dag_queued_event(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/dags/{self.dag_id}/assets/{self.asset_id}/queuedEvents"
            return httpx.Response(200, json=json.loads(self.asset_queued_event_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.get_dag_queued_event(dag_id=self.dag_id, asset_id=self.asset_id)
        assert response == self.asset_queued_event_response

    @pytest.mark.parametrize(
        "payload,expected_status,expected_response_fields",
        [
            # Success cases
            (
                {"name": "test_asset", "uri": "s3://bucket/test"},
                201,
                {"name": "test_asset", "uri": "s3://bucket/test", "group": None, "extra": None},
            ),
            (
                {"name": "full_asset", "uri": "gs://bucket/data", "group": "raw", "extra": {"owner": "data-team"}},
                201,
                {"name": "full_asset", "uri": "gs://bucket/data", "group": "raw", "extra": {"owner": "data-team"}},
            ),
            (
                {"name": "azure_asset", "uri": "wasb://container@account.blob.core.windows.net/path"},
                201,
                {"name": "azure_asset", "uri": "wasb://container@account.blob.core.windows.net/path"},
            ),
        ],
        ids=["minimal_payload", "full_payload", "azure_uri"],
    )
    def test_create_asset_success_cases(self, payload, expected_status, expected_response_fields):
        """Test successful asset creation with various payloads."""
        # Create a mock response based on the payload
        mock_response = AssetResponse(
            id=1,
            name=payload["name"],
            uri=payload["uri"],
            group=payload.get("group"),
            extra=payload.get("extra"),
            created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
            scheduled_dags=[],
            producing_tasks=[],
            consuming_tasks=[],
            aliases=[],
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            
            # Verify request payload
            request_data = json.loads(request.content)
            for key, value in payload.items():
                assert request_data[key] == value
            
            return httpx.Response(expected_status, json=json.loads(mock_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.create(asset_body=payload)
        
        # Verify response structure
        assert isinstance(response, AssetResponse)
        assert response.id == 1
        assert response.name == expected_response_fields["name"]
        assert response.uri == expected_response_fields["uri"]
        assert response.group == expected_response_fields.get("group")
        assert response.extra == expected_response_fields.get("extra")

    @pytest.mark.parametrize(
        "payload,expected_status,expected_error_pattern",
        [
            # 400 Bad Request cases
            ({"name": "", "uri": "s3://bucket/key"}, 400, "name"),
            ({"name": "valid_name", "uri": ""}, 400, "uri"),
            ({"name": "x" * 1501, "uri": "s3://bucket/key"}, 400, "name"),
            ({"name": "valid_name", "uri": "x" * 1501}, 400, "uri"),
            
            # 422 Unprocessable Entity cases
            ({"name": 123, "uri": "s3://bucket/key"}, 422, "name"),
            ({"name": "valid_name", "uri": ["not-a-string"]}, 422, "uri"),
            ({"name": None, "uri": "s3://bucket/key"}, 422, "name"),
            ({"name": "valid_name", "uri": None}, 422, "uri"),
            ({"extra": "not-a-dict", "name": "valid_name", "uri": "s3://bucket/key"}, 422, "extra"),
            ({"group": 123, "name": "valid_name", "uri": "s3://bucket/key"}, 422, "group"),
            
            # Missing required fields
            ({"uri": "s3://bucket/key"}, 422, "name"),
            ({"name": "valid_name"}, 422, "uri"),
            ({}, 422, "name"),
        ],
        ids=[
            "empty_name", "empty_uri", "name_too_long", "uri_too_long",
            "name_not_string", "uri_not_string", "name_none", "uri_none",
            "extra_not_dict", "group_not_string", "missing_name", "missing_uri", "missing_both"
        ],
    )
    def test_create_asset_validation_errors(self, payload, expected_status, expected_error_pattern):
        """Test validation error cases with various invalid payloads."""
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            
            # Return error response
            error_detail = f"Validation error: {expected_error_pattern} is invalid"
            return httpx.Response(
                expected_status, 
                json={"detail": error_detail}
            )

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.assets.create(asset_body=payload)
        
        assert exc_info.value.response.status_code == expected_status
        error_response = exc_info.value.response.json()
        assert expected_error_pattern in error_response["detail"].lower()

    def test_create_asset_conflict_409(self):
        """Test that creating duplicate assets returns 409 Conflict."""
        payload = {"name": "duplicate_asset", "uri": "s3://bucket/duplicate"}
        
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            
            # First call succeeds, second call returns 409
            if not hasattr(handle_request, 'call_count'):
                handle_request.call_count = 0
            handle_request.call_count += 1
            
            if handle_request.call_count == 1:
                # First call - success
                mock_response = AssetResponse(
                    id=1,
                    name=payload["name"],
                    uri=payload["uri"],
                    group=None,
                    extra=None,
                    created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                    updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                    scheduled_dags=[],
                    producing_tasks=[],
                    consuming_tasks=[],
                    aliases=[],
                )
                return httpx.Response(201, json=json.loads(mock_response.model_dump_json()))
            else:
                # Second call - conflict
                return httpx.Response(409, json={"detail": "Asset with this name and URI already exists"})

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        
        # First creation should succeed
        response1 = client.assets.create(asset_body=payload)
        assert isinstance(response1, AssetResponse)
        assert response1.name == payload["name"]
        
        # Second creation should fail with 409
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.assets.create(asset_body=payload)
        
        assert exc_info.value.response.status_code == 409
        error_response = exc_info.value.response.json()
        assert "already exists" in error_response["detail"].lower()

    @pytest.mark.parametrize(
        "expected_status,expected_error_type",
        [
            (401, "Unauthorized"),
            (403, "Forbidden"),
        ],
        ids=["unauthorized_401", "forbidden_403"],
    )
    def test_create_asset_auth_errors(self, expected_status, expected_error_type):
        """Test authentication and authorization error cases."""
        payload = {"name": "test_asset", "uri": "s3://bucket/test"}
        
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            
            return httpx.Response(
                expected_status, 
                json={"detail": f"{expected_error_type}: Access denied"}
            )

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            client.assets.create(asset_body=payload)
        
        assert exc_info.value.response.status_code == expected_status
        error_response = exc_info.value.response.json()
        assert expected_error_type.lower() in error_response["detail"].lower()

    def test_create_asset_request_payload_validation(self):
        """Test that the request payload is properly formatted and sent."""
        payload = {
            "name": "payload_test",
            "uri": "s3://bucket/payload-test",
            "group": "test-group",
            "extra": {"test": "data", "number": 42}
        }
        
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            assert request.headers["content-type"] == "application/json"
            
            # Verify request payload structure
            request_data = json.loads(request.content)
            assert request_data == payload
            
            # Verify all fields are present
            assert "name" in request_data
            assert "uri" in request_data
            assert "group" in request_data
            assert "extra" in request_data
            
            # Verify field types
            assert isinstance(request_data["name"], str)
            assert isinstance(request_data["uri"], str)
            assert isinstance(request_data["group"], str)
            assert isinstance(request_data["extra"], dict)
            
            mock_response = AssetResponse(
                id=1,
                name=payload["name"],
                uri=payload["uri"],
                group=payload["group"],
                extra=payload["extra"],
                created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                scheduled_dags=[],
                producing_tasks=[],
                consuming_tasks=[],
                aliases=[],
            )
            return httpx.Response(201, json=json.loads(mock_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.create(asset_body=payload)
        
        assert isinstance(response, AssetResponse)
        assert response.name == payload["name"]
        assert response.uri == payload["uri"]
        assert response.group == payload["group"]
        assert response.extra == payload["extra"]

    def test_create_asset_response_structure(self):
        """Test that the response has the correct structure and all required fields."""
        payload = {"name": "structure_test", "uri": "s3://bucket/structure"}
        
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            
            mock_response = AssetResponse(
                id=42,
                name=payload["name"],
                uri=payload["uri"],
                group=None,
                extra=None,
                created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                scheduled_dags=[],
                producing_tasks=[],
                consuming_tasks=[],
                aliases=[],
            )
            return httpx.Response(201, json=json.loads(mock_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.create(asset_body=payload)
        
        # Verify response is AssetResponse instance
        assert isinstance(response, AssetResponse)
        
        # Verify all required fields are present and have correct types
        assert isinstance(response.id, int)
        assert isinstance(response.name, str)
        assert isinstance(response.uri, str)
        assert response.group is None or isinstance(response.group, str)
        assert response.extra is None or isinstance(response.extra, dict)
        assert isinstance(response.created_at, datetime.datetime)
        assert isinstance(response.updated_at, datetime.datetime)
        assert isinstance(response.scheduled_dags, list)
        assert isinstance(response.producing_tasks, list)
        assert isinstance(response.consuming_tasks, list)
        assert isinstance(response.aliases, list)
        
        # Verify specific values
        assert response.id == 42
        assert response.name == payload["name"]
        assert response.uri == payload["uri"]
        assert response.group is None
        assert response.extra is None

    def test_create_asset_with_special_characters(self):
        """Test asset creation with special characters in name and URI."""
        payload = {
            "name": "asset-with-dashes_and_underscores.and.dots",
            "uri": "s3://bucket-name/path/with/special-chars_123",
            "group": "test-group",
            "extra": {"special": "chars", "unicode": "测试", "numbers": 123}
        }
        
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/assets"
            assert request.method == "POST"
            
            # Verify special characters are preserved
            request_data = json.loads(request.content)
            assert request_data["name"] == payload["name"]
            assert request_data["uri"] == payload["uri"]
            assert request_data["group"] == payload["group"]
            assert request_data["extra"] == payload["extra"]
            
            mock_response = AssetResponse(
                id=1,
                name=payload["name"],
                uri=payload["uri"],
                group=payload["group"],
                extra=payload["extra"],
                created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                scheduled_dags=[],
                producing_tasks=[],
                consuming_tasks=[],
                aliases=[],
            )
            return httpx.Response(201, json=json.loads(mock_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.assets.create(asset_body=payload)
        
        assert isinstance(response, AssetResponse)
        assert response.name == payload["name"]
        assert response.uri == payload["uri"]
        assert response.group == payload["group"]
        assert response.extra == payload["extra"]


class TestBackfillOperations:
    backfill_id: NonNegativeInt = 1
    backfill_body = BackfillPostBody(
        dag_id="dag_id",
        from_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
        to_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        run_backwards=False,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.COMPLETED,
        max_active_runs=1,
    )
    backfill_response = BackfillResponse(
        id=backfill_id,
        dag_id="dag_id",
        from_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
        to_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        dag_run_conf={},
        is_paused=False,
        reprocess_behavior=ReprocessBehavior.COMPLETED,
        max_active_runs=1,
        created_at=datetime.datetime(2024, 12, 31, 23, 59, 59),
        completed_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        updated_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        dag_display_name="TEST_DAG_1",
    )
    backfills_collection_response = BackfillCollectionResponse(
        backfills=[backfill_response],
        total_entries=1,
    )

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/backfills"
            return httpx.Response(200, json=json.loads(self.backfill_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.create(backfill=self.backfill_body)
        assert response == self.backfill_response

    def test_create_dry_run(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/backfills/dry_run"
            return httpx.Response(200, json=json.loads(self.backfill_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.create_dry_run(backfill=self.backfill_body)
        assert response == self.backfill_response

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/backfills/{self.backfill_id}"
            return httpx.Response(200, json=json.loads(self.backfill_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.get(self.backfill_id)
        assert response == self.backfill_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/backfills"
            return httpx.Response(200, json=json.loads(self.backfills_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.list(dag_id="dag_id")
        assert response == self.backfills_collection_response

    def test_pause(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/backfills/{self.backfill_id}/pause"
            return httpx.Response(200, json=json.loads(self.backfill_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.pause(self.backfill_id)
        assert response == self.backfill_response

    def test_unpause(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/backfills/{self.backfill_id}/unpause"
            return httpx.Response(200, json=json.loads(self.backfill_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.unpause(self.backfill_id)
        assert response == self.backfill_response

    def test_cancel(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/backfills/{self.backfill_id}/cancel"
            return httpx.Response(200, json=json.loads(self.backfill_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.backfills.cancel(self.backfill_id)
        assert response == self.backfill_response


class TestConfigOperations:
    section: str = "core"
    option: str = "config"

    def test_get(self):
        response_config = Config(
            sections=[
                ConfigSection(
                    name=self.section,
                    options=[
                        ConfigOption(
                            key=self.option,
                            value="config",
                        )
                    ],
                )
            ]
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/config/section/{self.section}/option/{self.option}"
            return httpx.Response(200, json=response_config.model_dump())

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.configs.get(section=self.section, option=self.option)
        assert response == response_config

    def test_list(self):
        response_config = Config(
            sections=[
                ConfigSection(name="section-1", options=[ConfigOption(key="option-1", value="value-1")]),
                ConfigSection(name="section-2", options=[ConfigOption(key="option-2", value="value-2")]),
            ]
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/config"
            return httpx.Response(200, json=response_config.model_dump())

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.configs.list()
        assert response == response_config


class TestConnectionsOperations:
    connection_id: str = "test_connection"
    conn_type: str = "conn_type"
    host: str = "host"
    schema_: str = "schema"
    login: str = "login"
    password: str = "password"
    port: int = 1
    extra: str = json.dumps({"extra": "extra"})
    connection = ConnectionBody(
        connection_id=connection_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port,
        extra=extra,
    )

    connection_response = ConnectionResponse(
        connection_id=connection_id,
        conn_type=conn_type,
        host=host,
        schema_=schema_,
        login=login,
        password=password,
        port=port,
        extra=extra,
    )

    connections_response = ConnectionCollectionResponse(
        connections=[connection_response],
        total_entries=1,
    )

    connection_bulk_body = BulkBodyConnectionBody(
        actions=[
            BulkCreateActionConnectionBody(
                action="create",
                entities=[connection],
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )

    connection_bulk_response = BulkResponse(
        create=BulkActionResponse(success=[connection_id], errors=[]),
        update=None,
        delete=None,
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/connections/{self.connection_id}"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.get(self.connection_id)
        assert response == self.connection_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/connections"
            return httpx.Response(200, json=json.loads(self.connections_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.list()
        assert response == self.connections_response

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/connections"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.create(connection=self.connection)
        assert response == self.connection_response

    def test_bulk(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/connections"
            return httpx.Response(200, json=json.loads(self.connection_bulk_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.bulk(connections=self.connection_bulk_body)
        assert response == self.connection_bulk_response

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/connections/{self.connection_id}"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.delete(self.connection_id)
        assert response == self.connection_id

    def test_update(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/connections/{self.connection_id}"
            return httpx.Response(200, json=json.loads(self.connection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.update(connection=self.connection)
        assert response == self.connection_response

    def test_test(self):
        connection_test_response = ConnectionTestResponse(
            status=True,
            message="message",
        )

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/connections/test"
            return httpx.Response(200, json=json.loads(connection_test_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.connections.test(connection=self.connection)
        assert response == connection_test_response


class TestDagOperations:
    dag_id = "dag_id"
    dag_display_name = "dag_display_name"
    dag_response = DAGResponse(
        dag_id=dag_id,
        dag_display_name=dag_display_name,
        is_paused=False,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        fileloc="fileloc",
        relative_fileloc="relative_fileloc",
        description="description",
        timetable_summary="timetable_summary",
        timetable_description="timetable_description",
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=True,
        has_import_errors=True,
        next_dagrun_logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        owners=["apache-airflow"],
        file_token="file_token",
        bundle_name="bundle_name",
        is_stale=False,
    )

    dag_details_response = DAGDetailsResponse(
        dag_id=dag_id,
        dag_display_name="dag_display_name",
        is_paused=False,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        fileloc="fileloc",
        relative_fileloc="relative_fileloc",
        description="description",
        timetable_summary="timetable_summary",
        timetable_description="timetable_description",
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=True,
        has_import_errors=True,
        next_dagrun_logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        owners=["apache-airflow"],
        catchup=False,
        dag_run_timeout=datetime.timedelta(days=1),
        asset_expression=None,
        doc_md=None,
        start_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        is_paused_upon_creation=False,
        params={},
        render_template_as_native_obj=True,
        template_search_path=[],
        timezone="timezone",
        last_parsed=datetime.datetime(2024, 12, 31, 23, 59, 59),
        file_token="file_token",
        concurrency=1,
        bundle_name="bundle_name",
        is_stale=False,
    )

    dag_tag_collection_response = DAGTagCollectionResponse(
        tags=["tag"],
        total_entries=1,
    )

    dag_collection_response = DAGCollectionResponse(
        dags=[dag_response],
        total_entries=1,
    )

    import_error_response = ImportErrorResponse(
        import_error_id=0,
        timestamp=datetime.datetime(2025, 1, 1, 0, 0, 0),
        filename="filename",
        bundle_name="bundle_name",
        stack_trace="stack_trace",
    )

    import_error_collection_response = ImportErrorCollectionResponse(
        import_errors=[import_error_response],
        total_entries=1,
    )

    dag_stats_collection_response = DagStatsCollectionResponse(
        dags=[
            DagStatsResponse(
                dag_id=dag_id,
                dag_display_name=dag_id,
                stats=[DagStatsStateResponse(state=DagRunState.RUNNING, count=1)],
            )
        ],
        total_entries=1,
    )

    dag_version_response = DagVersionResponse(
        id=uuid.uuid4(),
        version_number=1,
        dag_id=dag_id,
        bundle_name="bundle_name",
        bundle_version="1",
        created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        dag_display_name=dag_id,
    )

    dag_version_collection_response = DAGVersionCollectionResponse(
        dag_versions=[dag_version_response],
        total_entries=1,
    )

    dag_warning_collection_response = DAGWarningCollectionResponse(
        dag_warnings=[
            DAGWarningResponse(
                dag_id=dag_id,
                warning_type=DagWarningType.NON_EXISTENT_POOL,
                message="message",
                timestamp=datetime.datetime(2025, 1, 1, 0, 0, 0),
                dag_display_name=dag_display_name,
            )
        ],
        total_entries=1,
    )

    dag_patch_body = DAGPatchBody(
        is_paused=True,
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/dag_id"
            return httpx.Response(200, json=json.loads(self.dag_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get("dag_id")
        assert response == self.dag_response

    def test_get_details(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/dag_id/details"
            return httpx.Response(200, json=json.loads(self.dag_details_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get_details("dag_id")
        assert response == self.dag_details_response

    def test_get_tags(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dagTags"
            return httpx.Response(200, json=json.loads(self.dag_tag_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get_tags()
        assert response == self.dag_tag_collection_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags"
            return httpx.Response(200, json=json.loads(self.dag_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.list()
        assert response == self.dag_collection_response

    def test_patch(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/dag_id"
            return httpx.Response(200, json=json.loads(self.dag_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.patch(dag_id="dag_id", dag_body=self.dag_patch_body)
        assert response == self.dag_response

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/dag_id"
            return httpx.Response(200, json=json.loads(self.dag_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.delete(dag_id="dag_id")
        assert response == self.dag_id

    def test_get_import_error(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/importErrors/0"
            return httpx.Response(200, json=json.loads(self.import_error_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get_import_error(import_error_id=0)
        assert response == self.import_error_response

    def test_list_import_errors(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/importErrors"
            return httpx.Response(
                200, json=json.loads(self.import_error_collection_response.model_dump_json())
            )

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.list_import_errors()
        assert response == self.import_error_collection_response

    def test_get_stats(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dagStats"
            return httpx.Response(200, json=json.loads(self.dag_stats_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get_stats(dag_ids=["dag_id"])
        assert response == self.dag_stats_collection_response

    def test_get_version(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/dag_id/dagVersions/0"
            return httpx.Response(200, json=json.loads(self.dag_version_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.get_version(dag_id="dag_id", version_number="0")
        assert response == self.dag_version_response

    def test_list_version(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dags/dag_id/dagVersions"
            return httpx.Response(
                200, json=json.loads(self.dag_version_collection_response.model_dump_json())
            )

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.list_version(dag_id="dag_id")
        assert response == self.dag_version_collection_response

    def test_list_warning(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/dagWarnings"
            return httpx.Response(
                200, json=json.loads(self.dag_warning_collection_response.model_dump_json())
            )

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dags.list_warning()
        assert response == self.dag_warning_collection_response


class TestDagRunOperations:
    dag_id = "dag_id"
    dag_run_id = "dag_run_id"
    dag_run_response = DAGRunResponse(
        dag_display_name=dag_run_id,
        dag_run_id=dag_run_id,
        dag_id=dag_id,
        logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        queued_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
        start_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        last_scheduling_decision=datetime.datetime(2025, 1, 1, 0, 0, 0),
        run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        triggered_by=DagRunTriggeredByType.UI,
        conf={},
        note=None,
        dag_versions=[
            DagVersionResponse(
                id=uuid.uuid4(),
                version_number=1,
                dag_id=dag_id,
                bundle_name="bundle_name",
                bundle_version="1",
                created_at=datetime.datetime(2025, 1, 1, 0, 0, 0),
                dag_display_name=dag_id,
            )
        ],
    )

    dag_run_collection_response = DAGRunCollectionResponse(
        dag_runs=[dag_run_response],
        total_entries=1,
    )

    trigger_dag_run = TriggerDAGRunPostBody(
        conf=None,
        note=None,
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}"
            return httpx.Response(200, json=json.loads(self.dag_run_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dag_runs.get(dag_id=self.dag_id, dag_run_id=self.dag_run_id)
        assert response == self.dag_run_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/dags/{self.dag_id}/dagRuns"
            return httpx.Response(200, json=json.loads(self.dag_run_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dag_runs.list(
            dag_id=self.dag_id,
            start_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
            state=DagRunState.RUNNING,
            limit=1,
        )
        assert response == self.dag_run_collection_response

    def test_trigger(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/dags/{self.dag_id}/dagRuns"
            return httpx.Response(200, json=json.loads(self.dag_run_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.dag_runs.trigger(dag_id=self.dag_id, trigger_dag_run=self.trigger_dag_run)
        assert response == self.dag_run_response


class TestJobsOperations:
    job_response = JobResponse(
        id=1,
        dag_id="dag_id",
        state="state",
        job_type="job_type",
        start_date=datetime.datetime(2024, 12, 31, 23, 59, 59),
        end_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        latest_heartbeat=datetime.datetime(2025, 1, 1, 0, 0, 0),
        executor_class="LocalExecutor",
        hostname="hostname",
        unixname="unixname",
    )

    job_collection_response = JobCollectionResponse(
        jobs=[job_response],
        total_entries=1,
    )

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/jobs"
            return httpx.Response(200, json=json.loads(self.job_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.jobs.list(
            job_type="job_type",
            hostname="hostname",
            is_alive=True,
        )
        assert response == self.job_collection_response


class TestPoolsOperations:
    pool_name = "pool_name"
    pool = PoolBody(
        name=pool_name,
        slots=1,
        description="description",
        include_deferred=True,
    )
    pools_bulk_body = BulkBodyPoolBody(
        actions=[
            BulkCreateActionPoolBody(
                action="create",
                entities=[pool],
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )
    pool_response = PoolResponse(
        name=pool_name,
        slots=1,
        description="description",
        include_deferred=True,
        occupied_slots=1,
        running_slots=1,
        queued_slots=1,
        scheduled_slots=1,
        open_slots=1,
        deferred_slots=1,
    )
    pool_response_collection = PoolCollectionResponse(
        pools=[pool_response],
        total_entries=1,
    )
    pool_bulk_aresponse = BulkResponse(
        create=BulkActionResponse(success=[pool_name], errors=[]),
        update=None,
        delete=None,
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/pools/{self.pool_name}"
            return httpx.Response(200, json=json.loads(self.pool_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.get(self.pool_name)
        assert response == self.pool_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/pools"
            return httpx.Response(200, json=json.loads(self.pool_response_collection.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.list()
        assert response == self.pool_response_collection

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/pools"
            return httpx.Response(200, json=json.loads(self.pool_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.create(pool=self.pool)
        assert response == self.pool_response

    def test_bulk(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/pools"
            return httpx.Response(200, json=json.loads(self.pool_bulk_aresponse.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.bulk(pools=self.pools_bulk_body)
        assert response == self.pool_bulk_aresponse

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/pools/{self.pool_name}"
            return httpx.Response(200, json=json.loads(self.pool_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.pools.delete(self.pool_name)
        assert response == self.pool_name


class TestProvidersOperations:
    provider_response = ProviderResponse(
        package_name="package_name",
        version="version",
        description="description",
    )
    provider_collection_response = ProviderCollectionResponse(
        providers=[provider_response],
        total_entries=1,
    )

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/providers"
            return httpx.Response(200, json=json.loads(self.provider_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.providers.list()
        assert response == self.provider_collection_response


class TestVariablesOperations:
    key = "key"
    value = "val"
    description = "description"
    variable = VariableBody.model_validate(
        {
            "key": key,
            "value": value,
            "description": description,
        }
    )
    variable_response = VariableResponse(
        key=key,
        value=value,
        description=description,
        is_encrypted=False,
    )
    variable_collection_response = VariableCollectionResponse(
        variables=[variable_response],
        total_entries=1,
    )
    variable_bulk = BulkBodyVariableBody(
        actions=[
            BulkCreateActionVariableBody(
                action="create",
                entities=[variable],
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )
    variable_bulk_response = BulkResponse(
        create=BulkActionResponse(success=[key], errors=[]),
        update=None,
        delete=None,
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/variables/{self.key}"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.get(self.key)
        assert response == self.variable_response

    def test_list(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/variables"
            return httpx.Response(200, json=json.loads(self.variable_collection_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.list()
        assert response == self.variable_collection_response

    def test_create(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/variables"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.create(variable=self.variable)
        assert response == self.variable_response

    def test_bulk(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/variables"
            return httpx.Response(200, json=json.loads(self.variable_bulk_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.bulk(variables=self.variable_bulk)
        assert response == self.variable_bulk_response

    def test_delete(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/variables/{self.key}"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.delete(self.key)
        assert response == self.key

    def test_update(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == f"/api/v2/variables/{self.key}"
            return httpx.Response(200, json=json.loads(self.variable_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.variables.update(variable=self.variable)
        assert response == self.variable_response


class TestVersionOperations:
    version_info = VersionInfo(
        version="version",
        git_version="git_version",
    )

    def test_get(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/v2/version"
            return httpx.Response(200, json=json.loads(self.version_info.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request))
        response = client.version.get()
        assert response == self.version_info


class TestAuthOperations:
    login_response = LoginResponse(
        access_token="NO_TOKEN",
    )

    def test_login(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/auth/token/cli"
            return httpx.Response(200, json=json.loads(self.login_response.model_dump_json()))

        client = make_api_client(transport=httpx.MockTransport(handle_request), kind=ClientKind.AUTH)
        response = client.login.login_with_username_and_password(
            LoginBody(
                username="username",
                password="password",
            )
        )
        assert response.access_token == "NO_TOKEN"
