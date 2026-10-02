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

import json
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
import pytest
import structlog
from task_sdk import make_client

from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.datamodels._generated import AssetStateStoreResponse
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    AssetStateStoreResult,
    ClearAssetStateStoreByName,
    ClearAssetStateStoreByUri,
    DeleteAssetStateStoreByName,
    DeleteAssetStateStoreByUri,
    DeleteXCom,
    ErrorResponse,
    GetAssetStateStoreByName,
    GetAssetStateStoreByUri,
    GetPreviousTI,
    GetTaskBreadcrumbs,
    GetTaskStates,
    GetTICount,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    SetAssetStateStoreByName,
    SetAssetStateStoreByUri,
    SetXCom,
)
from airflow.sdk.execution_time.request_handlers import (
    handle_clear_asset_state_store_by_name,
    handle_clear_asset_state_store_by_uri,
    handle_delete_asset_state_store_by_name,
    handle_delete_asset_state_store_by_uri,
    handle_delete_xcom,
    handle_get_asset_state_store_by_name,
    handle_get_asset_state_store_by_uri,
    handle_get_previous_ti,
    handle_get_task_states,
    handle_get_ti_count,
    handle_get_xcom,
    handle_get_xcom_count,
    handle_get_xcom_sequence_item,
    handle_get_xcom_sequence_slice,
    handle_set_asset_state_store_by_name,
    handle_set_asset_state_store_by_uri,
    handle_set_xcom,
)
from airflow.sdk.execution_time.supervisor import ActivitySubprocess


@pytest.fixture
def client():
    return MagicMock(spec=sdk_client.Client)


@pytest.fixture
def client_ssl_cache():
    sdk_client.Client._get_ssl_context_cached.cache_clear()
    yield
    sdk_client.Client._get_ssl_context_cached.cache_clear()


@pytest.mark.parametrize(
    ("message", "handler", "extra", "response"),
    [
        (GetXCom, handle_get_xcom, {}, {"key": "key", "value": "result"}),
        (GetXComCount, handle_get_xcom_count, {}, None),
        (GetXComSequenceItem, handle_get_xcom_sequence_item, {"offset": 0}, "result"),
        (
            GetXComSequenceSlice,
            handle_get_xcom_sequence_slice,
            {"start": 0, "stop": 2, "step": 1},
            ["result"],
        ),
        (SetXCom, handle_set_xcom, {"value": "result"}, None),
        (DeleteXCom, handle_delete_xcom, {}, None),
        (GetTICount, handle_get_ti_count, {}, 1),
        (GetTaskStates, handle_get_task_states, {}, {"task_states": {}}),
        (GetPreviousTI, handle_get_previous_ti, {}, None),
        (
            GetTaskBreadcrumbs,
            lambda client, msg: ActivitySubprocess._handle_get_task_breadcrumbs(
                MagicMock(spec=ActivitySubprocess, client=client), msg, structlog.get_logger(), 1
            ),
            {},
            {"breadcrumbs": []},
        ),
    ],
)
@pytest.mark.parametrize("coordinates", [None, (UUID(int=0), -1), (uuid4(), 2)])
def test_region_selectors_survive_comms_and_http_transport(
    message, handler, extra, response, coordinates, client_ssl_cache
):
    selectors = {} if coordinates is None else {"region_id": coordinates[0], "region_index": coordinates[1]}
    msg = message(dag_id="dag", run_id="run", task_id="task", key="key", **extra, **selectors)
    decoded = message.model_validate_json(msg.model_dump_json())
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, content=json.dumps(response), headers={"Content-Range": "map_indexes 1"})

    handler(make_client(transport=httpx.MockTransport(respond)), decoded)

    assert len(requests) == 1
    params = requests[0].url.params
    if coordinates is None:
        assert "region_id" not in params
        assert "region_index" not in params
    else:
        assert params["region_id"] == str(coordinates[0])
        assert params["region_index"] == str(coordinates[1])


@pytest.mark.parametrize(
    ("message", "handler", "extra", "response", "field"),
    [
        (GetXCom, handle_get_xcom, {}, {"key": "key", "value": 0}, "previous_iteration"),
        (GetXComCount, handle_get_xcom_count, {}, None, "previous_iteration"),
        (GetXComSequenceItem, handle_get_xcom_sequence_item, {"offset": -1}, 0, "previous_iteration"),
        (
            GetXComSequenceSlice,
            handle_get_xcom_sequence_slice,
            {"start": None, "stop": None, "step": -1},
            [],
            "previous_iteration",
        ),
        (SetXCom, handle_set_xcom, {"value": "stop"}, None, "loop_decision"),
    ],
)
def test_loop_selectors_survive_comms_and_http_transport(
    message, handler, extra, response, field, client_ssl_cache
):
    msg = message(dag_id="dag", run_id="run", task_id="task", key="key", **extra, **{field: True})
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, content=json.dumps(response), headers={"Content-Range": "map_indexes 1"})

    handler(
        make_client(transport=httpx.MockTransport(respond)),
        message.model_validate_json(msg.model_dump_json()),
    )

    assert requests[0].url.params[field] == "true"


def test_get_asset_state_store_by_name_wraps_response_as_result(client):
    client.asset_state_store.get.return_value = AssetStateStoreResponse(value="2026-01-01")

    result, dump_opts = handle_get_asset_state_store_by_name(
        client, GetAssetStateStoreByName(name="asset_a", key="watermark")
    )

    client.asset_state_store.get.assert_called_once_with(key="watermark", name="asset_a")
    assert result == AssetStateStoreResult(value="2026-01-01")
    assert dump_opts == {}


def test_get_asset_state_store_by_name_passes_through_error_response(client):
    err = ErrorResponse(error=ErrorType.ASSET_STORE_NOT_FOUND, detail={"key": "watermark"})
    client.asset_state_store.get.return_value = err

    result, dump_opts = handle_get_asset_state_store_by_name(
        client, GetAssetStateStoreByName(name="asset_a", key="watermark")
    )

    assert result is err
    assert dump_opts == {}


def test_get_asset_state_store_by_uri_wraps_response_as_result(client):
    client.asset_state_store.get.return_value = AssetStateStoreResponse(value="2026-01-01")

    result, dump_opts = handle_get_asset_state_store_by_uri(
        client, GetAssetStateStoreByUri(uri="s3://bucket/a", key="watermark")
    )

    client.asset_state_store.get.assert_called_once_with(key="watermark", uri="s3://bucket/a")
    assert result == AssetStateStoreResult(value="2026-01-01")
    assert dump_opts == {}


def test_get_asset_state_store_by_uri_passes_through_error_response(client):
    err = ErrorResponse(error=ErrorType.ASSET_STORE_NOT_FOUND, detail={"key": "watermark"})
    client.asset_state_store.get.return_value = err

    result, dump_opts = handle_get_asset_state_store_by_uri(
        client, GetAssetStateStoreByUri(uri="s3://bucket/a", key="watermark")
    )

    assert result is err
    assert dump_opts == {}


@pytest.mark.parametrize(
    ("handler", "msg", "call_kwargs", "method"),
    [
        (
            handle_set_asset_state_store_by_name,
            SetAssetStateStoreByName,
            {
                "name": "asset_a",
                "key": "watermark",
                "value": "2026-01-01",
            },
            "set",
        ),
        (
            handle_set_asset_state_store_by_uri,
            SetAssetStateStoreByUri,
            {
                "uri": "s3://bucket/a",
                "key": "watermark",
                "value": "2026-01-01",
            },
            "set",
        ),
        (
            handle_delete_asset_state_store_by_name,
            DeleteAssetStateStoreByName,
            {
                "name": "asset_a",
                "key": "watermark",
            },
            "delete",
        ),
        (
            handle_delete_asset_state_store_by_uri,
            DeleteAssetStateStoreByUri,
            {
                "uri": "s3://bucket/a",
                "key": "watermark",
            },
            "delete",
        ),
        (
            handle_clear_asset_state_store_by_name,
            ClearAssetStateStoreByName,
            {
                "name": "asset_a",
            },
            "clear",
        ),
        (
            handle_clear_asset_state_store_by_uri,
            ClearAssetStateStoreByUri,
            {
                "uri": "s3://bucket/a",
            },
            "clear",
        ),
    ],
)
def test_asset_store_delegates_to_client(client, handler, msg, call_kwargs, method):
    result, dump_opts = handler(client, msg(**call_kwargs))

    getattr(client.asset_state_store, method).assert_called_once_with(**call_kwargs)
    assert result is None
    assert dump_opts == {}
