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

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.datamodels._generated import AssetStateStoreResponse, TaskStateStoreResponse
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    AssetStateStoreResult,
    ClearAssetStateStoreByName,
    ClearAssetStateStoreByUri,
    ClearTaskStateStore,
    DeleteAssetStateStoreByName,
    DeleteAssetStateStoreByUri,
    DeleteTaskStateStore,
    ErrorResponse,
    GetAssetStateStoreByName,
    GetAssetStateStoreByUri,
    GetTaskStateStore,
    SetAssetStateStoreByName,
    SetAssetStateStoreByUri,
    SetTaskStateStore,
    TaskStateStoreResult,
)
from airflow.sdk.execution_time.request_handlers import (
    handle_clear_asset_state_store_by_name,
    handle_clear_asset_state_store_by_uri,
    handle_clear_task_state_store,
    handle_delete_asset_state_store_by_name,
    handle_delete_asset_state_store_by_uri,
    handle_delete_task_state_store,
    handle_get_asset_state_store_by_name,
    handle_get_asset_state_store_by_uri,
    handle_get_task_state_store,
    handle_set_asset_state_store_by_name,
    handle_set_asset_state_store_by_uri,
    handle_set_task_state_store,
)


@pytest.fixture
def client():
    return MagicMock(spec=sdk_client.Client)


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


def test_get_task_state_store_wraps_response_as_result(client):
    ti_id = uuid4()
    client.task_state_store.get.return_value = TaskStateStoreResponse(value="job-123")

    result, dump_opts = handle_get_task_state_store(client, GetTaskStateStore(ti_id=ti_id, key="external_id"))

    client.task_state_store.get.assert_called_once_with(ti_id, "external_id")
    assert result == TaskStateStoreResult(value="job-123")
    assert dump_opts == {}


def test_get_task_state_store_passes_through_error_response(client):
    err = ErrorResponse(error=ErrorType.TASK_STORE_NOT_FOUND, detail={"key": "external_id"})
    client.task_state_store.get.return_value = err

    result, dump_opts = handle_get_task_state_store(
        client, GetTaskStateStore(ti_id=uuid4(), key="external_id")
    )

    assert result is err
    assert dump_opts == {}


def test_set_task_state_store_delegates_to_client(client):
    ti_id = uuid4()
    expires_at = datetime(2026, 6, 11, tzinfo=timezone.utc)

    result, dump_opts = handle_set_task_state_store(
        client,
        SetTaskStateStore(ti_id=ti_id, key="external_id", value="job-123", expires_at=expires_at),
    )

    client.task_state_store.set.assert_called_once_with(
        ti_id, "external_id", "job-123", expires_at=expires_at
    )
    assert result is None
    assert dump_opts == {}


def test_delete_task_state_store_delegates_to_client(client):
    ti_id = uuid4()

    result, dump_opts = handle_delete_task_state_store(
        client, DeleteTaskStateStore(ti_id=ti_id, key="external_id")
    )

    client.task_state_store.delete.assert_called_once_with(ti_id, "external_id")
    assert result is None
    assert dump_opts == {}


def test_clear_task_state_store_delegates_to_client(client):
    ti_id = uuid4()

    result, dump_opts = handle_clear_task_state_store(client, ClearTaskStateStore(ti_id=ti_id))

    client.task_state_store.clear.assert_called_once_with(ti_id)
    assert result is None
    assert dump_opts == {}
