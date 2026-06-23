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

from unittest.mock import MagicMock

import pytest

from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.datamodels._generated import AssetStateStoreResponse
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    AssetStateStoreResult,
    ClearAssetStateStoreByName,
    ClearAssetStateStoreByUri,
    DeleteAssetStateStoreByName,
    DeleteAssetStateStoreByUri,
    ErrorResponse,
    GetAssetStateStoreByName,
    GetAssetStateStoreByUri,
    SetAssetStateStoreByName,
    SetAssetStateStoreByUri,
)
from airflow.sdk.execution_time.request_handlers import (
    handle_clear_asset_state_store_by_name,
    handle_clear_asset_state_store_by_uri,
    handle_delete_asset_state_store_by_name,
    handle_delete_asset_state_store_by_uri,
    handle_get_asset_state_store_by_name,
    handle_get_asset_state_store_by_uri,
    handle_set_asset_state_store_by_name,
    handle_set_asset_state_store_by_uri,
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


def test_set_asset_state_store_by_name_delegates_to_client(client):
    result, dump_opts = handle_set_asset_state_store_by_name(
        client, SetAssetStateStoreByName(name="asset_a", key="watermark", value="v")
    )

    client.asset_state_store.set.assert_called_once_with(key="watermark", value="v", name="asset_a")
    assert result is None
    assert dump_opts == {}


def test_set_asset_state_store_by_uri_delegates_to_client(client):
    result, dump_opts = handle_set_asset_state_store_by_uri(
        client, SetAssetStateStoreByUri(uri="s3://bucket/a", key="watermark", value="v")
    )

    client.asset_state_store.set.assert_called_once_with(key="watermark", value="v", uri="s3://bucket/a")
    assert result is None
    assert dump_opts == {}


def test_delete_asset_state_store_by_name_delegates_to_client(client):
    result, dump_opts = handle_delete_asset_state_store_by_name(
        client, DeleteAssetStateStoreByName(name="asset_a", key="watermark")
    )

    client.asset_state_store.delete.assert_called_once_with(key="watermark", name="asset_a")
    assert result is None
    assert dump_opts == {}


def test_delete_asset_state_store_by_uri_delegates_to_client(client):
    result, dump_opts = handle_delete_asset_state_store_by_uri(
        client, DeleteAssetStateStoreByUri(uri="s3://bucket/a", key="watermark")
    )

    client.asset_state_store.delete.assert_called_once_with(key="watermark", uri="s3://bucket/a")
    assert result is None
    assert dump_opts == {}


def test_clear_asset_state_store_by_name_delegates_to_client(client):
    result, dump_opts = handle_clear_asset_state_store_by_name(
        client, ClearAssetStateStoreByName(name="asset_a")
    )

    client.asset_state_store.clear.assert_called_once_with(name="asset_a")
    assert result is None
    assert dump_opts == {}


def test_clear_asset_state_store_by_uri_delegates_to_client(client):
    result, dump_opts = handle_clear_asset_state_store_by_uri(
        client, ClearAssetStateStoreByUri(uri="s3://bucket/a")
    )

    client.asset_state_store.clear.assert_called_once_with(uri="s3://bucket/a")
    assert result is None
    assert dump_opts == {}
