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

import pytest
from pydantic import ValidationError

from airflow.sdk import AssetState
from airflow.sdk.definitions.asset.state import AssetState as DirectAssetState
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time.comms import (
    AssetStateResult,
    ClearAssetStateByName,
    ClearAssetStateByUri,
    DeleteAssetStateByName,
    DeleteAssetStateByUri,
    ErrorResponse,
    GetAssetStateByName,
    GetAssetStateByUri,
    OKResponse,
    SetAssetStateByName,
    SetAssetStateByUri,
)
from airflow.sdk.execution_time.context import AssetStateAccessor


class TestAssetState:
    """Validate the public AssetState SDK interface."""

    asset_name: str = "my_asset"
    asset_uri: str = "s3://bucket/key"

    def test_lazy_import_from_airflow_sdk(self):
        """Validate the lazy __init__.py alias resolves to the real class"""
        assert AssetState is DirectAssetState

    def test_is_asset_state_accessor_subclass(self):
        """Validate that AssetState inherits all AssetStateAccess logic"""
        assert issubclass(AssetState, AssetStateAccessor)
        assert isinstance(AssetState(name=self.asset_name), AssetStateAccessor)

    def test_requires_name_or_uri(self):
        """Validate that constructing without either identifier must fail fast at init time"""
        with pytest.raises(ValueError, match="Either `name` or `uri` must be provided"):
            AssetState()

    def test_set_fails_on_non_string_key(self, mock_supervisor_comms):
        """Validate that set(key, value) where isinstance(key, str) false raises a ValidationError"""
        with pytest.raises(ValidationError):
            AssetState(name=self.asset_name).set(123, "some_value")  # type: ignore[arg-type]

        mock_supervisor_comms.send.assert_not_called()

    def test_set_fails_on_non_string_value(self, mock_supervisor_comms):
        """Validate that set(key, value) where isinstance(value, str) false raises a ValidationError"""
        with pytest.raises(ValidationError):
            AssetState(name=self.asset_name).set("watermark", 12345)  # type: ignore[arg-type]

        mock_supervisor_comms.send.assert_not_called()

    def test_get_by_name_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that get() with name=... dispatches GetAssetStateByName and unwraps the value"""
        mock_supervisor_comms.send.return_value = AssetStateResult(value="2026-04-30T00:00:00Z")

        result = AssetState(name=self.asset_name).get("watermark")

        assert result == "2026-04-30T00:00:00Z"
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetStateByName(name=self.asset_name, key="watermark")
        )

    def test_get_by_uri_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that get() with uri=... dispatches GetAssetStateByUri and unwraps the value"""
        mock_supervisor_comms.send.return_value = AssetStateResult(value="2026-04-30T00:00:00Z")

        result = AssetState(uri=self.asset_uri).get("watermark")

        assert result == "2026-04-30T00:00:00Z"
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetStateByUri(uri=self.asset_uri, key="watermark")
        )

    def test_get_returns_none_when_key_not_found(self, mock_supervisor_comms):
        """
        Validate that a 404-style ASSET_STATE_NOT_FOUND response must silently return None rather than
        raising, matching the contract documented in AssetStateAccessor
        """
        mock_supervisor_comms.send.return_value = ErrorResponse(
            error=ErrorType.ASSET_STATE_NOT_FOUND, detail={"key": "missing_key"}
        )

        result = AssetState(name=self.asset_name).get("missing_key")

        assert result is None

    def test_get_raises_on_generic_error(self, mock_supervisor_comms):
        """Validate that any error other than ASSET_STATE_NOT_FOUND must propagate as AirflowRuntimeError"""
        mock_supervisor_comms.send.return_value = ErrorResponse(
            error=ErrorType.GENERIC_ERROR, detail={"message": "server error"}
        )

        with pytest.raises(AirflowRuntimeError):
            AssetState(name=self.asset_name).get("some_key")

    def test_set_by_name_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that set() with name= dispatches SetAssetStateByName"""
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(name=self.asset_name).set("watermark", "2026-04-30T00:00:00Z")

        mock_supervisor_comms.send.assert_called_once_with(
            SetAssetStateByName(name=self.asset_name, key="watermark", value="2026-04-30T00:00:00Z")
        )

    def test_set_by_uri_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that set() with uri= dispatches SetAssetStateByUri"""
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(uri=self.asset_uri).set("watermark", "2026-04-30T00:00:00Z")

        mock_supervisor_comms.send.assert_called_once_with(
            SetAssetStateByUri(uri=self.asset_uri, key="watermark", value="2026-04-30T00:00:00Z")
        )

    def test_delete_by_name_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that delete() with name= dispatches DeleteAssetStateByName"""
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(name=self.asset_name).delete("watermark")

        mock_supervisor_comms.send.assert_called_once_with(
            DeleteAssetStateByName(name=self.asset_name, key="watermark")
        )

    def test_delete_by_uri_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that delete() with uri= dispatches DeleteAssetStateByUri"""
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(uri=self.asset_uri).delete("watermark")

        mock_supervisor_comms.send.assert_called_once_with(
            DeleteAssetStateByUri(uri=self.asset_uri, key="watermark")
        )

    def test_clear_by_name_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that clear() with name= dispatches ClearAssetStateByName (no key argument)"""
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(name=self.asset_name).clear()

        mock_supervisor_comms.send.assert_called_once_with(ClearAssetStateByName(name=self.asset_name))

    def test_clear_by_uri_sends_supervisor_message(self, mock_supervisor_comms):
        """Validate that clear() with uri= dispatches ClearAssetStateByUri (no key argument)"""
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(uri=self.asset_uri).clear()

        mock_supervisor_comms.send.assert_called_once_with(ClearAssetStateByUri(uri=self.asset_uri))
