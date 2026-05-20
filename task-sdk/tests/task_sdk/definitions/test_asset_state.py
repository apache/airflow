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

from airflow.sdk import AssetState
from airflow.sdk.definitions.asset.state import AssetState as DirectAssetState
from airflow.sdk.execution_time.comms import (
    AssetStateResult,
    GetAssetStateByName,
    GetAssetStateByUri,
    OKResponse,
    SetAssetStateByName,
)
from airflow.sdk.execution_time.context import AssetStateAccessor


class TestAssetState:
    """Validate the AssetState SDK interface."""
    asset_name: str = "my_asset"
    asset_uri: str = "s3://bucket/key"

    def test_lazy_import_from_airflow_sdk(self):
        assert AssetState is DirectAssetState

    def test_is_asset_state_accessor_subclass(self):
        assert issubclass(AssetState, AssetStateAccessor)
        assert isinstance(AssetState(name=self.asset_name), AssetStateAccessor)

    def test_requires_name_or_uri(self):
        with pytest.raises(ValueError, match="Either `name` or `uri` must be provided"):
            AssetState()

    def test_get_by_name_sends_supervisor_message(self, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = AssetStateResult(value="2026-04-30T00:00:00Z")

        result = AssetState(name=self.asset_name).get("watermark")

        assert result == "2026-04-30T00:00:00Z"
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetStateByName(name=self.asset_name, key="watermark")
        )

    def test_get_by_uri_sends_supervisor_message(self, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = AssetStateResult(value="2026-04-30T00:00:00Z")

        result = AssetState(uri=self.asset_uri).get("watermark")

        assert result == "2026-04-30T00:00:00Z"
        mock_supervisor_comms.send.assert_called_once_with(
            GetAssetStateByUri(uri=self.asset_uri, key="watermark")
        )

    def test_set_by_name_sends_supervisor_message(self, mock_supervisor_comms):
        mock_supervisor_comms.send.return_value = OKResponse(ok=True)

        AssetState(name=self.asset_name).set("watermark", "2026-04-30T00:00:00Z")

        mock_supervisor_comms.send.assert_called_once_with(
            SetAssetStateByName(name=self.asset_name, key="watermark", value="2026-04-30T00:00:00Z")
        )
