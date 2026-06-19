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
from unittest.mock import patch

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.asset_state_store import AssetStateStoreBody
from airflow.models.asset import AssetModel
from airflow.models.asset_state_store import AssetStateStoreModel

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_assets

pytestmark = pytest.mark.db_test

ASSET_URI = "s3://bucket/watermarks"
ASSET_NAME = "test_asset"


def _create_asset(session) -> AssetModel:
    asset = AssetModel(uri=ASSET_URI, name=ASSET_NAME, group="test")
    session.add(asset)
    session.flush()
    return asset


def _create_asset_state_store_row(session, asset_id: int, key: str, value: str) -> None:
    row = AssetStateStoreModel(asset_id=asset_id, key=key, value=json.dumps(value))
    session.add(row)
    session.flush()


class TestAssetStateEndpoint:
    @staticmethod
    def clear_db():
        clear_db_assets()

    @pytest.fixture(autouse=True)
    def setup(self, session):
        self.clear_db()
        self.asset = _create_asset(session)
        session.commit()
        self._session = session
        self._base_url = f"/assets/{self.asset.id}/state-store"

    def teardown_method(self):
        self.clear_db()


class TestAssetNotFound(TestAssetStateEndpoint):
    """All endpoints return 404 when asset_id does not exist."""

    def test_list_unknown_asset_returns_404(self, test_client):
        assert test_client.get("/assets/99999/state-store").status_code == 404

    def test_get_unknown_asset_returns_404(self, test_client):
        assert test_client.get("/assets/99999/state-store/key").status_code == 404

    def test_set_unknown_asset_returns_404(self, test_client):
        assert test_client.put("/assets/99999/state-store/key", json={"value": "v"}).status_code == 404

    def test_delete_unknown_asset_returns_404(self, test_client):
        assert test_client.delete("/assets/99999/state-store/key").status_code == 404

    def test_clear_unknown_asset_returns_404(self, test_client):
        assert test_client.delete("/assets/99999/state-store").status_code == 404


class TestListAssetState(TestAssetStateEndpoint):
    def test_returns_empty_list_when_no_state(self, test_client):
        response = test_client.get(self._base_url)
        assert response.status_code == 200
        assert response.json() == {"asset_state_store": [], "total_entries": 0}

    def test_returns_all_keys(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "2026-05-01")
        _create_asset_state_store_row(self._session, self.asset.id, "file_count", "42")
        self._session.commit()

        response = test_client.get(self._base_url)
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 2
        keys = {item["key"]: item["value"] for item in data["asset_state_store"]}
        assert keys == {"watermark": "2026-05-01", "file_count": "42"}

    def test_returns_metadata_fields(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "2026-05-01")
        self._session.commit()

        item = test_client.get(self._base_url).json()["asset_state_store"][0]
        assert "updated_at" in item
        assert item["key"] == "watermark"

    def test_last_updated_by_returned_when_set(self, test_client, create_task_instance):
        ti = create_task_instance()
        row = AssetStateStoreModel(
            asset_id=self.asset.id,
            key="watermark",
            value='"v"',
            last_updated_by_kind="task",
            last_updated_by_dag_id=ti.dag_id,
            last_updated_by_run_id=ti.run_id,
            last_updated_by_task_id=ti.task_id,
            last_updated_by_map_index=ti.map_index,
        )
        self._session.add(row)
        self._session.commit()

        item = test_client.get(self._base_url).json()["asset_state_store"][0]
        assert item["last_updated_by"]["kind"] == "task"
        assert item["last_updated_by"]["dag_id"] == ti.dag_id
        assert item["last_updated_by"]["run_id"] == ti.run_id
        assert item["last_updated_by"]["task_id"] == ti.task_id
        assert item["last_updated_by"]["map_index"] == ti.map_index

    def test_pagination_limit(self, test_client):
        for k in ("watermark", "file_count", "last_run"):
            _create_asset_state_store_row(self._session, self.asset.id, k, "v")
        self._session.commit()

        response = test_client.get(f"{self._base_url}?limit=2")
        data = response.json()
        assert data["total_entries"] == 3
        assert len(data["asset_state_store"]) == 2

    def test_pagination_offset(self, test_client):
        for k in ("watermark", "file_count", "last_run"):
            _create_asset_state_store_row(self._session, self.asset.id, k, "v")
        self._session.commit()

        response = test_client.get(f"{self._base_url}?limit=2&offset=2")
        data = response.json()
        assert data["total_entries"] == 3
        assert len(data["asset_state_store"]) == 1

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(self._base_url).status_code == 401


class TestGetAssetState(TestAssetStateEndpoint):
    def test_returns_value(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "2026-05-01")
        self._session.commit()

        response = test_client.get(f"{self._base_url}/watermark")
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "watermark"
        assert data["value"] == "2026-05-01"
        assert "updated_at" in data
        assert data["last_updated_by"] is None

    def test_last_updated_by_returned_in_get(self, test_client, create_task_instance):
        ti = create_task_instance()
        row = AssetStateStoreModel(
            asset_id=self.asset.id,
            key="watermark",
            value='"v"',
            last_updated_by_kind="task",
            last_updated_by_dag_id=ti.dag_id,
            last_updated_by_run_id=ti.run_id,
            last_updated_by_task_id=ti.task_id,
            last_updated_by_map_index=ti.map_index,
        )
        self._session.add(row)
        self._session.commit()

        data = test_client.get(f"{self._base_url}/watermark").json()
        assert data["last_updated_by"]["kind"] == "task"
        assert data["last_updated_by"]["dag_id"] == ti.dag_id
        assert data["last_updated_by"]["run_id"] == ti.run_id
        assert data["last_updated_by"]["task_id"] == ti.task_id
        assert data["last_updated_by"]["map_index"] == ti.map_index

    def test_missing_key_returns_404(self, test_client):
        assert test_client.get(f"{self._base_url}/nonexistent").status_code == 404

    def test_key_with_slash_is_supported(self, test_client):
        """Keys containing slashes must work — route uses {key:path}."""
        _create_asset_state_store_row(self._session, self.asset.id, "partition/date", "2026-05-01")
        self._session.commit()

        response = test_client.get(f"{self._base_url}/partition/date")
        assert response.status_code == 200
        assert response.json()["key"] == "partition/date"

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(f"{self._base_url}/watermark").status_code == 401


class TestSetAssetState(TestAssetStateEndpoint):
    def test_creates_new_key(self, test_client):
        response = test_client.put(f"{self._base_url}/watermark", json={"value": "2026-05-01"})
        assert response.status_code == 204

        assert test_client.get(f"{self._base_url}/watermark").json()["value"] == "2026-05-01"

    def test_overwrites_existing_key(self, test_client):
        test_client.put(f"{self._base_url}/watermark", json={"value": "v1"})
        test_client.put(f"{self._base_url}/watermark", json={"value": "v2"})

        assert test_client.get(f"{self._base_url}/watermark").json()["value"] == "v2"

    def test_empty_body_returns_422(self, test_client):
        assert test_client.put(f"{self._base_url}/watermark", json={}).status_code == 422

    def test_null_value_returns_422(self, test_client):
        assert test_client.put(f"{self._base_url}/watermark", json={"value": None}).status_code == 422

    def test_put_value_over_limit_returns_422(self, test_client):
        # default is 65535 bytes
        big = "x" * 70000
        assert test_client.put(f"{self._base_url}/watermark", json={"value": big}).status_code == 422

    @conf_vars({("state_store", "max_value_storage_bytes"): "0"})
    def test_put_value_over_limit_accepted_when_limit_disabled(self, test_client):
        big = "x" * 70000
        assert test_client.put(f"{self._base_url}/watermark", json={"value": big}).status_code == 204

    @pytest.mark.parametrize("bad_value", [float("nan"), float("inf"), {"a": float("nan")}, [float("inf")]])
    def test_non_finite_float_rejected_by_validator(self, bad_value):
        with pytest.raises(ValidationError, match="non-finite"):
            AssetStateStoreBody(value=bad_value)

    @pytest.mark.parametrize(
        ("value", "expected_db"),
        [
            (42, "42"),
            ("hello", '"hello"'),
            ({"k": 1}, '{"k": 1}'),
            ([1, 2], "[1, 2]"),
        ],
    )
    def test_put_stores_json_encoded_value(self, test_client, value, expected_db):
        test_client.put(f"{self._base_url}/k", json={"value": value})
        row = self._session.scalar(
            select(AssetStateStoreModel).where(
                AssetStateStoreModel.asset_id == self.asset.id,
                AssetStateStoreModel.key == "k",
            )
        )
        assert row is not None
        assert row.value == expected_db

    def test_put_records_api_kind(self, test_client):
        """PUT via the Core API sets last_updated_by.kind='api' in the response."""
        test_client.put(f"{self._base_url}/watermark", json={"value": "v"})

        data = test_client.get(f"{self._base_url}/watermark").json()
        assert data["last_updated_by"]["kind"] == "api"
        assert data["last_updated_by"]["dag_id"] is None
        assert data["last_updated_by"]["run_id"] is None
        assert data["last_updated_by"]["task_id"] is None
        assert data["last_updated_by"]["map_index"] is None

    @pytest.mark.parametrize("value", [42, True, {"rows": 100}, [1, "two"], "hello"])
    def test_core_api_write_read_roundtrip(self, test_client, value):
        """Core API write then Core API read returns the same native value."""
        test_client.put(f"{self._base_url}/k", json={"value": value})
        assert test_client.get(f"{self._base_url}/k").json()["value"] == value

    @pytest.mark.parametrize("value", [42, True, {"rows": 100}, [1, "two"], "hello"])
    def test_worker_write_core_api_read_roundtrip(self, test_client, value):
        """Worker write (json.dumps in DB) then Core API read returns native value."""
        _create_asset_state_store_row(self._session, self.asset.id, "k", value)
        self._session.commit()
        assert test_client.get(f"{self._base_url}/k").json()["value"] == value

    def test_key_with_slash_is_supported(self, test_client):
        response = test_client.put(f"{self._base_url}/partition/date", json={"value": "2026-05-01"})
        assert response.status_code == 204
        assert test_client.get(f"{self._base_url}/partition/date").json()["key"] == "partition/date"

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert (
            unauthenticated_test_client.put(f"{self._base_url}/watermark", json={"value": "v"}).status_code
            == 401
        )


class TestDeleteAssetState(TestAssetStateEndpoint):
    def test_deletes_key(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "2026-05-01")
        self._session.commit()

        assert test_client.delete(f"{self._base_url}/watermark").status_code == 204
        assert test_client.get(f"{self._base_url}/watermark").status_code == 404

    def test_delete_noop_for_missing_key(self, test_client):
        assert test_client.delete(f"{self._base_url}/nonexistent").status_code == 204

    def test_only_deletes_target_key(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "a")
        _create_asset_state_store_row(self._session, self.asset.id, "file_count", "b")
        self._session.commit()

        test_client.delete(f"{self._base_url}/watermark")

        assert test_client.get(f"{self._base_url}/watermark").status_code == 404
        assert test_client.get(f"{self._base_url}/file_count").json()["value"] == "b"

    def test_key_with_slash_is_supported(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "partition/date", "v")
        self._session.commit()

        assert test_client.delete(f"{self._base_url}/partition/date").status_code == 204
        assert test_client.get(f"{self._base_url}/partition/date").status_code == 404

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.delete(f"{self._base_url}/watermark").status_code == 401


class TestClearAssetState(TestAssetStateEndpoint):
    def test_clears_all_keys(self, test_client):
        for k, v in [("watermark", "a"), ("file_count", "b"), ("last_run", "c")]:
            _create_asset_state_store_row(self._session, self.asset.id, k, v)
        self._session.commit()

        assert test_client.delete(self._base_url).status_code == 204
        assert test_client.get(self._base_url).json()["total_entries"] == 0

    def test_clear_is_noop_when_no_state(self, test_client):
        assert test_client.delete(self._base_url).status_code == 204

    def test_clear_does_not_affect_other_assets(self, test_client):
        other_asset = AssetModel(uri="s3://other/asset", name="other_asset", group="test")
        self._session.add(other_asset)
        self._session.flush()
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "mine")
        _create_asset_state_store_row(self._session, other_asset.id, "watermark", "theirs")
        self._session.commit()

        test_client.delete(self._base_url)

        other_url = f"/assets/{other_asset.id}/state-store"
        assert test_client.get(f"{other_url}/watermark").json()["value"] == "theirs"

    def test_clears_slash_keyed_entries(self, test_client):
        _create_asset_state_store_row(self._session, self.asset.id, "partition/date", "v")
        self._session.commit()

        assert test_client.delete(self._base_url).status_code == 204
        assert test_client.get(self._base_url).json()["total_entries"] == 0

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.delete(self._base_url).status_code == 401


class TestRoutesNeverCallCustomBackend(TestAssetStateEndpoint):
    """Tests to validate that core API routes must use MetastoreStateBackend directly."""

    @pytest.mark.parametrize(
        ("method", "path_suffix", "kwargs"),
        [
            ("get", "", {}),
            ("get", "/watermark", {}),
            ("put", "/watermark", {"json": {"value": "v2"}}),
            ("delete", "/watermark", {}),
            ("delete", "", {}),
        ],
    )
    def test_route_never_calls_get_state_backend(self, test_client, method, path_suffix, kwargs):
        _create_asset_state_store_row(self._session, self.asset.id, "watermark", "v1")
        self._session.commit()

        with patch("airflow.state.get_state_backend") as mock_get_backend:
            getattr(test_client, method)(f"{self._base_url}{path_suffix}", **kwargs)

        mock_get_backend.assert_not_called()
