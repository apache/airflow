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

from airflow.models.asset import AssetModel
from airflow.models.asset_state import AssetStateModel

from tests_common.test_utils.db import clear_db_assets

pytestmark = pytest.mark.db_test

ASSET_URI = "s3://bucket/watermarks"
ASSET_NAME = "test_asset"


def _create_asset(session) -> AssetModel:
    asset = AssetModel(uri=ASSET_URI, name=ASSET_NAME, group="test")
    session.add(asset)
    session.flush()
    return asset


def _create_asset_state(session, asset_id: int, key: str, value: str) -> None:
    row = AssetStateModel(asset_id=asset_id, key=key, value=value)
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
        self._base_url = f"/assets/{self.asset.id}/states"

    def teardown_method(self):
        self.clear_db()


class TestListAssetState(TestAssetStateEndpoint):
    def test_returns_empty_list_when_no_state(self, test_client):
        response = test_client.get(self._base_url)
        assert response.status_code == 200
        assert response.json() == {"asset_states": [], "total_entries": 0}

    def test_returns_all_keys(self, test_client):
        _create_asset_state(self._session, self.asset.id, "watermark", "2026-05-01")
        _create_asset_state(self._session, self.asset.id, "file_count", "42")
        self._session.commit()

        response = test_client.get(self._base_url)
        assert response.status_code == 200
        data = response.json()
        assert data["total_entries"] == 2
        keys = {item["key"]: item["value"] for item in data["asset_states"]}
        assert keys == {"watermark": "2026-05-01", "file_count": "42"}

    def test_returns_metadata_fields(self, test_client):
        _create_asset_state(self._session, self.asset.id, "watermark", "2026-05-01")
        self._session.commit()

        item = test_client.get(self._base_url).json()["asset_states"][0]
        assert "updated_at" in item
        assert item["key"] == "watermark"

    def test_pagination_limit(self, test_client):
        for k in ("watermark", "file_count", "last_run"):
            _create_asset_state(self._session, self.asset.id, k, "v")
        self._session.commit()

        response = test_client.get(f"{self._base_url}?limit=2")
        data = response.json()
        assert data["total_entries"] == 3
        assert len(data["asset_states"]) == 2

    def test_pagination_offset(self, test_client):
        for k in ("watermark", "file_count", "last_run"):
            _create_asset_state(self._session, self.asset.id, k, "v")
        self._session.commit()

        response = test_client.get(f"{self._base_url}?limit=2&offset=2")
        data = response.json()
        assert data["total_entries"] == 3
        assert len(data["asset_states"]) == 1

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(self._base_url).status_code == 401


class TestGetAssetState(TestAssetStateEndpoint):
    def test_returns_value(self, test_client):
        _create_asset_state(self._session, self.asset.id, "watermark", "2026-05-01")
        self._session.commit()

        response = test_client.get(f"{self._base_url}/watermark")
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "watermark"
        assert data["value"] == "2026-05-01"
        assert "updated_at" in data

    def test_missing_key_returns_404(self, test_client):
        assert test_client.get(f"{self._base_url}/nonexistent").status_code == 404

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

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert (
            unauthenticated_test_client.put(f"{self._base_url}/watermark", json={"value": "v"}).status_code
            == 401
        )


class TestDeleteAssetState(TestAssetStateEndpoint):
    def test_deletes_key(self, test_client):
        _create_asset_state(self._session, self.asset.id, "watermark", "2026-05-01")
        self._session.commit()

        assert test_client.delete(f"{self._base_url}/watermark").status_code == 204
        assert test_client.get(f"{self._base_url}/watermark").status_code == 404

    def test_delete_noop_for_missing_key(self, test_client):
        assert test_client.delete(f"{self._base_url}/nonexistent").status_code == 204

    def test_only_deletes_target_key(self, test_client):
        _create_asset_state(self._session, self.asset.id, "watermark", "a")
        _create_asset_state(self._session, self.asset.id, "file_count", "b")
        self._session.commit()

        test_client.delete(f"{self._base_url}/watermark")

        assert test_client.get(f"{self._base_url}/watermark").status_code == 404
        assert test_client.get(f"{self._base_url}/file_count").json()["value"] == "b"

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.delete(f"{self._base_url}/watermark").status_code == 401


class TestClearAssetState(TestAssetStateEndpoint):
    def test_clears_all_keys(self, test_client):
        for k, v in [("watermark", "a"), ("file_count", "b"), ("last_run", "c")]:
            _create_asset_state(self._session, self.asset.id, k, v)
        self._session.commit()

        assert test_client.delete(self._base_url).status_code == 204
        assert test_client.get(self._base_url).json()["total_entries"] == 0

    def test_clear_is_noop_when_no_state(self, test_client):
        assert test_client.delete(self._base_url).status_code == 204

    def test_clear_does_not_affect_other_assets(self, test_client):
        other_asset = AssetModel(uri="s3://other/asset", name="other_asset", group="test")
        self._session.add(other_asset)
        self._session.flush()
        _create_asset_state(self._session, self.asset.id, "watermark", "mine")
        _create_asset_state(self._session, other_asset.id, "watermark", "theirs")
        self._session.commit()

        test_client.delete(self._base_url)

        other_url = f"/assets/{other_asset.id}/states"
        assert test_client.get(f"{other_url}/watermark").json()["value"] == "theirs"

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.delete(self._base_url).status_code == 401
