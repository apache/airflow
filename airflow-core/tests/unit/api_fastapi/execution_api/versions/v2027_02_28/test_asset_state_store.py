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

import pytest
from sqlalchemy import delete

from airflow.models.asset import AssetActive, AssetModel
from airflow.models.asset_state_store import AssetStateStoreModel

pytestmark = pytest.mark.db_test


@pytest.fixture
def asset(session):
    asset = AssetModel(name="test_asset", uri="s3://bucket/test", group="asset")
    session.add(asset)
    session.flush()
    session.add(AssetActive.for_asset(asset))
    session.add(AssetStateStoreModel(asset_id=asset.id, key="watermark", value=json.dumps("2026-04-29")))
    session.commit()
    yield asset
    session.execute(delete(AssetStateStoreModel))
    session.execute(delete(AssetModel))
    session.commit()


@pytest.mark.parametrize(
    ("path", "params"),
    [
        pytest.param(
            "/execution/store/asset/by-name/value", {"name": "test_asset", "key": "watermark"}, id="by-name"
        ),
        pytest.param(
            "/execution/store/asset/by-uri/value",
            {"uri": "s3://bucket/test", "key": "watermark"},
            id="by-uri",
        ),
    ],
)
def test_old_version_omits_updated_at(client, asset, path, params):
    """``updated_at`` was added to the asset state store response in the 2027-02-28 API version."""
    client.headers["Airflow-API-Version"] = "2026-10-30"

    response = client.get(path, params=params)

    assert response.status_code == 200
    assert response.json() == {"value": "2026-04-29"}
