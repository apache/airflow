#
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

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from airflow.models.asset import AssetModel
from airflow.models.asset_state import AssetStateModel

from tests_common.test_utils.db import clear_db_assets

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_tables():
    clear_db_assets()
    yield
    clear_db_assets()


@pytest.fixture
def asset(session: Session) -> AssetModel:
    a = AssetModel(uri="s3://bucket/prefix", name="test_asset", group="test")
    session.add(a)
    session.flush()
    return a


class TestAssetStateModel:
    def test_insert_and_read(self, session: Session, asset: AssetModel):
        """A row written for an asset key is readable back with the correct value."""
        row = AssetStateModel(
            asset_id=asset.id,
            key="last_processed_at",
            value="2026-04-24T00:00:00Z",
        )
        session.add(row)
        session.flush()

        result = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == asset.id,
                AssetStateModel.key == "last_processed_at",
            )
        )
        assert result is not None
        assert result.value == "2026-04-24T00:00:00Z"

    def test_duplicate_pk_raises(self, session: Session, asset: AssetModel):
        """Inserting a second row with the same (asset_id, key) raises IntegrityError."""
        session.add(AssetStateModel(asset_id=asset.id, key="watermark", value="first"))
        session.flush()

        session.add(AssetStateModel(asset_id=asset.id, key="watermark", value="duplicate"))
        with pytest.raises(IntegrityError):
            session.flush()

    def test_multiple_keys_per_asset(self, session: Session, asset: AssetModel):
        """An asset can hold multiple independent keys in the same namespace."""
        for key in ("watermark", "last_file_count", "last_error"):
            session.add(AssetStateModel(asset_id=asset.id, key=key, value=f"val_{key}"))
        session.flush()

        results = session.scalars(select(AssetStateModel).where(AssetStateModel.asset_id == asset.id)).all()
        assert len(results) == 3

    def test_cascade_delete_on_asset(self, session: Session, asset: AssetModel):
        """Deleting an asset cascades to its asset_state rows — no orphans remain."""
        session.add(
            AssetStateModel(
                asset_id=asset.id,
                key="last_processed_at",
                value="2026-04-24T00:00:00Z",
            )
        )
        session.flush()

        session.execute(delete(AssetModel).where(AssetModel.id == asset.id))
        session.flush()

        remaining = session.scalars(select(AssetStateModel).where(AssetStateModel.asset_id == asset.id)).all()
        assert remaining == []

    def test_different_assets_are_isolated(self, session: Session, asset: AssetModel):
        """State written for one asset is not visible when querying a different asset."""
        asset2 = AssetModel(uri="s3://bucket/other", name="other_asset", group="test")
        session.add(asset2)
        session.flush()

        session.add(
            AssetStateModel(
                asset_id=asset.id,
                key="watermark",
                value="asset1_value",
            )
        )
        session.flush()

        result = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == asset2.id,
                AssetStateModel.key == "watermark",
            )
        )
        assert result is None

    def test_state_survives_across_runs(self, session: Session, asset: AssetModel):
        """Asset state is not scoped to a DAG run — updating a value persists it for future reads."""
        row = AssetStateModel(
            asset_id=asset.id,
            key="watermark",
            value="2026-04-01T00:00:00Z",
        )
        session.add(row)
        session.flush()

        row.value = "2026-04-24T00:00:00Z"
        session.flush()

        result = session.scalar(
            select(AssetStateModel).where(
                AssetStateModel.asset_id == asset.id,
                AssetStateModel.key == "watermark",
            )
        )
        assert result is not None
        assert result.value == "2026-04-24T00:00:00Z"
