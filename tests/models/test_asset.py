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

from airflow.models.asset import AssetAliasModel, AssetModel, expand_alias_to_assets
from airflow.sdk.definitions.asset import AssetAlias

pytestmark = pytest.mark.db_test


@pytest.fixture
def clear_assets():
    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


def test_asset_alias_from_public():
    asset_alias = AssetAlias(name="test_alias")
    asset_alias_model = AssetAliasModel.from_public(asset_alias)
    assert asset_alias_model.name == "test_alias"


class TestAssetAliasModel:
    @pytest.fixture
    def asset_model(self, session):
        """Example asset links to asset alias resolved_asset_alias_2."""
        asset_model = AssetModel(
            id=1,
            uri="test://asset1/",
            name="test_name",
            group="asset",
        )
        session.add(asset_model)
        session.flush()
        return asset_model

    @pytest.fixture
    def asset_alias_1(self, session):
        """Example asset alias links to no assets."""
        asset_alias_model = AssetAliasModel(name="test_name", group="test")
        session.add(asset_alias_model)
        session.flush()
        return asset_alias_model

    @pytest.fixture
    def resolved_asset_alias_2(self, session, asset_model):
        """Example asset alias links to asset asset_alias_1."""
        asset_alias_2 = AssetAliasModel(name="test_name_2")
        asset_alias_2.assets.append(asset_model)
        session.add(asset_alias_2)
        session.flush()
        return asset_alias_2

    def test_expand_alias_to_assets_empty(self, session, asset_alias_1):
        assert expand_alias_to_assets(asset_alias_1.name, session) == []

    def test_expand_alias_to_assets_resolved(self, session, resolved_asset_alias_2, asset_model):
        assert expand_alias_to_assets(resolved_asset_alias_2.name, session) == [asset_model]
