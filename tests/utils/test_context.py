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

import pytest

from airflow.models.asset import AssetAliasModel, AssetModel
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAliasUniqueKey, AssetUniqueKey
from airflow.utils.context import AssetAliasEvent, OutletEventAccessor, OutletEventAccessors


class TestOutletEventAccessor:
    @pytest.mark.parametrize(
        "key, asset_alias_events",
        (
            (AssetUniqueKey.from_asset(Asset("test_uri")), []),
            (
                AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias")),
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(uri="test_uri", name="test_uri"),
                        extra={},
                    )
                ],
            ),
        ),
    )
    def test_add(self, key, asset_alias_events):
        outlet_event_accessor = OutletEventAccessor(key=key, extra={})
        outlet_event_accessor.add(Asset("test_uri"))
        assert outlet_event_accessor.asset_alias_events == asset_alias_events

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "key, asset_alias_events",
        (
            (
                "test_alias",
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(name="test-asset", uri="test://asset-uri/"),
                        extra={},
                    )
                ],
            ),
            (AssetUniqueKey.from_asset(Asset("test_uri")), []),
            (
                AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias")),
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(name="test-asset", uri="test://asset-uri/"),
                        extra={},
                    )
                ],
            ),
        ),
    )
    def test_add_with_db(self, key, asset_alias_events, session):
        asm = AssetModel(uri="test://asset-uri", name="test-asset", group="asset")
        aam = AssetAliasModel(name="test_alias")
        session.add_all([asm, aam])
        session.flush()
        asset = Asset(uri="test://asset-uri", name="test-asset")

        outlet_event_accessor = OutletEventAccessor(key=key, extra={"not": ""})
        outlet_event_accessor.add(asset, extra={})
        assert outlet_event_accessor.asset_alias_events == asset_alias_events


class TestOutletEventAccessors:
    @pytest.mark.parametrize(
        "access_key, internal_key",
        (
            ("test", "test"),
            (Asset("test"), AssetUniqueKey.from_asset(Asset("test"))),
            (
                Asset(name="test", uri="test://asset"),
                AssetUniqueKey.from_asset(Asset(name="test", uri="test://asset")),
            ),
            (AssetAlias("test_alias"), AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias"))),
        ),
    )
    def test___get_item__dict_key_not_exists(self, access_key, internal_key):
        outlet_event_accessors = OutletEventAccessors()
        assert len(outlet_event_accessors) == 0
        outlet_event_accessor = outlet_event_accessors[access_key]
        assert len(outlet_event_accessors) == 1
        assert outlet_event_accessor.key == internal_key
        assert outlet_event_accessor.extra == {}
