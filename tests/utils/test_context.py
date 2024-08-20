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

from airflow.assets import AssetAlias, AssetAliasEvent, Dataset
from airflow.models.dataset import AssetAliasModel, AssetModel
from airflow.utils.context import OutletEventAccessor, OutletEventAccessors


class TestOutletEventAccessor:
    @pytest.mark.parametrize(
        "raw_key, asset_alias_events",
        (
            (
                AssetAlias("test_alias"),
                [AssetAliasEvent(source_alias_name="test_alias", dest_asset_uri="test_uri", extra={})],
            ),
            (Dataset("test_uri"), []),
        ),
    )
    def test_add(self, raw_key, asset_alias_events):
        outlet_event_accessor = OutletEventAccessor(raw_key=raw_key, extra={})
        outlet_event_accessor.add(Dataset("test_uri"))
        assert outlet_event_accessor.asset_alias_events == asset_alias_events

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "raw_key, asset_alias_events",
        (
            (
                AssetAlias("test_alias"),
                [AssetAliasEvent(source_alias_name="test_alias", dest_dataset_uri="test_uri", extra={})],
            ),
            (
                "test_alias",
                [AssetAliasEvent(source_alias_name="test_alias", dest_dataset_uri="test_uri", extra={})],
            ),
            (Dataset("test_uri"), []),
        ),
    )
    def test_add_with_db(self, raw_key, asset_alias_events, session):
        asm = AssetModel(uri="test_uri")
        aam = AssetAliasModel(name="test_alias")
        session.add_all([asm, aam])
        session.flush()

        outlet_event_accessor = OutletEventAccessor(raw_key=raw_key, extra={"not": ""})
        outlet_event_accessor.add("test_uri", extra={})
        assert outlet_event_accessor.asset_alias_events == asset_alias_events


class TestOutletEventAccessors:
    @pytest.mark.parametrize("key", ("test", Dataset("test"), AssetAlias("test_alias")))
    def test____get_item___dict_key_not_exists(self, key):
        outlet_event_accessors = OutletEventAccessors()
        assert len(outlet_event_accessors) == 0
        outlet_event_accessor = outlet_event_accessors[key]
        assert len(outlet_event_accessors) == 1
        assert outlet_event_accessor.raw_key == key
        assert outlet_event_accessor.extra == {}
