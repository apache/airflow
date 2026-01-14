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

from airflow.api_fastapi.execution_api.datamodels.asset import AssetProfile
from airflow.serialization.definitions.assets import (
    SerializedAsset,
    SerializedAssetAlias,
    SerializedAssetAll,
    SerializedAssetAny,
    SerializedAssetUniqueKey,
)

asset1 = SerializedAsset(
    name="asset-1",
    uri="s3://bucket1/data1/",
    group="group-1",
    extra={},
    watchers=[],
)


def test_asset_iter_assets():
    assert list(asset1.iter_assets()) == [
        (SerializedAssetUniqueKey("asset-1", "s3://bucket1/data1/"), asset1)
    ]


def test_asset_iter_asset_aliases():
    base_asset = SerializedAssetAll(
        [
            SerializedAssetAlias(name="example-alias-1", group=""),
            SerializedAsset(name="1", uri="foo://1/", group="", extra={}, watchers=[]),
            SerializedAssetAny(
                [
                    SerializedAsset(name="2", uri="test://2/", group="", extra={}, watchers=[]),
                    SerializedAssetAlias(name="example-alias-2", group=""),
                    SerializedAsset(name="3", uri="test://3/", group="", extra={}, watchers=[]),
                    SerializedAssetAll(
                        [
                            SerializedAssetAlias("example-alias-3", group=""),
                            SerializedAsset(name="4", uri="test://4/", group="", extra={}, watchers=[]),
                            SerializedAssetAlias("example-alias-4", group=""),
                        ],
                    ),
                ],
            ),
            SerializedAssetAll(
                [
                    SerializedAssetAlias("example-alias-5", group=""),
                    SerializedAsset(name="5", uri="test://5/", group="", extra={}, watchers=[]),
                ],
            ),
        ],
    )
    assert list(base_asset.iter_asset_aliases()) == [
        (f"example-alias-{i}", SerializedAssetAlias(name=f"example-alias-{i}", group="")) for i in range(1, 6)
    ]


def test_asset_alias_as_expression():
    alias = SerializedAssetAlias(name="test_name", group="test")
    assert alias.as_expression() == {"alias": {"name": "test_name", "group": "test"}}


class TestSerializedAssetUniqueKey:
    def test_from_asset(self):
        key = SerializedAssetUniqueKey.from_asset(asset1)
        assert key == SerializedAssetUniqueKey(name="asset-1", uri="s3://bucket1/data1/")

    def test_from_str(self):
        key = SerializedAssetUniqueKey.from_str('{"name": "test", "uri": "test://test/"}')
        assert key == SerializedAssetUniqueKey(name="test", uri="test://test/")

    def test_to_str(self):
        key = SerializedAssetUniqueKey(name="test", uri="test://test/")
        assert key.to_str() == '{"name": "test", "uri": "test://test/"}'

    def test_asprofile(self):
        key = SerializedAssetUniqueKey(name="test", uri="test://test/")
        assert key.asprofile() == AssetProfile(name="test", uri="test://test/", type="Asset")
