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

import functools
from typing import TYPE_CHECKING

import attrs

from airflow.models.asset import expand_alias_to_assets, resolve_ref_to_asset
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetBooleanCondition,
    AssetRef,
    AssetUniqueKey,
    BaseAsset,
)
from airflow.sdk.definitions.asset.decorators import MultiAssetDefinition

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@attrs.define
class AssetEvaluator:
    """Evaluates whether an asset-like object has been satisfied."""

    _session: Session

    def _resolve_asset_ref(self, o: AssetRef) -> Asset | None:
        asset = resolve_ref_to_asset(**attrs.asdict(o), session=self._session)
        return asset.to_public() if asset else None

    def _resolve_asset_alias(self, o: AssetAlias) -> list[Asset]:
        asset_models = expand_alias_to_assets(o.name, session=self._session)
        return [m.to_public() for m in asset_models]

    @functools.singledispatchmethod
    def run(self, o: BaseAsset, statuses: dict[AssetUniqueKey, bool]) -> bool:
        raise NotImplementedError(f"can not evaluate {o!r}")

    @run.register
    def _(self, o: Asset, statuses: dict[AssetUniqueKey, bool]) -> bool:
        return statuses.get(AssetUniqueKey.from_asset(o), False)

    @run.register
    def _(self, o: AssetRef, statuses: dict[AssetUniqueKey, bool]) -> bool:
        if asset := self._resolve_asset_ref(o):
            return self.run(asset, statuses)
        return False

    @run.register
    def _(self, o: AssetAlias, statuses: dict[AssetUniqueKey, bool]) -> bool:
        return any(self.run(x, statuses) for x in self._resolve_asset_alias(o))

    @run.register
    def _(self, o: AssetBooleanCondition, statuses: dict[AssetUniqueKey, bool]) -> bool:
        return o.agg_func(self.run(x, statuses) for x in o.objects)

    @run.register
    def _(self, o: MultiAssetDefinition, statuses: dict[AssetUniqueKey, bool]) -> bool:
        return all(self.run(x, statuses) for x in o.iter_outlets())
