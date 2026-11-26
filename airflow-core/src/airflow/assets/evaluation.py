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
from airflow.serialization.definitions.assets import (
    SerializedAsset,
    SerializedAssetAlias,
    SerializedAssetAll,
    SerializedAssetAny,
    SerializedAssetBase,
    SerializedAssetBooleanCondition,
    SerializedAssetRef,
    SerializedAssetUniqueKey,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@attrs.define
class AssetEvaluator:
    """Evaluates whether an asset-like object has been satisfied."""

    _session: Session

    def _resolve_asset_ref(self, o: SerializedAssetRef) -> SerializedAsset | None:
        asset = resolve_ref_to_asset(**attrs.asdict(o), session=self._session)
        return asset.to_serialized() if asset else None

    def _resolve_asset_alias(self, o: SerializedAssetAlias) -> list[SerializedAsset]:
        asset_models = expand_alias_to_assets(o.name, session=self._session)
        return [m.to_serialized() for m in asset_models]

    @functools.singledispatchmethod
    def run(self, o: SerializedAssetBase, statuses: dict[SerializedAssetUniqueKey, bool]) -> bool:
        raise NotImplementedError(f"can not evaluate {o!r}")

    @run.register
    def _(self, o: SerializedAsset, statuses: dict[SerializedAssetUniqueKey, bool]) -> bool:
        return statuses.get(SerializedAssetUniqueKey.from_asset(o), False)

    @run.register
    def _(self, o: SerializedAssetRef, statuses: dict[SerializedAssetUniqueKey, bool]) -> bool:
        if asset := self._resolve_asset_ref(o):
            return self.run(asset, statuses)
        return False

    @run.register
    def _(self, o: SerializedAssetAlias, statuses: dict[SerializedAssetUniqueKey, bool]) -> bool:
        return any(self.run(x, statuses) for x in self._resolve_asset_alias(o))

    @run.register
    def _(self, o: SerializedAssetBooleanCondition, statuses: dict[SerializedAssetUniqueKey, bool]) -> bool:
        return type(o).agg_func(self.run(x, statuses) for x in o.objects)

    def uses_branch_consumption(self, o: SerializedAssetBase) -> bool:
        """Return whether any ``AssetAny`` in the condition consumes only its satisfied branches."""
        if isinstance(o, SerializedAssetAny) and o.consume == "branch":
            return True
        if isinstance(o, SerializedAssetBooleanCondition):
            return any(self.uses_branch_consumption(x) for x in o.objects)
        return False

    def _find_queued_keys(
        self, o: SerializedAssetBase, statuses: dict[SerializedAssetUniqueKey, bool]
    ) -> set[SerializedAssetUniqueKey]:
        match o:
            case SerializedAsset():
                key = SerializedAssetUniqueKey.from_asset(o)
                return {key} if statuses.get(key) else set()
            case SerializedAssetRef():
                asset = self._resolve_asset_ref(o)
                return self._find_queued_keys(asset, statuses) if asset else set()
            case SerializedAssetAlias():
                return set().union(
                    *(self._find_queued_keys(x, statuses) for x in self._resolve_asset_alias(o))
                )
            case SerializedAssetBooleanCondition():
                return set().union(*(self._find_queued_keys(x, statuses) for x in o.objects))
        raise NotImplementedError(f"can not evaluate {o!r}")

    def find_satisfied_keys(
        self, o: SerializedAssetBase, statuses: dict[SerializedAssetUniqueKey, bool]
    ) -> set[SerializedAssetUniqueKey] | None:
        """
        Return the queued asset keys a Dag run triggered by *o* should consume.

        Return *None* if *o* is not satisfied. An ``AssetAny`` with
        ``consume="branch"`` contributes only the keys of its satisfied children;
        every other node contributes all its queued keys, matching the
        consume-everything behaviour.
        """
        if not self.run(o, statuses):
            return None
        if isinstance(o, SerializedAssetAny) and o.consume == "branch":
            return set().union(
                *(keys for x in o.objects if (keys := self.find_satisfied_keys(x, statuses)) is not None)
            )
        if isinstance(o, SerializedAssetAll):
            return set().union(*((self.find_satisfied_keys(x, statuses) or set()) for x in o.objects))
        return self._find_queued_keys(o, statuses)
