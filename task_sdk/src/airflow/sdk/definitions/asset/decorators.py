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

import inspect
from typing import TYPE_CHECKING, Any

import attrs

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Asset, AssetNameRef, AssetRef, BaseAsset

# ✅ Import the correct DAG class from SDK
from airflow.sdk.dag import SdkDAG  # ✅ Updated import

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterator, Mapping

    from sqlalchemy.orm import Session

    from airflow.io.path import ObjectStoragePath
    from airflow.sdk.definitions.asset import AssetAlias, AssetUniqueKey
    from airflow.sdk.definitions.param import ParamsDict
    from airflow.serialization.dag_dependency import DagDependency
    from airflow.triggers.base import BaseTrigger
    from airflow.typing_compat import Self


class _AssetMainOperator(PythonOperator):
    """Custom Operator for Asset Execution"""

    def __init__(self, *, definition_name: str, uri: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._definition_name = definition_name

    @classmethod
    def from_definition(cls, definition: AssetDefinition | MultiAssetDefinition) -> Self:
        """Creates the Operator from an Asset Definition."""
        return cls(
            task_id="__main__",
            inlets=[
                Asset.ref(name=inlet_asset_name)
                for inlet_asset_name in inspect.signature(definition._function).parameters
                if inlet_asset_name not in ("self", "context")
            ],
            outlets=[v for _, v in definition.iter_assets()],
            python_callable=definition._function,
            definition_name=definition._function.__name__,
        )

    def _iter_kwargs(
        self, context: Mapping[str, Any], active_assets: dict[str, Asset]
    ) -> Iterator[tuple[str, Any]]:
        """Extracts function arguments based on asset context."""
        for key in inspect.signature(self.python_callable).parameters:
            if key == "self":
                value = active_assets.get(self._definition_name)
            elif key == "context":
                value = context
            else:
                value = active_assets.get(key, Asset(name=key))
            yield key, value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        """Determines which kwargs to pass into the function."""
        from airflow.models.asset import fetch_active_assets_by_name
        from airflow.utils.session import create_session

        asset_names = {asset_ref.name for asset_ref in self.inlets if isinstance(asset_ref, AssetNameRef)}
        if "self" in inspect.signature(self.python_callable).parameters:
            asset_names.add(self._definition_name)

        if asset_names:
            with create_session() as session:
                active_assets = fetch_active_assets_by_name(asset_names, session)
        else:
            active_assets = {}

        return dict(self._iter_kwargs(context, active_assets))


@attrs.define(kw_only=True)
class AssetDefinition(Asset):
    """Represents an Asset from `@asset` decorator."""

    _function: Callable
    _source: asset

    def __attrs_post_init__(self) -> None:
        """Creates a DAG using SdkDAG inside AssetDefinition."""
        with self._source.create_dag(dag_id=self.name):
            _AssetMainOperator.from_definition(self)


@attrs.define(kw_only=True)
class MultiAssetDefinition(BaseAsset):
    """Represents multiple assets from `@asset.multi` decorator."""

    _function: Callable
    _source: asset.multi

    def __attrs_post_init__(self) -> None:
        """Creates a DAG for multi-asset definition."""
        with self._source.create_dag(dag_id=self._function.__name__):
            _AssetMainOperator.from_definition(self)

    def evaluate(self, statuses: dict[AssetUniqueKey, bool], *, session: Session | None = None) -> bool:
        """Evaluates asset statuses."""
        return all(o.evaluate(statuses=statuses, session=session) for o in self._source.outlets)

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        """Iterates over all assets."""
        for o in self._source.outlets:
            yield from o.iter_assets()

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        """Iterates over asset aliases."""
        for o in self._source.outlets:
            yield from o.iter_asset_aliases()

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        """Iterates over asset references."""
        for o in self._source.outlets:
            yield from o.iter_asset_refs()

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        """Iterates over DAG dependencies."""
        for obj in self._source.outlets:
            yield from obj.iter_dag_dependencies(source=source, target=target)


@attrs.define(kw_only=True)
class _DAGFactory:
    """Common class for things that take DAG-like arguments."""

    schedule: ScheduleArg
    is_paused_upon_creation: bool | None = None
    display_name: str | None = None
    description: str | None = None
    params: ParamsDict | None = None
    on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    access_control: dict[str, dict[str, Collection[str]]] | None = None
    owner_links: dict[str, str] | None = None

    def create_dag(self, *, dag_id: str) -> SdkDAG:
        """✅ Uses `SdkDAG` Instead of Standard DAG."""
        return SdkDAG(
            dag_id=dag_id,
            schedule=self.schedule,
            is_paused_upon_creation=self.is_paused_upon_creation,
            catchup=False,
            dag_display_name=self.display_name or dag_id,
            description=self.description,
            params=self.params,
            on_success_callback=self.on_success_callback,
            on_failure_callback=self.on_failure_callback,
            auto_register=True,
        )

