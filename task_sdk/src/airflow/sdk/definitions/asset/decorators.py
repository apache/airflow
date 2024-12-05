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
from typing import TYPE_CHECKING, Any, Callable

import attrs

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Asset, AssetRef

if TYPE_CHECKING:
    from collections.abc import Collection, Iterator, Mapping

    from airflow.io.path import ObjectStoragePath
    from airflow.models.dag import DagStateChangeCallback, ScheduleArg
    from airflow.models.param import ParamsDict
    from airflow.triggers.base import BaseTrigger


class _AssetMainOperator(PythonOperator):
    def __init__(self, *, definition_name: str, uri: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._definition_name = definition_name
        self._uri = uri

    def _iter_kwargs(
        self, context: Mapping[str, Any], active_assets: dict[str, Asset]
    ) -> Iterator[tuple[str, Any]]:
        value: Any
        for key in inspect.signature(self.python_callable).parameters:
            if key == "self":
                value = active_assets.get(self._definition_name)
            elif key == "context":
                value = context
            else:
                value = active_assets.get(key, Asset(name=key))
            yield key, value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        from airflow.models.asset import _fetch_active_assets_by_name
        from airflow.utils.session import create_session

        asset_names = {asset_ref.name for asset_ref in self.inlets if isinstance(asset_ref, AssetRef)}
        if "self" in inspect.signature(self.python_callable).parameters:
            asset_names.add(self._definition_name)

        if asset_names:
            with create_session() as session:
                active_assets = _fetch_active_assets_by_name(asset_names, session)
        else:
            active_assets = {}
        return dict(self._iter_kwargs(context, active_assets))


@attrs.define(kw_only=True)
class AssetDefinition(Asset):
    """
    Asset representation from decorating a function with ``@asset``.

    :meta private:
    """

    _function: Callable
    _source: asset

    def __attrs_post_init__(self) -> None:
        from airflow.models.dag import DAG

        with DAG(
            dag_id=self.name,
            schedule=self._source.schedule,
            is_paused_upon_creation=self._source.is_paused_upon_creation,
            dag_display_name=self._source.display_name or self.name,
            description=self._source.description,
            params=self._source.params,
            on_success_callback=self._source.on_success_callback,
            on_failure_callback=self._source.on_failure_callback,
            auto_register=True,
        ):
            _AssetMainOperator(
                task_id="__main__",
                inlets=[
                    AssetRef(name=inlet_asset_name)
                    for inlet_asset_name in inspect.signature(self._function).parameters
                    if inlet_asset_name not in ("self", "context")
                ],
                outlets=[self],
                python_callable=self._function,
                definition_name=self.name,
                uri=self.uri,
            )


@attrs.define(kw_only=True)
class asset:
    """Create an asset by decorating a materialization function."""

    uri: str | ObjectStoragePath | None = None
    group: str = Asset.asset_type
    extra: dict[str, Any] = attrs.field(factory=dict)
    watchers: list[BaseTrigger] = attrs.field(factory=list)

    schedule: ScheduleArg
    is_paused_upon_creation: bool | None = None

    display_name: str | None = None
    description: str | None = None

    params: ParamsDict | None = None
    on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None

    access_control: dict[str, dict[str, Collection[str]]] | None = None
    owner_links: dict[str, str] | None = None

    def __call__(self, f: Callable) -> AssetDefinition:
        if self.schedule is not None:
            raise NotImplementedError("asset scheduling not implemented yet")

        if (name := f.__name__) != f.__qualname__:
            raise ValueError("nested function not supported")

        return AssetDefinition(
            name=name,
            uri=name if self.uri is None else str(self.uri),
            group=self.group,
            extra=self.extra,
            function=f,
            source=self,
        )
