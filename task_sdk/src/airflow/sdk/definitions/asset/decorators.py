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
from collections.abc import Iterator, Mapping
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
)

import attrs

from airflow.models.asset import _fetch_active_assets_by_name
from airflow.models.dag import DAG, ScheduleArg
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Asset, AssetRef
from airflow.utils.session import create_session

if TYPE_CHECKING:
    from airflow.io.path import ObjectStoragePath
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
        active_assets: dict[str, Asset] = {}
        asset_names = [asset_ref.name for asset_ref in self.inlets if isinstance(asset_ref, AssetRef)]
        if "self" in inspect.signature(self.python_callable).parameters:
            asset_names.append(self._definition_name)

        if asset_names:
            with create_session() as session:
                active_assets = _fetch_active_assets_by_name(asset_names, session)
        return dict(self._iter_kwargs(context, active_assets))


@attrs.define(kw_only=True)
class AssetDefinition(Asset):
    """
    Asset representation from decorating a function with ``@asset``.

    :meta private:
    """

    function: Callable
    schedule: ScheduleArg

    def __attrs_post_init__(self) -> None:
        parameters = inspect.signature(self.function).parameters

        with DAG(dag_id=self.name, schedule=self.schedule, auto_register=True):
            _AssetMainOperator(
                task_id="__main__",
                inlets=[
                    AssetRef(name=inlet_asset_name)
                    for inlet_asset_name in parameters
                    if inlet_asset_name not in ("self", "context")
                ],
                outlets=[self.to_asset()],
                python_callable=self.function,
                definition_name=self.name,
                uri=self.uri,
            )

    def to_asset(self) -> Asset:
        return Asset(
            name=self.name,
            uri=self.uri,
            group=self.group,
            extra=self.extra,
        )

    def serialize(self):
        return {
            "uri": self.uri,
            "name": self.name,
            "group": self.group,
            "extra": self.extra,
        }


@attrs.define(kw_only=True)
class asset:
    """Create an asset by decorating a materialization function."""

    schedule: ScheduleArg
    uri: str | ObjectStoragePath | None = None
    group: str = ""
    extra: dict[str, Any] = attrs.field(factory=dict)
    watchers: list[BaseTrigger] = attrs.field(factory=list)

    def __call__(self, f: Callable) -> AssetDefinition:
        if (name := f.__name__) != f.__qualname__:
            raise ValueError("nested function not supported")

        return AssetDefinition(
            name=name,
            uri=name if self.uri is None else str(self.uri),
            group=self.group,
            extra=self.extra,
            watchers=self.watchers,
            function=f,
            schedule=self.schedule,
        )
