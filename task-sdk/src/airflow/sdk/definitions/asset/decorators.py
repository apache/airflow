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
from typing import TYPE_CHECKING, Any, cast

import attrs

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.definitions.asset import Asset, AssetRef, BaseAsset
from airflow.sdk.exceptions import AirflowRuntimeError

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterator, Mapping

    from pydantic.types import JsonValue
    from typing_extensions import Self

    from airflow.sdk import DAG, AssetAlias, ObjectStoragePath
    from airflow.sdk.bases.decorator import _TaskDecorator
    from airflow.sdk.definitions.asset import AssetUniqueKey
    from airflow.sdk.definitions.dag import DagStateChangeCallback, ScheduleArg
    from airflow.sdk.definitions.param import ParamsDict
    from airflow.serialization.dag_dependency import DagDependency
    from airflow.triggers.base import BaseTrigger


def _validate_asset_function_arguments(f: Callable) -> None:
    for name, param in inspect.signature(f).parameters.items():
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(f"wildcard '*{name}' is not supported in @asset")
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            raise TypeError(f"wildcard '**{name}' is not supported in @asset")
        if param.kind == inspect.Parameter.POSITIONAL_ONLY and param.default is inspect.Parameter.empty:
            raise TypeError(f"positional-only argument '{name}' without a default is not supported in @asset")


class _AssetMainOperator(PythonOperator):
    def __init__(self, *, definition_name: str, uri: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._definition_name = definition_name

    @classmethod
    def from_definition(cls, definition: AssetDefinition | MultiAssetDefinition) -> Self:
        _validate_asset_function_arguments(definition._function)
        return cls(
            task_id=definition._function.__name__,
            inlets=[
                Asset.ref(name=inlet_asset_name)
                for inlet_asset_name, param in inspect.signature(definition._function).parameters.items()
                if inlet_asset_name not in ("self", "context") and param.default is inspect.Parameter.empty
            ],
            outlets=[v for _, v in definition.iter_assets()],
            python_callable=definition._function,
            definition_name=definition.name,
        )

    def _iter_kwargs(self, context: Mapping[str, Any]) -> Iterator[tuple[str, Any]]:
        from airflow.sdk.execution_time.comms import ErrorResponse, GetAssetByName
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        def _fetch_asset(name: str) -> Asset:
            resp = SUPERVISOR_COMMS.send(GetAssetByName(name=name))
            if resp is None:
                raise RuntimeError("Empty non-error response received")
            if isinstance(resp, ErrorResponse):
                raise AirflowRuntimeError(resp)
            return Asset(**resp.model_dump(exclude={"type"}))

        value: Any
        for key, param in inspect.signature(self.python_callable).parameters.items():
            if param.default is not inspect.Parameter.empty:
                value = param.default
            elif key == "self":
                value = _fetch_asset(self._definition_name)
            elif key == "context":
                value = context
            else:
                value = _fetch_asset(key)
            yield key, value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return dict(self._iter_kwargs(context))


def _instantiate_task(definition: AssetDefinition | MultiAssetDefinition) -> None:
    decorated_operator = cast("_TaskDecorator", definition._function)
    if getattr(decorated_operator, "_airflow_is_task_decorator", False):
        if "outlets" in decorated_operator.kwargs:
            raise TypeError("@task decorator with 'outlets' argument is not supported in @asset")

        decorated_operator.kwargs["outlets"] = [v for _, v in definition.iter_assets()]
        decorated_operator()
    else:
        _AssetMainOperator.from_definition(definition)


@attrs.define(kw_only=True)
class AssetDefinition(Asset):
    """
    Asset representation from decorating a function with ``@asset``.

    :meta private:
    """

    _function: Callable
    _source: asset

    def __attrs_post_init__(self) -> None:
        with self._source.create_dag(default_dag_id=self.name):
            _instantiate_task(self)


@attrs.define(kw_only=True)
class MultiAssetDefinition(BaseAsset):
    """
    Representation from decorating a function with ``@asset.multi``.

    This is implemented as an "asset-like" object that can be used in all places
    that accept asset-ish things (e.g. normal assets, aliases, AssetAll,
    AssetAny).

    :meta private:
    """

    name: str
    _function: Callable
    _source: asset.multi

    def __attrs_post_init__(self) -> None:
        with self._source.create_dag(default_dag_id=self._function.__name__):
            _instantiate_task(self)

    def iter_assets(self) -> Iterator[tuple[AssetUniqueKey, Asset]]:
        for o in self._source.outlets:
            yield from o.iter_assets()

    def iter_asset_aliases(self) -> Iterator[tuple[str, AssetAlias]]:
        for o in self._source.outlets:
            yield from o.iter_asset_aliases()

    def iter_asset_refs(self) -> Iterator[AssetRef]:
        for o in self._source.outlets:
            yield from o.iter_asset_refs()

    def iter_dag_dependencies(self, *, source: str, target: str) -> Iterator[DagDependency]:
        for obj in self._source.outlets:
            yield from obj.iter_dag_dependencies(source=source, target=target)

    def iter_outlets(self) -> Iterator[BaseAsset]:
        """For asset evaluation in the scheduler."""
        return iter(self._source.outlets)


@attrs.define(kw_only=True)
class _DAGFactory:
    """
    Common class for things that take Dag-like arguments.

    This exists so we don't need to define these arguments separately for
    ``@asset`` and ``@asset.multi``.
    """

    schedule: ScheduleArg
    is_paused_upon_creation: bool | None = None

    dag_id: str | None = None
    dag_display_name: str | None = None
    description: str | None = None

    params: ParamsDict | None = None
    on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None

    access_control: dict[str, dict[str, Collection[str]]] | None = None
    owner_links: dict[str, str] = attrs.field(factory=dict)
    tags: Collection[str] = attrs.field(factory=set)

    def create_dag(self, *, default_dag_id: str) -> DAG:
        from airflow.sdk.definitions.dag import DAG

        dag_id = self.dag_id or default_dag_id
        return DAG(
            dag_id=dag_id,
            schedule=self.schedule,
            is_paused_upon_creation=self.is_paused_upon_creation,
            catchup=False,
            dag_display_name=self.dag_display_name or dag_id,
            description=self.description,
            params=self.params,
            on_success_callback=self.on_success_callback,
            on_failure_callback=self.on_failure_callback,
            access_control=self.access_control,
            owner_links=self.owner_links,
            tags=self.tags,
            auto_register=True,
        )


@attrs.define(kw_only=True)
class asset(_DAGFactory):
    """Create an asset by decorating a materialization function."""

    name: str | None = None
    uri: str | ObjectStoragePath | None = None
    group: str = Asset.asset_type
    extra: dict[str, JsonValue] = attrs.field(factory=dict)
    watchers: list[BaseTrigger] = attrs.field(factory=list)

    @attrs.define(kw_only=True)
    class multi(_DAGFactory):
        """Create a one-task Dag that emits multiple assets."""

        outlets: Collection[BaseAsset]  # TODO: Support non-asset outlets?

        def __call__(self, f: Callable) -> MultiAssetDefinition:
            if f.__name__ != f.__qualname__:
                raise ValueError("nested function not supported")
            if not self.outlets:
                raise ValueError("no outlets provided")
            return MultiAssetDefinition(function=f, source=self, name=f.__name__)

    def __call__(self, f: Callable) -> AssetDefinition:
        if f.__name__ != f.__qualname__:
            raise ValueError("nested function not supported")
        name = self.name or f.__name__
        return AssetDefinition(
            name=name,
            uri=name if self.uri is None else str(self.uri),
            group=self.group,
            extra=self.extra,
            function=f,
            source=self,
        )
