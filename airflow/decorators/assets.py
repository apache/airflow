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
import types
from typing import TYPE_CHECKING, Any, Iterator, Mapping

import attrs

from airflow.assets import Asset, _validate_identifier
from airflow.models.dag import DAG, ScheduleArg
from airflow.operators.python import PythonOperator

if TYPE_CHECKING:
    from airflow.io.path import ObjectStoragePath


@attrs.define(kw_only=True)
class AssetRef:
    """Reference to an asset."""

    name: str


class _AssetMainOperator(PythonOperator):
    def __init__(self, *, definition_name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._definition_name = definition_name

    def _iter_kwargs(self, context: Mapping[str, Any]) -> Iterator[tuple[str, Any]]:
        for key in inspect.signature(self.python_callable).parameters:
            if key == "self":
                value: Any = AssetRef(name=self._definition_name)
            elif key == "context":
                value = context
            else:
                # TODO: This does not check if the upstream asset actually
                # exists. Should we do a second pass in the DAG processor to
                # raise parse-time errors if a non-existent asset is referenced?
                # How? Should we also fail the task at runtime? Or should the
                # dangling reference simply do nothing?
                value = AssetRef(name=key)
            yield key, value

    def determine_kwargs(self, context: Mapping[str, Any]) -> Mapping[str, Any]:
        return dict(self._iter_kwargs(context))


@attrs.define(kw_only=True)
class AssetDefinition(Asset):
    """
    Asset representation from decorating a function with ``@asset``.

    :meta private:
    """

    function: types.FunctionType
    schedule: ScheduleArg

    def __attrs_post_init__(self) -> None:
        parameters = inspect.signature(self.function).parameters
        with DAG(dag_id=self.name, schedule=self.schedule, auto_register=True) as dag:
            _AssetMainOperator(
                task_id="__main__",
                inlets=[
                    Asset(name=inlet_aset_name)
                    for inlet_aset_name in parameters
                    if inlet_aset_name not in ("self", "context")
                ],
                outlets=[self],
                python_callable=self.function,
                definition_name=self.name,
            )
        # TODO: Currently this just gets serialized into a string.
        # When we create UI for assets, we should add logic to serde so the
        # serialized DAG contains appropriate asset information.
        dag._wrapped_definition = self

    def serialize(self):
        return "AssetDefinition"


@attrs.define(kw_only=True)
class asset:
    """Create an asset by decorating a materialization function."""

    schedule: ScheduleArg
    uri: str | ObjectStoragePath | None = None
    group: str = attrs.field(
        kw_only=True,
        default="",
        validator=[attrs.validators.max_len(1500), _validate_identifier],
    )
    extra: dict[str, Any] = attrs.field(factory=dict)

    def __call__(self, f: types.FunctionType) -> AssetDefinition:
        if (name := f.__name__) != f.__qualname__:
            raise ValueError("nested function not supported")
        if name == "self" or name == "context":
            raise ValueError(f"prohibited name for asset: {name}")
        return AssetDefinition(
            name=name,
            uri=name if self.uri is None else str(self.uri),
            group=self.group,
            extra=self.extra,
            function=f,
            schedule=self.schedule,
        )
