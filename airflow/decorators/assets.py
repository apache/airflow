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

import types
import typing

import attrs

from airflow.datasets import Dataset as Asset
from airflow.models.dag import DAG, ScheduleArg
from airflow.operators.python import PythonOperator

if typing.TYPE_CHECKING:
    from airflow.io.path import ObjectStoragePath


@attrs.define(kw_only=True)
class AssetDefinition:
    """
    Asset representation from decorating a function with ``@asset``.

    :meta private:
    """

    name: str  # TODO: This should be stored on Asset.
    asset: Asset
    function: types.FunctionType
    schedule: ScheduleArg

    def __attrs_post_init__(self) -> None:
        with DAG(dag_id=self.name, schedule=self.schedule, auto_register=True) as dag:
            PythonOperator(task_id="__main__", outlets=[self.asset], python_callable=self.function)
        # TODO: Currently this just gets serialized into a string.
        # When we create UI for assets, we should add logic to serde so the
        # serialized DAG contains appropriate asset information.
        dag._wrapped_definition = self


@attrs.define(kw_only=True)
class asset:
    """Create an asset by decorating a materialization function."""

    schedule: ScheduleArg
    uri: str | ObjectStoragePath | None
    extra: dict[str, typing.Any] = attrs.field(factory=dict)

    def __call__(self, f: types.FunctionType) -> AssetDefinition:
        if (name := f.__name__) != f.__qualname__:
            raise ValueError("nested function not supported")
        return AssetDefinition(
            name=name,
            asset=Asset(
                uri=name if self.uri is None else str(self.uri),
                extra=self.extra,
            ),
            function=f,
            schedule=self.schedule,
        )
