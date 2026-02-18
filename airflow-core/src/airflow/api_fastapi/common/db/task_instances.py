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
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.interfaces import LoaderOption

from airflow.api_fastapi.core_api.datamodels.task_instances import TaskInstanceResponse
from airflow.models import Base
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.taskinstance import TaskInstance, TaskInstanceNote


def _nested_model(tp: Any) -> type[BaseModel] | None:
    origin = get_origin(tp)
    if origin in (Union, types.UnionType):
        for arg in get_args(tp):
            if isinstance(arg, type) and issubclass(arg, BaseModel):
                return arg
        return None
    if isinstance(tp, type) and issubclass(tp, BaseModel):
        return tp
    return None


def _columns(orm_cls: type) -> set[str]:
    return {a.key for a in sa_inspect(orm_cls).column_attrs}


def _fields(model: type[BaseModel]) -> set[str]:
    names: set[str] = set()
    for name, field in model.model_fields.items():
        nested = _nested_model(field.annotation)
        if nested:
            names |= _fields(nested)
        else:
            names.add(name)
    return names


def _attrs(orm_cls: type, names: set[str]):
    return [getattr(orm_cls, n) for n in sorted(names) if hasattr(orm_cls, n)]


def eager_load_TI_and_TIH_for_validation(orm_model: Base | None = None) -> tuple[LoaderOption, ...]:
    """Construct the eager loading options necessary for both TaskInstanceResponse and TaskInstanceHistoryResponse objects."""
    if orm_model is None:
        orm_model = TaskInstance

    tir = _fields(TaskInstanceResponse)
    ti_cols = _columns(TaskInstance)
    dv_cols = _columns(DagVersion)
    dbm_cols = _columns(DagBundleModel)
    tin_cols = _columns(TaskInstanceNote)

    options: list[LoaderOption] = []

    if tir & (dv_cols - ti_cols):
        if tir & (dbm_cols - ti_cols - dv_cols):
            options.append(
                selectinload(orm_model.dag_version)
                .load_only(*_attrs(DagVersion, tir & (dv_cols - ti_cols)))
                .selectinload(DagVersion.bundle)
                .load_only(*_attrs(DagBundleModel, tir & (dbm_cols - ti_cols - dv_cols)))
            )
        else:
            options.append(
                selectinload(orm_model.dag_version).load_only(*_attrs(DagVersion, tir & (dv_cols - ti_cols)))
            )

    if orm_model is TaskInstance:
        tin_names = tir & (ti_cols - tin_cols) | {"content"}
        if tin_names:
            options.append(
                selectinload(orm_model.task_instance_note).load_only(*_attrs(TaskInstanceNote, tin_names))
            )
    return tuple(options)
