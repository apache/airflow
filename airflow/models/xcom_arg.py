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

from functools import singledispatch
from typing import TYPE_CHECKING

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from airflow.sdk.definitions._internal.types import ArgNotSet
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.xcom_arg import (
    ConcatXComArg,
    MapXComArg,
    PlainXComArg,
    XComArg,
    ZipXComArg,
)
from airflow.utils.db import exists_query
from airflow.utils.state import State
from airflow.utils.xcom import XCOM_RETURN_KEY

__all__ = ["XComArg", "get_task_map_length"]

if TYPE_CHECKING:
    from airflow.models.expandinput import OperatorExpandArgument


@singledispatch
def get_task_map_length(xcom_arg: OperatorExpandArgument, run_id: str, *, session: Session) -> int | None:
    # The base implementation -- specific XComArg subclasses have specialised implementations
    raise NotImplementedError()


@get_task_map_length.register
def _(xcom_arg: PlainXComArg, run_id: str, *, session: Session):
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskmap import TaskMap
    from airflow.models.xcom import XCom

    dag_id = xcom_arg.operator.dag_id
    task_id = xcom_arg.operator.task_id
    is_mapped = isinstance(xcom_arg.operator, MappedOperator)

    if is_mapped:
        unfinished_ti_exists = exists_query(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.task_id == task_id,
            # Special NULL treatment is needed because 'state' can be NULL.
            # The "IN" part would produce "NULL NOT IN ..." and eventually
            # "NULl = NULL", which is a big no-no in SQL.
            or_(
                TaskInstance.state.is_(None),
                TaskInstance.state.in_(s.value for s in State.unfinished if s is not None),
            ),
            session=session,
        )
        if unfinished_ti_exists:
            return None  # Not all of the expanded tis are done yet.
        query = select(func.count(XCom.map_index)).where(
            XCom.dag_id == dag_id,
            XCom.run_id == run_id,
            XCom.task_id == task_id,
            XCom.map_index >= 0,
            XCom.key == XCOM_RETURN_KEY,
        )
    else:
        query = select(TaskMap.length).where(
            TaskMap.dag_id == dag_id,
            TaskMap.run_id == run_id,
            TaskMap.task_id == task_id,
            TaskMap.map_index < 0,
        )
    return session.scalar(query)


@get_task_map_length.register
def _(xcom_arg: MapXComArg, run_id: str, *, session: Session):
    return get_task_map_length(xcom_arg.arg, run_id, session=session)


@get_task_map_length.register
def _(xcom_arg: ZipXComArg, run_id: str, *, session: Session):
    all_lengths = (get_task_map_length(arg, run_id, session=session) for arg in xcom_arg.args)
    ready_lengths = [length for length in all_lengths if length is not None]
    if len(ready_lengths) != len(xcom_arg.args):
        return None  # If any of the referenced XComs is not ready, we are not ready either.
    if isinstance(xcom_arg.fillvalue, ArgNotSet):
        return min(ready_lengths)
    return max(ready_lengths)


@get_task_map_length.register
def _(xcom_arg: ConcatXComArg, run_id: str, *, session: Session):
    all_lengths = (get_task_map_length(arg, run_id, session=session) for arg in xcom_arg.args)
    ready_lengths = [length for length in all_lengths if length is not None]
    if len(ready_lengths) != len(xcom_arg.args):
        return None  # If any of the referenced XComs is not ready, we are not ready either.
    return sum(ready_lengths)
