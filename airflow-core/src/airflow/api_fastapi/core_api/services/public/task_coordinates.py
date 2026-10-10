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

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, TypeVar
from uuid import UUID

from fastapi import Depends, HTTPException, Query, status
from pydantic import BaseModel

from airflow._shared.state import TaskScope
from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.models.dynamic_region import SENTINEL_REGION_ID, AmbiguousProducerError
from airflow.models.task_coordinates import TaskCoordinateResolver, public_map_index_expression
from airflow.models.taskinstance import TaskInstance

if TYPE_CHECKING:
    from sqlalchemy import Select

    from airflow.models.task_coordinates import TaskCoordinate


Response = TypeVar("Response", bound=BaseModel)


@dataclass
class TaskCoordinateView:
    """Present public mapping coordinates without changing ORM state."""

    value: TaskCoordinate
    resolver: TaskCoordinateResolver
    projected_map_index: int | None = None

    def __getattr__(self, name: str) -> Any:
        if name == "map_index":
            if self.projected_map_index is not None:
                return self.projected_map_index
            return self.resolver.public_map_index(self.value)
        return getattr(self.value, name)


def add_public_map_index(statement: Select) -> Select:
    """Select each row's public map index next to it, so sorting and presenting agree."""
    return statement.add_columns(public_map_index_expression(TaskInstance).label("map_index"))


def task_coordinate_response(
    schema: type[Response],
    value: TaskCoordinate,
    resolver: TaskCoordinateResolver,
    *,
    map_index: int | None = None,
) -> Response:
    return schema.model_validate(TaskCoordinateView(value, resolver, map_index))


def resolve_task_scope(
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    resolver: TaskCoordinateResolver,
    map_index: int = -1,
    region_id: UUID | None = None,
    region_index: int | None = None,
    all_map_indices: bool = False,
) -> TaskScope:
    """Resolve a public data address, including explicitly addressed retained data."""
    if region_index is not None and region_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "region_index requires region_id")
    if region_id is not None and region_index is not None:
        if map_index not in (-1, region_index):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "map_index conflicts with region_index")
        return TaskScope(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=region_index,
            region_id=region_id,
        )
    if region_id == SENTINEL_REGION_ID or not resolver.has_regions(dag_id, run_id, task_id):
        return TaskScope(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            region_id=region_id or SENTINEL_REGION_ID,
        )
    try:
        if resolver.is_loop_pass_unselected(dag_id, run_id, task_id, region_id):
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "Select a region and index for this task instance"
            )
        tasks = resolver.resolve(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            region_id=region_id,
            map_indexes=None if all_map_indices else map_index,
        )
    except AmbiguousProducerError as error:
        raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
    except ValueError as error:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(error)) from error
    if not tasks:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Task instance not found for selected coordinates")
    if len({ti.region_id for ti in tasks}) != 1 or (not all_map_indices and len(tasks) != 1):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Select a region and index for this task instance")
    return TaskScope(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        map_index=tasks[0].region_index,
        region_id=tasks[0].region_id,
    )


def _coordinate_resolver(dag_bag: DagBagDep, session: SessionDep) -> TaskCoordinateResolver:
    return TaskCoordinateResolver(dag_bag, session)


CoordinateResolverDep = Annotated[TaskCoordinateResolver, Depends(_coordinate_resolver)]


def _task_scope(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    resolver: CoordinateResolverDep,
    map_index: int = -1,
    region_id: Annotated[UUID | None, Query()] = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
) -> TaskScope:
    if map_index < -1:
        raise HTTPException(HTTP_422_UNPROCESSABLE_CONTENT, "map_index must be greater than or equal to -1")
    return resolve_task_scope(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        resolver=resolver,
        map_index=map_index,
        region_id=region_id,
        region_index=region_index,
    )


def _unmapped_task_scope(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    resolver: CoordinateResolverDep,
    region_id: Annotated[UUID | None, Query()] = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
) -> TaskScope:
    return resolve_task_scope(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        resolver=resolver,
        region_id=region_id,
        region_index=region_index,
    )


TaskScopeDep = Annotated[TaskScope, Depends(_task_scope)]
UnmappedTaskScopeDep = Annotated[TaskScope, Depends(_unmapped_task_scope)]
