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
from sqlalchemy import and_, false, or_, select

from airflow._shared.state import TaskScope
from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.compat import HTTP_422_UNPROCESSABLE_CONTENT
from airflow.models.dynamic_region import (
    SENTINEL_REGION_ID,
    AmbiguousProducerError,
    DynamicRegion,
    loop_position,
)
from airflow.models.task_coordinates import TaskCoordinateResolver, public_map_index_expression
from airflow.models.taskinstance import TaskInstance

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy import Select
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement

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
        if name == "rendered_map_index" and self.resolver.public_map_index(self.value) < 0:
            return getattr(self.value, "_rendered_map_index", None)
        if name == "in_loop":
            return self.resolver.get_loop_iteration(self.value) is not None
        if name == "loop_iteration":
            if (position := self.resolver.get_loop_iteration(self.value)) is None:
                return None
            return {"loop_id": position[0], "iteration": position[1]}
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


def task_coordinate_responses(
    schema: type[Response],
    values: Iterable[TaskCoordinate],
    resolver: TaskCoordinateResolver,
    *,
    map_indexes: Iterable[int | None] | None = None,
) -> list[Response]:
    values = list(values)
    resolver.prefetch_regions(values)
    indexes = [None] * len(values) if map_indexes is None else list(map_indexes)
    return [
        task_coordinate_response(schema, value, resolver, map_index=map_index)
        for value, map_index in zip(values, indexes, strict=True)
    ]


def loop_iteration_filter(
    *,
    dag_id: str,
    run_id: str,
    loop_id: str,
    iteration: int | None,
    session: Session,
) -> ColumnElement[bool]:
    """
    Match the task instances a named loop produced, optionally narrowed to one iteration.

    Keyed by the loop's node id rather than a region, so it spans every invocation of that loop
    in the run: a filter names a loop the author wrote, not one of its executions. Regions stay
    an internal coordinate -- nothing here asks the caller to know one.
    """
    regions = {
        region.id: region
        for region in session.scalars(
            select(DynamicRegion).where(DynamicRegion.dag_id == dag_id, DynamicRegion.run_id == run_id)
        )
    }
    nested: list[UUID] = []
    own_node: list[UUID] = []
    for region in regions.values():
        position = loop_position(regions, region.id, -1, loop_id)
        if position is None:
            continue
        if iteration is not None and region.node_id == loop_id:
            own_node.append(region.id)
        elif iteration is None or position[1] == iteration:
            nested.append(region.id)
    predicates: list[ColumnElement[bool]] = []
    if nested:
        predicates.append(TaskInstance.region_id.in_(nested))
    if own_node:
        predicates.append(and_(TaskInstance.region_id.in_(own_node), TaskInstance.region_index == iteration))
    return or_(*predicates) if predicates else false()


def region_scope_filter(
    *,
    dag_id: str,
    run_id: str,
    region_id: UUID,
    region_index: int | None,
    session: Session,
) -> ColumnElement[bool]:
    """
    Match task instances at a region coordinate, including the regions nested beneath it.

    One coordinate can span several regions: a loop iteration holds the loop's own region plus
    every mapped expansion created inside that pass, hence the two predicates rather than a
    single equality. For a region with nothing nested under it this degrades to exact match.

    ``region_id`` is resolved to its fork family, so a coordinate keeps resolving after a clear
    replaces the execution with a forked successor.
    """
    regions = {
        region.id: region
        for region in session.scalars(
            select(DynamicRegion).where(DynamicRegion.dag_id == dag_id, DynamicRegion.run_id == run_id)
        )
    }
    target = regions.get(region_id)
    if target is None:
        return false()
    node_id = target.node_id
    origin = loop_position(regions, region_id, -1, node_id)
    if origin is None:
        return false()
    family_id = origin[0]
    nested: list[UUID] = []
    own_node: list[UUID] = []
    for region in regions.values():
        position = loop_position(regions, region.id, -1, node_id)
        if position is None or position[0] != family_id:
            continue
        if region_index is not None and region.node_id == node_id:
            own_node.append(region.id)
        elif region_index is None or position[1] == region_index:
            nested.append(region.id)
    predicates: list[ColumnElement[bool]] = []
    if nested:
        predicates.append(TaskInstance.region_id.in_(nested))
    if own_node:
        predicates.append(
            and_(TaskInstance.region_id.in_(own_node), TaskInstance.region_index == region_index)
        )
    return or_(*predicates) if predicates else false()


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
        raise HTTPException(status.HTTP_409_CONFLICT, "Select a region and index for this task instance")
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
