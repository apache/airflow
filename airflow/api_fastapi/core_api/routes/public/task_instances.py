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

from fastapi import Depends, HTTPException, Request, status
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.sql import select
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryLimit,
    QueryOffset,
    QueryTIExecutorFilter,
    QueryTIPoolFilter,
    QueryTIQueueFilter,
    QueryTIStateFilter,
    RangeFilter,
    SortParam,
    datetime_range_filter_factory,
    float_range_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.serializers.task_instances import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
)
from airflow.exceptions import TaskNotFound
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils.db import get_query_count

task_instances_router = AirflowRouter(
    tags=["Task Instance"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
)


@task_instances_router.get(
    "/{task_id}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
    ),
)
async def get_task_instance(
    dag_id: str, dag_run_id: str, task_id: str, session: Annotated[Session, Depends(get_session)]
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}` and task_id: `{task_id}` was not found",
        )
    if task_instance.map_index != -1:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "Task instance is mapped, add the map_index value to the URL"
        )

    return TaskInstanceResponse.model_validate(task_instance, from_attributes=True)


@task_instances_router.get(
    "/{task_id}/listMapped",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
    ),
)
async def get_mapped_task_instances(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    request: Request,
    logical_date_range: Annotated[
        RangeFilter, Depends(datetime_range_filter_factory("logical_date", TI, "execution_date"))
    ],
    start_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("start_date", TI))],
    end_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("end_date", TI))],
    update_at_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("updated_at", TI))],
    duration_range: Annotated[RangeFilter, Depends(float_range_filter_factory("duration", TI))],
    state: QueryTIStateFilter,
    pool: QueryTIPoolFilter,
    queue: QueryTIQueueFilter,
    executor: QueryTIExecutorFilter,
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["id", "state", "duration", "start_date", "end_date", "map_index", "rendered_map_index"],
                TI,
            ).dynamic_depends(default="map_index")
        ),
    ],
    session: Annotated[Session, Depends(get_session)],
) -> TaskInstanceCollectionResponse:
    """Get list of mapped task instances."""
    base_query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index >= 0)
        .join(TI.dag_run)
    )
    # 0 can mean a mapped TI that expanded to an empty list, so it is not an automatic 404
    unfiltered_total_count = get_query_count(base_query, session=session)
    if unfiltered_total_count == 0:
        dag = request.app.state.dag_bag.get_dag(dag_id)
        if not dag:
            error_message = f"DAG {dag_id} not found"
            raise HTTPException(404, error_message)
        try:
            task = dag.get_task(task_id)
        except TaskNotFound:
            error_message = f"Task id {task_id} not found"
            raise HTTPException(404, error_message)
        if not task.get_needs_expansion():
            error_message = f"Task id {task_id} is not mapped"
            raise HTTPException(404, error_message)

    task_instance_select, total_entries = paginated_select(
        base_query,
        [
            logical_date_range,
            start_date_range,
            end_date_range,
            update_at_range,
            duration_range,
            state,
            pool,
            queue,
            executor,
        ],
        order_by,
        offset,
        limit,
        session,
    )

    task_instances = session.scalars(task_instance_select).all()

    return TaskInstanceCollectionResponse(
        task_instances=[
            TaskInstanceResponse.model_validate(task_instance, from_attributes=True)
            for task_instance in task_instances
        ],
        total_entries=total_entries,
    )


@task_instances_router.get(
    "/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc(
        [status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
    ),
)
async def get_mapped_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    session: Annotated[Session, Depends(get_session)],
) -> TaskInstanceResponse:
    """Get task instance."""
    query = (
        select(TI)
        .where(TI.dag_id == dag_id, TI.run_id == dag_run_id, TI.task_id == task_id, TI.map_index == map_index)
        .join(TI.dag_run)
        .options(joinedload(TI.rendered_task_instance_fields))
    )
    task_instance = session.scalar(query)

    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Mapped Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}`, and map_index: `{map_index}` was not found",
        )

    return TaskInstanceResponse.model_validate(task_instance, from_attributes=True)
