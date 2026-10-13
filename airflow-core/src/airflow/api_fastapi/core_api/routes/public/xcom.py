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
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run_or_latest_version
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.db.dags import eager_load_teams
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    QueryXComDagDisplayNamePatternSearch,
    QueryXComDagDisplayNamePrefixPatternSearch,
    QueryXComKeyPatternSearch,
    QueryXComKeyPrefixPatternSearch,
    QueryXComRunIdPatternSearch,
    QueryXComRunIdPrefixPatternSearch,
    QueryXComTaskIdPatternSearch,
    QueryXComTaskIdPrefixPatternSearch,
    RangeFilter,
    SortParam,
    _DagIdTeamsFilter,
    datetime_range_filter_factory,
    filter_param_factory,
    teams_filter_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.xcom import (
    XComCollectionResponse,
    XComCreateBody,
    XComResponse,
    XComResponseNative,
    XComResponseString,
    XComUpdateBody,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import ReadableXComFilterDep, requires_access_dag
from airflow.api_fastapi.core_api.services.public.task_coordinates import TaskScopeDep, resolve_task_scope
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import TaskNotFound
from airflow.models import DagRun as DR
from airflow.models.dag import DagModel
from airflow.models.task_coordinates import TaskCoordinateResolver, public_map_index_expression
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel, build_xcom_read_query, select_producers, xcom_entity

xcom_router = AirflowRouter(
    tags=["XCom"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"
)


@dataclass(frozen=True)
class XComEntryOptions:
    """Query options of ``get_xcom_entry``, declared after the scope so their spec order follows the scope's."""

    deserialize: bool
    stringify: bool


def _get_xcom_entry_options(
    deserialize: Annotated[bool, Query()] = False,
    stringify: Annotated[bool, Query()] = False,
) -> XComEntryOptions:
    return XComEntryOptions(deserialize=deserialize, stringify=stringify)


XComEntryOptionsDep = Annotated[XComEntryOptions, Depends(_get_xcom_entry_options)]


@xcom_router.get(
    "/{xcom_key:path}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.XCOM))],
)
def get_xcom_entry(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    xcom_key: str,
    session: SessionDep,
    scope: TaskScopeDep,
    options: XComEntryOptionsDep,
) -> XComResponseNative | XComResponseString:
    """Get an XCom entry."""
    deserialize = options.deserialize
    stringify = options.stringify
    xcom_read = XComModel.get_many(
        run_id=dag_run_id,
        key=xcom_key,
        task_ids=task_id,
        dag_ids=dag_id,
        map_indexes=scope.region_index,
        region_id=scope.region_id,
    )
    entity = xcom_entity(xcom_read)
    xcom_query = xcom_read.options(
        joinedload(entity.task),
        joinedload(entity.dag_run).joinedload(DR.dag_model),
        *eager_load_teams(entity.dag_run, DR.dag_model),
    )

    # We use `BaseXCom.get_many` to fetch XComs directly from the database, bypassing the XCom Backend.
    # This avoids deserialization via the backend (e.g., from a remote storage like S3) and instead
    # retrieves the raw serialized value from the database.
    raw_result = session.execute(xcom_query.add_columns(public_map_index_expression(entity)).limit(1)).first()

    if raw_result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"XCom entry with key: `{xcom_key}` not found")
    result, public_map_index = raw_result

    value = result.value

    if deserialize:
        # Custom XCom backends may store references (eg: object storage paths) in the database.
        # The custom XCom backend's deserialize_value() resolves these to actual values, but that is only
        # used on workers during task execution. The API reads directly from the database and uses
        # stringify() to convert DB values (references or serialized data) to human readable
        # format for UI display or for API users.
        import json

        from airflow.serialization.stringify import (
            StringifyNotSupportedError,
            stringify as stringify_xcom,
        )

        try:
            parsed_value = json.loads(result.value)
        except (ValueError, TypeError):
            # Already deserialized (e.g., set via Task Execution API)
            parsed_value = result.value

        try:
            value = stringify_xcom(parsed_value)
        except StringifyNotSupportedError:
            value = XComModel.deserialize_value(result)
    else:
        # For native format, return the raw serialized value from the database
        # This preserves the JSON string format that the API expects
        value = result.value

    data = XComResponseNative.model_validate(result).model_dump()
    data["value"] = value
    data["map_index"] = public_map_index
    if stringify:
        return XComResponseString.model_validate(data)
    return XComResponseNative.model_validate(data)


@xcom_router.get(
    "",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.XCOM))],
)
def get_xcom_entries(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    limit: QueryLimit,
    offset: QueryOffset,
    readable_xcom_filter: ReadableXComFilterDep,
    session: SessionDep,
    xcom_key_pattern: QueryXComKeyPatternSearch,
    xcom_key_prefix_pattern: QueryXComKeyPrefixPatternSearch,
    dag_display_name_pattern: QueryXComDagDisplayNamePatternSearch,
    dag_display_name_prefix_pattern: QueryXComDagDisplayNamePrefixPatternSearch,
    run_id_pattern: QueryXComRunIdPatternSearch,
    run_id_prefix_pattern: QueryXComRunIdPrefixPatternSearch,
    task_id_pattern: QueryXComTaskIdPatternSearch,
    task_id_prefix_pattern: QueryXComTaskIdPrefixPatternSearch,
    map_index_filter: Annotated[
        FilterParam[int | None],
        Depends(
            filter_param_factory(
                XComModel.map_index,  # xcom-model-column: allow
                int | None,
                filter_name="map_index_filter",
            )
        ),
    ],
    logical_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", DR))],
    run_after_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DR))],
    teams: Annotated[
        _DagIdTeamsFilter,
        Depends(teams_filter_factory(XComModel.dag_id)),  # xcom-model-column: allow
    ],
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                ["key", "dag_id", "run_id", "task_id", "map_index", "region_id", "region_index", "timestamp"],
                XComModel,
                to_replace={"run_after": DR.run_after},
            ).dynamic_depends(default=("dag_id", "task_id", "run_id", "region_id", "region_index", "key"))
        ),
    ],
    xcom_key: Annotated[str | None, Query()] = None,
    map_index: Annotated[int | None, Query(ge=-1)] = None,
    region_id: Annotated[UUID | None, Query()] = None,
    region_index: Annotated[int | None, Query(ge=-1)] = None,
) -> XComCollectionResponse:
    """
    Get all XCom entries.

    This endpoint allows specifying `~` as the dag_id, dag_run_id, task_id to retrieve XCom entries for all Dags.
    """
    if region_index is not None and region_id is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "region_index requires region_id")
    xcom_read = build_xcom_read_query(
        producer_ids=select_producers(
            dag_ids=None if dag_id == "~" else dag_id,
            run_id=None if dag_run_id == "~" else dag_run_id,
            task_ids=None if task_id == "~" else task_id,
            map_indexes=region_index,
            region_id=region_id,
        ),
        key=xcom_key,
    )
    query, entity = xcom_read, xcom_entity(xcom_read)
    public_map_index = public_map_index_expression(entity)
    readable_xcom_filter.entity = entity
    for parameter, name in (
        (xcom_key_pattern, "key"),
        (xcom_key_prefix_pattern, "key"),
        (run_id_pattern, "run_id"),
        (run_id_prefix_pattern, "run_id"),
        (task_id_pattern, "task_id"),
        (task_id_prefix_pattern, "task_id"),
    ):
        parameter.attribute = getattr(entity, name)
    map_index_filter.attribute = public_map_index  # type: ignore[assignment]
    teams.dag_id_attribute = entity.dag_id
    order_by.model = entity
    order_by.to_replace = {**(order_by.to_replace or {}), "map_index": public_map_index}
    if dag_id != "~":
        query = query.where(entity.dag_id == dag_id)
    query = (
        query.join(DR, and_(entity.dag_id == DR.dag_id, entity.run_id == DR.run_id))
        .join(DagModel, DR.dag_id == DagModel.dag_id)
        .options(
            joinedload(entity.task),
            joinedload(entity.dag_run).joinedload(DR.dag_model),
            *eager_load_teams(entity.dag_run, DR.dag_model),
        )
    )

    if task_id != "~":
        query = query.where(entity.task_id == task_id)
    if dag_run_id != "~":
        query = query.where(DR.run_id == dag_run_id)
    if map_index is not None:
        query = query.where(public_map_index == map_index)
    if xcom_key is not None:
        query = query.where(entity.key == xcom_key)

    query, total_entries = paginated_select(
        statement=query,
        filters=[
            readable_xcom_filter,
            xcom_key_pattern,
            xcom_key_prefix_pattern,
            dag_display_name_pattern,
            dag_display_name_prefix_pattern,
            run_id_pattern,
            run_id_prefix_pattern,
            task_id_pattern,
            task_id_prefix_pattern,
            map_index_filter,
            logical_date_range,
            run_after_range,
            teams,
        ],
        order_by=order_by,
        offset=offset,
        limit=limit,
        session=session,
    )
    entries = []
    for row, row_map_index in session.execute(query.add_columns(public_map_index)):
        response = XComResponse.model_validate(row)
        response.map_index = row_map_index
        entries.append(response)
    return XComCollectionResponse(xcom_entries=entries, total_entries=total_entries)


@xcom_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.XCOM)),
    ],
)
def create_xcom_entry(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    request_body: XComCreateBody,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> XComResponseNative:
    """Create an XCom entry."""
    from airflow.models.dagrun import DagRun

    dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == dag_run_id))
    # Validate Dag ID
    dag = get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)

    # Validate Task ID
    try:
        dag.get_task(task_id)
    except TaskNotFound:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Task with ID: `{task_id}` not found in dag: `{dag_id}`"
        )

    # Validate Dag Run ID
    if not dag_run:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Dag Run with ID: `{dag_run_id}` not found for dag: `{dag_id}`"
        )

    scope = resolve_task_scope(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        resolver=TaskCoordinateResolver(dag_bag, session),
        map_index=request_body.map_index,
        region_id=request_body.region_id,
        region_index=request_body.region_index,
    )

    # Check existing XCom
    xcom_read = XComModel.get_many(
        key=request_body.key,
        task_ids=task_id,
        dag_ids=dag_id,
        run_id=dag_run_id,
        map_indexes=scope.region_index,
        region_id=scope.region_id,
    )
    result = session.execute(xcom_read.with_only_columns(xcom_entity(xcom_read).value).limit(1)).first()
    if result:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"The XCom with key: `{request_body.key}` with mentioned task instance already exists.",
        )

    try:
        XComModel.set(
            key=request_body.key,
            value=request_body.value,
            dag_id=dag_id,
            task_id=task_id,
            run_id=dag_run_id,
            map_index=scope.region_index,
            region_id=scope.region_id,
            serialize=False,
            session=session,
        )
    except (ValueError, TypeError) as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"Couldn't serialise the XCom with key: `{request_body.key}`"
        ) from e

    entity = xcom_entity(xcom_read)
    xcom, public_map_index = session.execute(
        xcom_read.add_columns(public_map_index_expression(entity))
        .limit(1)
        .options(
            joinedload(entity.task),
            joinedload(entity.dag_run).joinedload(DR.dag_model),
            *eager_load_teams(entity.dag_run, DR.dag_model),
        )
    ).one()

    response = XComResponseNative.model_validate(xcom)
    response.map_index = public_map_index
    return response


@xcom_router.patch(
    "/{xcom_key:path}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.XCOM)),
    ],
)
def update_xcom_entry(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    xcom_key: str,
    patch_body: XComUpdateBody,
    *,
    session: SessionDep,
    dag_bag: DagBagDep,
) -> XComResponseNative:
    """Update an existing XCom entry."""
    scope = resolve_task_scope(
        dag_id=dag_id,
        run_id=dag_run_id,
        task_id=task_id,
        resolver=TaskCoordinateResolver(dag_bag, session),
        map_index=patch_body.map_index,
        region_id=patch_body.region_id,
        region_index=patch_body.region_index,
    )
    xcom_read = XComModel.get_many(
        dag_ids=dag_id,
        task_ids=task_id,
        run_id=dag_run_id,
        key=xcom_key,
        map_indexes=scope.region_index,
        region_id=scope.region_id,
    )
    entity = xcom_entity(xcom_read)
    xcom_query = xcom_read.options(
        joinedload(entity.task),
        joinedload(entity.dag_run).joinedload(DR.dag_model),
        *eager_load_teams(entity.dag_run, DR.dag_model),
    )
    xcom_entry = session.scalar(xcom_query)

    if not xcom_entry:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The XCom with key: `{xcom_key}` with mentioned task instance doesn't exist.",
        )

    try:
        XComModel.set_for_attempt(
            key=xcom_key,
            value=patch_body.value,
            task_instance_id=xcom_entry.task_instance_id,
            serialize=False,
            # Not recomputed from the new value: a custom XCom backend stores only a reference.
            mapped_length=xcom_entry.mapped_length,
            session=session,
        )
    except (ValueError, TypeError) as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"Couldn't serialise the XCom with key: `{xcom_key}`"
        ) from e

    # Fetch after setting, to get fresh object for response
    xcom_entry, public_map_index = session.execute(
        xcom_query.add_columns(public_map_index_expression(entity))
    ).one()
    response = XComResponseNative.model_validate(xcom_entry)
    response.map_index = public_map_index
    return response


@xcom_router.delete(
    "/{xcom_key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(action_logging()),
        Depends(requires_access_dag(method="DELETE", access_entity=DagAccessEntity.XCOM)),
    ],
)
def delete_xcom_entry(
    dag_id: str,
    task_id: str,
    dag_run_id: str,
    xcom_key: str,
    session: SessionDep,
    scope: TaskScopeDep,
):
    """Delete an XCom entry."""
    read = XComModel.get_many(
        dag_ids=dag_id,
        task_ids=task_id,
        run_id=dag_run_id,
        key=xcom_key,
        map_indexes=scope.region_index,
        region_id=scope.region_id,
    )
    owner = session.scalar(read.with_only_columns(xcom_entity(read).task_instance_id))
    if owner is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The XCom with key: `{xcom_key}` with mentioned task instance doesn't exist.",
        )
    XComModel.delete_for_attempts(
        producer_ids=select(TaskInstance.id).where(TaskInstance.id == owner),
        key=xcom_key,
        session=session,
    )
