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

import copy
from typing import Annotated

from fastapi import Depends, HTTPException, Query, status
from sqlalchemy import and_, select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep, get_dag_for_run_or_latest_version
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    FilterParam,
    QueryLimit,
    QueryOffset,
    QueryXComDagDisplayNamePatternSearch,
    QueryXComKeyPatternSearch,
    QueryXComRunIdPatternSearch,
    QueryXComTaskIdPatternSearch,
    RangeFilter,
    datetime_range_filter_factory,
    filter_param_factory,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.xcom import (
    XComCollectionResponse,
    XComCreateBody,
    XComResponseNative,
    XComResponseString,
    XComUpdateBody,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import ReadableXComFilterDep, requires_access_dag
from airflow.api_fastapi.logging.decorators import action_logging
from airflow.exceptions import TaskNotFound
from airflow.models import DagRun as DR
from airflow.models.dag import DagModel
from airflow.models.xcom import XComModel

xcom_router = AirflowRouter(
    tags=["XCom"], prefix="/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"
)


@xcom_router.get(
    "/{xcom_key}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
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
    map_index: Annotated[int, Query(ge=-1)] = -1,
    deserialize: Annotated[bool, Query()] = False,
    stringify: Annotated[bool, Query()] = False,
) -> XComResponseNative | XComResponseString:
    """Get an XCom entry."""
    xcom_query = XComModel.get_many(
        run_id=dag_run_id,
        key=xcom_key,
        task_ids=task_id,
        dag_ids=dag_id,
        map_indexes=map_index,
        limit=1,
    ).options(joinedload(XComModel.task), joinedload(XComModel.dag_run).joinedload(DR.dag_model))

    # We use `BaseXCom.get_many` to fetch XComs directly from the database, bypassing the XCom Backend.
    # This avoids deserialization via the backend (e.g., from a remote storage like S3) and instead
    # retrieves the raw serialized value from the database.
    result = session.scalars(xcom_query).first()

    if result is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"XCom entry with key: `{xcom_key}` not found")

    item = copy.copy(result)

    if deserialize:
        # We use `airflow.serialization.serde` for deserialization here because custom XCom backends (with their own
        # serializers/deserializers) are only used on the worker side during task execution.

        # However, the XCom value is *always* stored in the metadata database as a valid JSON object.
        # Therefore, for purposes such as UI display or returning API responses, deserializing with
        # `airflow.serialization.serde` is safe and recommended.
        from airflow.serialization.serde import deserialize as serde_deserialize

        # full=False ensures that the `item` is deserialized without loading the classes, and it returns a stringified version
        item.value = serde_deserialize(XComModel.deserialize_value(item), full=False)
    else:
        # For native format, return the raw serialized value from the database
        # This preserves the JSON string format that the API expects
        item.value = result.value

    if stringify:
        return XComResponseString.model_validate(item)
    return XComResponseNative.model_validate(item)


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
    dag_display_name_pattern: QueryXComDagDisplayNamePatternSearch,
    run_id_pattern: QueryXComRunIdPatternSearch,
    task_id_pattern: QueryXComTaskIdPatternSearch,
    map_index_filter: Annotated[
        FilterParam[int | None],
        Depends(filter_param_factory(XComModel.map_index, int | None, filter_name="map_index_filter")),
    ],
    logical_date_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("logical_date", DR))],
    run_after_range: Annotated[RangeFilter, Depends(datetime_range_filter_factory("run_after", DR))],
    xcom_key: Annotated[str | None, Query()] = None,
    map_index: Annotated[int | None, Query(ge=-1)] = None,
) -> XComCollectionResponse:
    """
    Get all XCom entries.

    This endpoint allows specifying `~` as the dag_id, dag_run_id, task_id to retrieve XCom entries for all DAGs.
    """
    query = select(XComModel)
    if dag_id != "~":
        query = query.where(XComModel.dag_id == dag_id)
    query = (
        query.join(DR, and_(XComModel.dag_id == DR.dag_id, XComModel.run_id == DR.run_id))
        .join(DagModel, DR.dag_id == DagModel.dag_id)
        .options(joinedload(XComModel.task), joinedload(XComModel.dag_run).joinedload(DR.dag_model))
    )

    if task_id != "~":
        query = query.where(XComModel.task_id == task_id)
    if dag_run_id != "~":
        query = query.where(DR.run_id == dag_run_id)
    if map_index is not None:
        query = query.where(XComModel.map_index == map_index)
    if xcom_key is not None:
        query = query.where(XComModel.key == xcom_key)

    query, total_entries = paginated_select(
        statement=query,
        filters=[
            readable_xcom_filter,
            xcom_key_pattern,
            dag_display_name_pattern,
            run_id_pattern,
            task_id_pattern,
            map_index_filter,
            logical_date_range,
            run_after_range,
        ],
        offset=offset,
        limit=limit,
        session=session,
    )
    query = query.order_by(
        XComModel.dag_id, XComModel.task_id, XComModel.run_id, XComModel.map_index, XComModel.key
    )
    xcoms = session.scalars(query)
    return XComCollectionResponse(xcom_entries=xcoms, total_entries=total_entries)


@xcom_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
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
    # Validate DAG ID
    dag = get_dag_for_run_or_latest_version(dag_bag, dag_run, dag_id, session)

    # Validate Task ID
    try:
        dag.get_task(task_id)
    except TaskNotFound:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Task with ID: `{task_id}` not found in dag: `{dag_id}`"
        )

    # Validate DAG Run ID
    if not dag_run:
        if not dag_run:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, f"Dag Run with ID: `{dag_run_id}` not found for dag: `{dag_id}`"
            )

    # Check existing XCom
    already_existing_query = XComModel.get_many(
        key=request_body.key,
        task_ids=task_id,
        dag_ids=dag_id,
        run_id=dag_run_id,
        map_indexes=request_body.map_index,
    )
    result = session.execute(already_existing_query.with_only_columns(XComModel.value)).first()
    if result:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"The XCom with key: `{request_body.key}` with mentioned task instance already exists.",
        )

    try:
        value = XComModel.serialize_value(request_body.value)
    except (ValueError, TypeError):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"Couldn't serialise the XCom with key: `{request_body.key}`"
        )

    new = XComModel(
        dag_run_id=dag_run.id,
        key=request_body.key,
        value=value,
        run_id=dag_run_id,
        task_id=task_id,
        dag_id=dag_id,
        map_index=request_body.map_index,
    )
    session.add(new)
    session.flush()

    xcom = session.scalar(
        select(XComModel)
        .filter(
            XComModel.dag_id == dag_id,
            XComModel.task_id == task_id,
            XComModel.run_id == dag_run_id,
            XComModel.key == request_body.key,
            XComModel.map_index == request_body.map_index,
        )
        .limit(1)
        .options(joinedload(XComModel.task), joinedload(XComModel.dag_run).joinedload(DR.dag_model))
    )

    return XComResponseNative.model_validate(xcom)


@xcom_router.patch(
    "/{xcom_key}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_404_NOT_FOUND,
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
    session: SessionDep,
) -> XComResponseNative:
    """Update an existing XCom entry."""
    # Check if XCom entry exists
    xcom_new_value = XComModel.serialize_value(patch_body.value)
    xcom_entry = session.scalar(
        select(XComModel)
        .where(
            XComModel.dag_id == dag_id,
            XComModel.task_id == task_id,
            XComModel.run_id == dag_run_id,
            XComModel.key == xcom_key,
            XComModel.map_index == patch_body.map_index,
        )
        .limit(1)
        .options(joinedload(XComModel.task), joinedload(XComModel.dag_run).joinedload(DR.dag_model))
    )

    if not xcom_entry:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The XCom with key: `{xcom_key}` with mentioned task instance doesn't exist.",
        )

    # Update XCom entry
    xcom_entry.value = XComModel.serialize_value(xcom_new_value)

    return XComResponseNative.model_validate(xcom_entry)
