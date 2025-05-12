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

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.dagbag import DagBagDep
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import QueryLimit, QueryOffset
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
from airflow.models import DAG, DagRun as DR
from airflow.models.xcom import XComModel
from airflow.settings import conf

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
    if deserialize:
        if not conf.getboolean("api", "enable_xcom_deserialize_support", fallback=False):
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST, "XCom deserialization is disabled in configuration."
            )
        query = select(XComModel, XComModel.value)
    else:
        query = select(XComModel)

    query = query.where(
        XComModel.dag_id == dag_id,
        XComModel.task_id == task_id,
        XComModel.key == xcom_key,
        XComModel.map_index == map_index,
    )
    query = query.join(DR, and_(XComModel.dag_id == DR.dag_id, XComModel.run_id == DR.run_id))
    query = query.where(DR.run_id == dag_run_id)

    if deserialize:
        item = session.execute(query).one_or_none()
    else:
        item = session.scalars(query).one_or_none()

    if item is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"XCom entry with key: `{xcom_key}` not found")

    if deserialize:
        from airflow.sdk.execution_time.xcom import XCom

        xcom, value = item
        xcom_stub = copy.copy(xcom)
        xcom_stub.value = value
        xcom_stub.value = XCom.deserialize_value(xcom_stub)
        item = xcom_stub

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
    query = query.join(DR, and_(XComModel.dag_id == DR.dag_id, XComModel.run_id == DR.run_id))

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
        filters=[readable_xcom_filter],
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
    # Validate DAG ID
    dag: DAG = dag_bag.get_dag(dag_id)
    if not dag:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Dag with ID: `{dag_id}` was not found")

    # Validate Task ID
    try:
        dag.get_task(task_id)
    except TaskNotFound:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"Task with ID: `{task_id}` not found in DAG: `{dag_id}`"
        )

    # Validate DAG Run ID
    dag_run = dag.get_dagrun(dag_run_id, session)
    if not dag_run:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, f"DAG Run with ID: `{dag_run_id}` not found for DAG: `{dag_id}`"
        )

    # Check existing XCom
    already_existing_query = XComModel.get_many(
        key=request_body.key,
        task_ids=task_id,
        dag_ids=dag_id,
        run_id=dag_run_id,
        map_indexes=request_body.map_index,
        session=session,
    )
    result = already_existing_query.with_entities(XComModel.value).first()
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
    )

    if not xcom_entry:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The XCom with key: `{xcom_key}` with mentioned task instance doesn't exist.",
        )

    # Update XCom entry
    xcom_entry.value = XComModel.serialize_value(xcom_new_value)

    return XComResponseNative.model_validate(xcom_entry)
