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

from typing import Annotated

import structlog
from fastapi import Depends, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.parameters import (
    QueryHITLDetailBodySearch,
    QueryHITLDetailDagIdFilter,
    QueryHITLDetailDagIdPatternSearch,
    QueryHITLDetailDagRunIdFilter,
    QueryHITLDetailResponseReceivedFilter,
    QueryHITLDetailSubjectSearch,
    QueryHITLDetailTaskIdFilter,
    QueryHITLDetailTaskIdPatternSearch,
    QueryHITLDetailUserIdFilter,
    QueryLimit,
    QueryOffset,
    QueryTIStateFilter,
    SortParam,
)
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetail,
    HITLDetailCollection,
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, ReadableTIFilterDep, requires_access_dag
from airflow.api_fastapi.core_api.services.public.hitl import (
    get_hitl_detail_from_ti_keys,
    update_hitl_detail_through_payload,
)
from airflow.models.hitl import HITLDetail as HITLDetailModel
from airflow.models.taskinstance import TaskInstance as TI

hitl_router = AirflowRouter(tags=["HumanInTheLoop"], prefix="/hitlDetails")

log = structlog.get_logger(__name__)


@hitl_router.patch(
    "/{dag_id}/{dag_run_id}/{task_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def update_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
) -> HITLDetailResponse:
    """Update a Human-in-the-loop detail."""
    return update_hitl_detail_through_payload(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        map_index=None,
    )


@hitl_router.patch(
    "/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="PUT", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def update_mapped_ti_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
    map_index: int,
) -> HITLDetailResponse:
    """Update a Human-in-the-loop detail."""
    return update_hitl_detail_through_payload(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        map_index=map_index,
    )


@hitl_router.get(
    "/{dag_id}/{dag_run_id}/{task_id}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    return get_hitl_detail_from_ti_keys(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=None,
    )


@hitl_router.get(
    "/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def get_mapped_ti_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    return get_hitl_detail_from_ti_keys(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )


@hitl_router.get(
    "/",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.HITL_DETAIL))],
)
def get_hitl_details(
    limit: QueryLimit,
    offset: QueryOffset,
    order_by: Annotated[
        SortParam,
        Depends(
            SortParam(
                [
                    "ti_id",
                    "subject",
                    "response_at",
                    "task_instance.dag_id",
                    "task_instance.run_id",
                ],
                HITLDetailModel,
                to_replace={
                    "dag_id": TI.dag_id,
                    "run_id": TI.run_id,
                },
            ).dynamic_depends(),
        ),
    ],
    session: SessionDep,
    # ti related filter
    readable_ti_filter: ReadableTIFilterDep,
    dag_id: QueryHITLDetailDagIdFilter,
    dag_id_pattern: QueryHITLDetailDagIdPatternSearch,
    dag_run_id: QueryHITLDetailDagRunIdFilter,
    task_id: QueryHITLDetailTaskIdFilter,
    task_id_pattern: QueryHITLDetailTaskIdPatternSearch,
    ti_state: QueryTIStateFilter,
    # hitl detail related filter
    response_received: QueryHITLDetailResponseReceivedFilter,
    user_id: QueryHITLDetailUserIdFilter,
    subject_patten: QueryHITLDetailSubjectSearch,
    body_patten: QueryHITLDetailBodySearch,
) -> HITLDetailCollection:
    """Get Human-in-the-loop details."""
    query = (
        select(HITLDetailModel)
        .join(TI, HITLDetailModel.ti_id == TI.id)
        .options(joinedload(HITLDetailModel.task_instance))
    )
    hitl_detail_select, total_entries = paginated_select(
        statement=query,
        filters=[
            # ti related filter
            readable_ti_filter,
            dag_id,
            dag_id_pattern,
            dag_run_id,
            task_id,
            task_id_pattern,
            ti_state,
            # hitl detail related filter
            response_received,
            user_id,
            subject_patten,
            body_patten,
        ],
        offset=offset,
        limit=limit,
        order_by=order_by,
        session=session,
    )

    hitl_details = session.scalars(hitl_detail_select)

    return HITLDetailCollection(
        hitl_details=hitl_details,
        total_entries=total_entries,
    )
