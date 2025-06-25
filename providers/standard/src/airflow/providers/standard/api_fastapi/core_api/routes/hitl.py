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

from uuid import UUID

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select
from structlog.contextvars import bind_contextvars

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.api_fastapi.core_api.datamodels.hitl import (
    AddHITLResponsePayload,
    HITLInputRequest,
    HITLInputRequestCollection,
    HITLResponse,
)
from airflow.providers.standard.models import HITLInputRequestModel, HITLResponseModel

hitl_router = AirflowRouter(tags=["HumanInTheLoop"])

log = structlog.get_logger(__name__)


@hitl_router.post(
    "/{task_instance_id}/response",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def write_hitl_response_by_ti_id(
    task_instance_id: UUID,
    add_response_payload: AddHITLResponsePayload,
    user: GetUserDep,
    session: SessionDep,
) -> HITLResponse:
    """Write an HITLResponse."""
    ti_id_str = str(task_instance_id)
    input_request_id = session.scalar(
        select(HITLInputRequestModel.id).where(HITLInputRequestModel.ti_id == ti_id_str)
    )
    if not input_request_id:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop Input Request does not exist for Task Instance with id {ti_id_str}",
        )

    existing_response = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.input_request_id == input_request_id)
    )
    if existing_response:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop Response exists for Task Instance with id {ti_id_str}",
        )

    hitl_response_model = HITLResponseModel(
        input_request_id=input_request_id,
        content=add_response_payload.content,
        user_id=user.get_id(),
    )
    session.add(hitl_response_model)
    session.commit()
    return HITLResponse(
        ti_id=ti_id_str,
        content=hitl_response_model.content,
        created_at=hitl_response_model.created_at,
    )


@hitl_router.get(
    "/{task_instance_id}/input-requests",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def get_hitl_input_request_by_ti_id(
    task_instance_id: UUID,
    session: SessionDep,
) -> HITLInputRequest:
    """Get a Human-in-the-loop input request of a specific task instance."""
    ti_id_str = str(task_instance_id)
    bind_contextvars(ti_id=ti_id_str)
    ti = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not ti:
        log.error("Task Instance not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    input_request_model = session.scalar(
        select(HITLInputRequestModel).where(HITLInputRequestModel.ti_id == ti_id_str)
    )
    if not input_request_model:
        log.error("HITL input request not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "HITL input request not found",
            },
        )
    return HITLInputRequest.model_validate(input_request_model)


@hitl_router.get(
    "/input-requests",
    status_code=status.HTTP_200_OK,
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def get_hitl_input_requests(
    session: SessionDep,
    # readable_ti_filter: ReadableTIFilterDep,
) -> HITLInputRequestCollection:
    """Get a Human-in-the-loop input request of a specific task instance."""
    query = select(HITLInputRequestModel)
    # input_reuqest_select, total_entries = paginated_select(
    #     statement=query,
    #     # filters=[readable_ti_filter],
    #     filters=[],
    #     session=session,
    # )
    input_requests = session.scalars(query).all()
    return HITLInputRequestCollection(
        hitl_input_requests=input_requests,
        total_entries=len(input_requests),
    )
