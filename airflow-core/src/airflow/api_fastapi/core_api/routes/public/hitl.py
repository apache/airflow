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

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLResponseContentDetail,
    HITLResponseDetail,
    HITLResponseDetailCollection,
    UpdateHITLResponsePayload,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, ReadableTIFilterDep, requires_access_dag
from airflow.models.hitl import HITLResponseModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils import timezone

hitl_router = AirflowRouter(tags=["HumanInTheLoop"], prefix="/hitl-responses")

log = structlog.get_logger(__name__)


@hitl_router.patch(
    "/{task_instance_id}",
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
def update_hitl_response(
    task_instance_id: UUID,
    update_hitl_response_payload: UpdateHITLResponsePayload,
    user: GetUserDep,
    session: SessionDep,
) -> HITLResponseContentDetail:
    """Update a Human-in-the-loop response."""
    ti_id_str = str(task_instance_id)
    hitl_response_model = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    )
    if not hitl_response_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop Response does not exist for Task Instance with id {ti_id_str}",
        )

    if hitl_response_model.response_received:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop Response has already been updated for Task Instance with id {ti_id_str} "
            "and is not allowed to write again.",
        )

    hitl_response_model.user_id = user.get_id()
    hitl_response_model.response_at = timezone.utcnow()
    hitl_response_model.response_content = update_hitl_response_payload.response_content
    hitl_response_model.params_input = update_hitl_response_payload.params_input
    session.add(hitl_response_model)
    session.commit()
    return HITLResponseContentDetail.model_validate(hitl_response_model)


@hitl_router.get(
    "/{task_instance_id}",
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
def get_hitl_response(
    task_instance_id: UUID,
    session: SessionDep,
) -> HITLResponseDetail:
    """Get a Human-in-the-loop Response of a specific task instance."""
    ti_id_str = str(task_instance_id)
    hitl_response_model = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    )
    if not hitl_response_model:
        log.error("Human-in-the-loop response not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Human-in-the-loop response not found",
            },
        )
    return HITLResponseDetail.model_validate(hitl_response_model)


@hitl_router.get(
    "/",
    status_code=status.HTTP_200_OK,
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def get_hitl_responses(
    readable_ti_filter: ReadableTIFilterDep,
    session: SessionDep,
) -> HITLResponseDetailCollection:
    """Get Human-in-the-loop Responses."""
    query = select(HITLResponseModel).join(
        TI,
        HITLResponseModel.ti_id == TI.id,
    )
    hitl_response_select, total_entries = paginated_select(
        statement=query,
        filters=[readable_ti_filter],
        session=session,
    )
    hitl_responses = session.scalars(hitl_response_select)
    return HITLResponseDetailCollection(
        hitl_responses=hitl_responses,
        total_entries=total_entries,
    )
