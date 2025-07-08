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
    HITLSharedLinkActionRequest,
    HITLSharedLinkRequest,
    HITLSharedLinkResponse,
    UpdateHITLResponsePayload,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, ReadableTIFilterDep, requires_access_dag
from airflow.models.hitl import HITLResponseModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.hitl_shared_links import hitl_shared_link_manager

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
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
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


@hitl_router.post(
    "/{task_instance_id}/shared-link",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def create_hitl_shared_link(
    task_instance_id: UUID,
    shared_link_request: HITLSharedLinkRequest,
    user: GetUserDep,
    session: SessionDep,
) -> HITLSharedLinkResponse:
    """Create a shared link for a HITL task instance."""
    ti_id_str = str(task_instance_id)

    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    hitl_response_model = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    )
    if not hitl_response_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop Response does not exist for Task Instance with id {ti_id_str}",
        )

    try:
        if shared_link_request.link_type == "redirect":
            link_url, expires_at = hitl_shared_link_manager.generate_redirect_link(
                task_instance_id=ti_id_str,
                expires_in_hours=shared_link_request.expires_in_hours,
            )
        else:
            if shared_link_request.action is None:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    "Action must be provided for action links.",
                )
            link_url, expires_at = hitl_shared_link_manager.generate_action_link(
                task_instance_id=ti_id_str,
                action=shared_link_request.action,
                expires_in_hours=shared_link_request.expires_in_hours,
            )
    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )

    return HITLSharedLinkResponse(
        task_instance_id=ti_id_str,
        link_url=link_url,
        expires_at=expires_at,
        action=shared_link_request.action,
        link_type=shared_link_request.link_type,
    )


@hitl_router.get(
    "/shared/{task_instance_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
        ]
    ),
)
def get_hitl_shared_response(
    task_instance_id: UUID,
    payload: str,
    signature: str,
    session: SessionDep,
) -> HITLResponseDetail:
    """Get HITL response details via shared link."""
    ti_id_str = str(task_instance_id)

    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)
        if link_data["ti_id"] != ti_id_str:
            raise ValueError("Task instance ID mismatch")
    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Invalid shared link: {e}",
        )

    hitl_response_model = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    )
    if not hitl_response_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop Response does not exist for Task Instance with id {ti_id_str}",
        )

    return HITLResponseDetail.model_validate(hitl_response_model)


@hitl_router.post(
    "/shared/{task_instance_id}/action",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_409_CONFLICT,
        ]
    ),
)
def perform_hitl_shared_action(
    task_instance_id: UUID,
    payload: str,
    signature: str,
    action_request: HITLSharedLinkActionRequest,
    session: SessionDep,
) -> HITLResponseContentDetail:
    """Perform an action on a HITL task instance via shared link."""
    ti_id_str = str(task_instance_id)

    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)
        if link_data["ti_id"] != ti_id_str:
            raise ValueError("Task instance ID mismatch")
    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"Invalid shared link: {e}",
        )

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

    hitl_response_model.user_id = "Shared Link User"
    hitl_response_model.response_at = timezone.utcnow()
    hitl_response_model.response_content = action_request.response_content
    hitl_response_model.params_input = action_request.params_input
    session.add(hitl_response_model)
    session.commit()

    return HITLResponseContentDetail.model_validate(hitl_response_model)
