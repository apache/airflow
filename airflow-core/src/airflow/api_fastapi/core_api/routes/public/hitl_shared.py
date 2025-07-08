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
from fastapi import HTTPException, Query, Response, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLResponseContentDetail,
    HITLResponseDetail,
    HITLSharedLinkActionRequest,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.configuration import conf
from airflow.models.hitl import HITLResponseModel
from airflow.utils import timezone
from airflow.utils.hitl_shared_links import hitl_shared_link_manager

hitl_shared_router = AirflowRouter(tags=["HumanInTheLoop"], prefix="/hitl/shared")

log = structlog.get_logger(__name__)


@hitl_shared_router.get(
    "/{task_instance_id}/redirect",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
        ]
    ),
)
def redirect_to_hitl_ui(
    task_instance_id: UUID,
    session: SessionDep,
    payload: str = Query(..., description="Base64 encoded payload"),
    signature: str = Query(..., description="Base64 encoded signature"),
) -> Response:
    """Redirect to Airflow UI page for HITL task interaction."""
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

    from airflow.models.taskinstance import TaskInstance as TI

    hitl_response_model = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    )
    if not hitl_response_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop Response does not exist for Task Instance with id {ti_id_str}",
        )

    task_instance = session.scalar(select(TI).where(TI.id == ti_id_str))
    if not task_instance:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Task Instance with id {ti_id_str} not found",
        )

    base_url = conf.get("webserver", "base_url", fallback="http://localhost:8080")

    redirect_url = f"{base_url}/task?dag_id={task_instance.dag_id}&task_id={task_instance.task_id}&run_id={task_instance.run_id}&map_index={task_instance.map_index}&hitl_mode=true&shared_link_payload={payload}&shared_link_signature={signature}"

    return Response(status_code=status.HTTP_302_FOUND, headers={"Location": redirect_url})


@hitl_shared_router.get(
    "/{task_instance_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
        ]
    ),
)
def get_hitl_shared_response(
    task_instance_id: UUID,
    session: SessionDep,
    payload: str = Query(..., description="Base64 encoded payload"),
    signature: str = Query(..., description="Base64 encoded signature"),
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


@hitl_shared_router.post(
    "/{task_instance_id}/action",
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
    action_request: HITLSharedLinkActionRequest,
    session: SessionDep,
    payload: str = Query(..., description="Base64 encoded payload"),
    signature: str = Query(..., description="Base64 encoded signature"),
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
