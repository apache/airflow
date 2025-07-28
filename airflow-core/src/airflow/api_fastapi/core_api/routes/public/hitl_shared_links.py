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
"""API routes for HITL shared links functionality."""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import Depends, Request, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.hitl import HITLDetailResponse
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
from airflow.api_fastapi.core_api.services.public.hitl_shared_links import (
    service_execute_shared_link_action,
    service_generate_shared_link,
    service_redirect_shared_link,
)
from airflow.exceptions import HITLSharedLinkTimeout

hitl_shared_links_router = AirflowRouter(tags=["HITLSharedLinks"], prefix="/hitl-shared-links")

log = structlog.get_logger(__name__)


class GenerateSharedLinkRequest(BaseModel):
    """Request model for generating HITL shared links."""

    link_type: str = Field(
        default="direct_action",
        description="Type of link to generate: 'ui_redirect' for UI interaction or 'direct_action' for direct execution",
    )
    action: str | None = Field(
        default=None,
        description="Optional action to perform when link is accessed (e.g., 'approve', 'reject'). Required for direct_action links.",
    )
    chosen_options: list[str] | None = Field(
        default=None,
        description="Chosen options for direct_action links",
    )
    params_input: dict[str, Any] | None = Field(
        default=None,
        description="Parameters input for direct_action links",
    )
    expiration_hours: int | None = Field(
        default=None,
        description="Custom expiration time in hours",
    )


class GenerateSharedLinkResponse(BaseModel):
    """Response model for generated HITL shared links."""

    url: str
    expires_at: str
    link_type: str
    action: str | None
    dag_id: str
    dag_run_id: str
    task_id: str
    try_number: int
    map_index: int | None
    task_instance_uuid: str


@hitl_shared_links_router.post(
    "/generate/{dag_id}/{dag_run_id}/{task_id}",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def generate_shared_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
    request: GenerateSharedLinkRequest,
    user: GetUserDep,
    session: SessionDep,
    http_request: Request,
    map_index: int | None = None,
) -> GenerateSharedLinkResponse:
    """
    Generate a shared link for HITL tasks.

    This endpoint generates a secure, time-limited shared link that allows external users
    to interact with HITL tasks without requiring full Airflow authentication. The link
    can be configured for either direct action execution or UI redirection.

    :param dag_id: The DAG identifier
    :param dag_run_id: The DAG run identifier
    :param task_id: The task identifier
    :param try_number: The try number for the task
    :param request: Request containing link configuration
    :param user: The authenticated user creating the shared link
    :param session: Database session for data persistence
    :param http_request: HTTP request for base URL extraction
    :param map_index: The map index for mapped tasks

    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance does not exist
    :raises HTTPException: 400 if link generation fails due to invalid parameters

    :return: GenerateSharedLinkResponse containing the generated link URL and metadata
    """
    base_url = f"{http_request.base_url.scheme}://{http_request.base_url.netloc}"

    link_data = service_generate_shared_link(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        link_type=request.link_type,
        action=request.action,
        chosen_options=request.chosen_options,
        params_input=request.params_input,
        map_index=map_index,
        expiration_hours=request.expiration_hours,
        base_url=base_url,
        session=session,
    )

    return GenerateSharedLinkResponse(**link_data)


@hitl_shared_links_router.post(
    "/generate/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[
        Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE)),
    ],
)
def generate_mapped_ti_shared_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    try_number: int,
    request: GenerateSharedLinkRequest,
    user: GetUserDep,
    session: SessionDep,
    http_request: Request,
) -> GenerateSharedLinkResponse:
    """
    Generate a shared link for mapped HITL tasks.

    This endpoint generates a secure, time-limited shared link for mapped task instances,
    allowing external users to interact with specific mapped HITL tasks without requiring
    full Airflow authentication. The link can be configured for either direct action
    execution or UI redirection.

    :param dag_id: The DAG identifier
    :param dag_run_id: The DAG run identifier
    :param task_id: The task identifier
    :param map_index: The map index for the mapped task instance
    :param try_number: The try number for the task
    :param request: Request containing link configuration
    :param user: The authenticated user creating the shared link
    :param session: Database session for data persistence
    :param http_request: HTTP request for base URL extraction

    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance does not exist
    :raises HTTPException: 400 if link generation fails due to invalid parameters

    :return: GenerateSharedLinkResponse containing the generated link URL and metadata
    """
    base_url = f"{http_request.base_url.scheme}://{http_request.base_url.netloc}"

    link_data = service_generate_shared_link(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        link_type=request.link_type,
        action=request.action,
        chosen_options=request.chosen_options,
        params_input=request.params_input,
        map_index=map_index,
        expiration_hours=request.expiration_hours,
        base_url=base_url,
        session=session,
    )

    return GenerateSharedLinkResponse(**link_data)


@hitl_shared_links_router.get(
    "/redirect",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def redirect_shared_link(
    token: str,
    http_request: Request,
) -> RedirectResponse:
    """
    Redirect to Airflow UI for HITL task interaction via shared link.

    This endpoint redirects external users to the Airflow UI where they can interact
    with HITL tasks through a secure shared link. The link must be a ui_redirect-type
    link, which provides access to the full Airflow interface for decision-making.

    :param token: Base64-encoded token containing link metadata and expiration
    :param http_request: HTTP request for base URL extraction

    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance does not exist
    :raises HTTPException: 400 if token is invalid or link has expired

    :return: RedirectResponse to Airflow UI
    """
    base_url = f"{http_request.base_url.scheme}://{http_request.base_url.netloc}"
    redirect_url = service_redirect_shared_link(token, base_url)
    return RedirectResponse(url=redirect_url)


@hitl_shared_links_router.get(
    "/execute",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def execute_shared_link_action(
    token: str,
    session: SessionDep,
) -> HITLDetailResponse:
    """
    Execute an action via shared link by making PATCH request to existing HITL endpoint.

    This endpoint allows external users to execute HITL task actions through a secure
    shared link. The link must be a direct_action-type link, which enables direct execution
    of predefined actions (e.g., approve, reject) by making a PATCH request to the existing
    HITL endpoint without requiring full Airflow authentication.

    :param token: Base64-encoded token containing link metadata and action data
    :param session: Database session for data persistence

    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance or HITL detail does not exist
    :raises HTTPException: 400 if token is invalid or link has expired

    :return: HITLDetailResponse containing the execution result
    """
    return service_execute_shared_link_action(token, session)
