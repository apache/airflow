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

from json import JSONDecodeError

import structlog
from cryptography.exceptions import InvalidTag
from fastapi import Depends, HTTPException, Request, status
from fastapi.responses import RedirectResponse

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.datamodels.hitl import HITLDetailResponse
from airflow.api_fastapi.core_api.datamodels.hitl_shared_links import (
    GenerateHITLSharedLinkRequest,
    HITLSharedLinkResponse,
)
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep, requires_access_dag
from airflow.api_fastapi.core_api.services.public.hitl_shared_links import (
    generate_hitl_shared_link_from_request,
    hitl_shared_link_enabled,
    retrieve_redirect_url_from_token,
    update_hitl_detail_through_token,
)

hitl_shared_links_router = AirflowRouter(tags=["HumanInTheLoopSharedLinks"], prefix="/hitlSharedLinks")

log = structlog.get_logger(__name__)


@hitl_shared_link_enabled
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
        Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.HITL_DETAIL)),
    ],
)
def generate_shared_link(
    # task instance identifier
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    # payload
    generate_shared_link_request: GenerateHITLSharedLinkRequest,
    # utitliy
    request: Request,
    session: SessionDep,
) -> HITLSharedLinkResponse:
    """
    Generate a shared link for a Human-in-the-look task instance.

    This endpoint generates a secure, time-limited shared link that allows users
    to interact with the Human-in-the-look task instance. The link
    can be configured for either respond directly or UI redirection.

    :param dag_id: The dag identifier
    :param dag_run_id: The dag run identifier
    :param task_id: The task identifier
    :param generate_shared_link_request: Request containing link configuration
    :param http_request: HTTP request for base URL extraction
    :param session: Database session for data persistence

    :raises HTTPException: 400 if link generation fails due to invalid parameters
    :raises HTTPException: 403 if Human-in-the-look link sharing feature is not enabled
    :raises HTTPException: 404 if the task instance does not exist

    :return: SharedLinkResponse containing the generated link URL and metadata
    """
    return generate_hitl_shared_link_from_request(
        # task instance identifier
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=None,
        # payload
        generate_shared_link_request=generate_shared_link_request,
        # utitliy
        request=request,
        session=session,
    )


@hitl_shared_link_enabled
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
        Depends(requires_access_dag(method="POST", access_entity=DagAccessEntity.HITL_DETAIL)),
    ],
)
def generate_mapped_ti_shared_link(
    # task instance identifier
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    # payload
    generate_shared_link_request: GenerateHITLSharedLinkRequest,
    # utitliy
    request: Request,
    session: SessionDep,
) -> HITLSharedLinkResponse:
    """
    Generate a shared link for a mapped Human-in-the-look task instance.

    This endpoint generates a secure, time-limited shared link that allows users
    to interact with the mapped Human-in-the-look task instance. The link
    can be configured for either respond directly or UI redirection.

    :param dag_id: The dag identifier
    :param dag_run_id: The dag run identifier
    :param task_id: The task identifier
    :param generate_shared_link_request: Request containing link configuration
    :param request: HTTP request for base URL extraction
    :param session: Database session for data persistence

    :raises HTTPException: 400 if link generation fails due to invalid parameters
    :raises HTTPException: 403 if Human-in-the-look link sharing feature is not enabled
    :raises HTTPException: 404 if the task instance does not exist

    :return: SharedLinkResponse containing the generated link URL and metadata
    """
    return generate_hitl_shared_link_from_request(
        # task instance identifier
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        # payload
        generate_shared_link_request=generate_shared_link_request,
        # utitliy
        request=request,
        session=session,
    )


@hitl_shared_link_enabled
@hitl_shared_links_router.get(
    "/redirect/{token}",
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
    request: Request,
    session: SessionDep,
) -> RedirectResponse:
    """
    Redirect to Airflow UI for Human-in-the-look task instance interaction via shared link.

    This endpoint redirects users to the Airflow UI where they can interact
    with Human-in-the-look task instances through a secure shared link. The link must be a "redirect" type
    link, which provides access to the Airflow interface for decision-making.

    :param token: Base64-encoded token containing link metadata and expiration
    :param http_request: HTTP request for base URL extraction

    :raises HTTPException: 400 if token is invalid or link has expired
    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance does not exist

    :return: RedirectResponse to Airflow UI
    """
    try:
        redirect_url = retrieve_redirect_url_from_token(
            token=token,
            secret_key=request.app.state.secret_key,
            base_url=str(request.base_url),
            session=session,
        )
    except (ValueError, JSONDecodeError, KeyError, InvalidTag) as err:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(err),
        )
    return RedirectResponse(url=redirect_url)


@hitl_shared_link_enabled
@hitl_shared_links_router.post(
    "/respond/{token}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
)
def update_hitl_detail_through_shared_link(
    token: str,
    user: GetUserDep,
    request: Request,
    session: SessionDep,
) -> HITLDetailResponse:
    """
    Respond to a Human-in-the-look task instance with predefined content.

    This endpoint allows users to respond Human-in-the-look task instances with predefined content
    through a secure shared link. The link must be a "direct_action" type link.

    :param token: Base64-encoded token containing link metadata and action data
    :param session: Database session for data persistence

    :raises HTTPException: 400 if token is invalid or link has expired
    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance or HITL detail does not exist

    :return: HITLDetailResponse containing the execution result
    """
    try:
        return update_hitl_detail_through_token(
            token=token,
            secret_key=request.app.state.secret_key,
            user=user,
            session=session,
        )
    except (ValueError, JSONDecodeError, InvalidTag, KeyError) as err:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(err),
        )
