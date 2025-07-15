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

from datetime import datetime

import structlog
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
from airflow.api_fastapi.common.db.common import SessionDep, paginated_select
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
    service_get_hitl_detail,
    service_get_task_instance,
    service_update_hitl_detail,
)
from airflow.models.hitl import HITLDetail as HITLDetailModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.utils.hitl_shared_links import hitl_shared_link_manager
from airflow.utils import timezone

hitl_router = AirflowRouter(tags=["HumanInTheLoop"], prefix="/hitl-details")

log = structlog.get_logger(__name__)


def requires_hitl_shared_links_enabled():
    """Dependency to check if HITL shared links are enabled."""
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )


@hitl_router.patch(
    "/{dag_id}/{dag_run_id}/{task_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
    try_number = update_hitl_detail_payload.try_number
    return service_update_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=try_number,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        session=session,
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
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
    return service_update_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        try_number=update_hitl_detail_payload.try_number,
        session=session,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        map_index=map_index,
    )


@hitl_router.get(
    "/{dag_id}/{dag_run_id}/{task_id}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    return service_get_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=None,
    )


@hitl_router.get(
    "/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc([status.HTTP_404_NOT_FOUND]),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_mapped_ti_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    return service_get_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )


@hitl_router.get(
    "/",
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_hitl_details(
    readable_ti_filter: ReadableTIFilterDep,
    session: SessionDep,
) -> HITLDetailCollection:
    """Get Human-in-the-loop details."""
    query = select(HITLDetailModel).join(TI, HITLDetailModel.ti_id == TI.id)
    hitl_detail_select, total_entries = paginated_select(
        statement=query,
        filters=[readable_ti_filter],
        session=session,
    )
    hitl_details = session.scalars(hitl_detail_select)
    return HITLDetailCollection(
        hitl_details=hitl_details,
        total_entries=total_entries,
    )


def _create_hitl_share_link_common(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
    request: Request,
    map_index: int | None = None,
) -> HITLDetailResponse:
    """Create a shared link for HITL tasks using common logic."""
    task_instance = service_get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        try_number=update_hitl_detail_payload.try_number,
        map_index=map_index,
    )

    ti_id_str = str(task_instance.id)
    hitl_detail_model = session.scalar(select(HITLDetailModel).where(HITLDetailModel.ti_id == ti_id_str))
    if not hitl_detail_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop detail does not exist for Task Instance with id {ti_id_str}",
        )

    try:
        base_url = f"{request.base_url.scheme}://{request.base_url.netloc}"

        link_data = hitl_shared_link_manager.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            try_number=update_hitl_detail_payload.try_number,
            map_index=map_index,
            link_type=update_hitl_detail_payload.link_type,
            action=update_hitl_detail_payload.action,
            base_url=base_url,
        )

        response = HITLDetailResponse(
            user_id=user.get_id(),
            response_at=timezone.utcnow(),
            chosen_options=update_hitl_detail_payload.chosen_options,
            params_input=update_hitl_detail_payload.params_input,
            task_instance_id=None,  # No longer using this field
            link_url=link_data["url"],
            expires_at=datetime.fromisoformat(link_data["expires_at"]),
            action=link_data["action"],
            link_type=link_data["link_type"],
        )

        return response

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.post(
    "/share-link/{dag_id}/{dag_run_id}/{task_id}",
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
        Depends(requires_hitl_shared_links_enabled),
    ],
)
def create_hitl_share_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
    request: Request,
) -> HITLDetailResponse:
    """
    Create a shared link for a Human-in-the-loop task.

    This endpoint generates a secure, time-limited shared link that allows external users
    to interact with HITL tasks without requiring full Airflow authentication. The link
    can be configured for either direct action execution or UI redirection.

    :param dag_id: The DAG identifier
    :param dag_run_id: The DAG run identifier
    :param task_id: The task identifier
    :param update_hitl_detail_payload: Payload containing link configuration and initial response data
    :param user: The authenticated user creating the shared link
    :param session: Database session for data persistence

    :raises HTTPException: 403 if HITL shared links are not enabled
    :raises HTTPException: 404 if the task instance or HITL detail does not exist
    :raises HTTPException: 400 if link generation fails due to invalid parameters

    :return: HITLDetailResponse containing the generated link URL and metadata
    """
    return _create_hitl_share_link_common(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        session=session,
        request=request,
        map_index=None,
    )


@hitl_router.post(
    "/share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
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
        Depends(requires_hitl_shared_links_enabled),
    ],
)
def create_mapped_ti_hitl_share_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
    request: Request,
) -> HITLDetailResponse:
    """
    Create a shared link for a mapped Human-in-the-loop task.

    This endpoint generates a secure, time-limited shared link for mapped task instances,
    allowing external users to interact with specific mapped HITL tasks without requiring
    full Airflow authentication. The link can be configured for either direct action
    execution or UI redirection.

    :param dag_id: The DAG identifier
    :param dag_run_id: The DAG run identifier
    :param task_id: The task identifier
    :param map_index: The map index for the mapped task instance
    :param update_hitl_detail_payload: Payload containing link configuration and initial response data
    :param user: The authenticated user creating the shared link
    :param session: Database session for data persistence
    """
    return _create_hitl_share_link_common(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        update_hitl_detail_payload=update_hitl_detail_payload,
        user=user,
        session=session,
        request=request,
        map_index=map_index,
    )


def _get_hitl_share_link_common(
    payload: str,
    signature: str,
    session: SessionDep,
) -> HITLDetail:
    """Get HITL details via shared link using common logic."""
    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)

        if link_data.link_type != "redirect":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "This link is not a redirect link",
            )

        return service_get_hitl_detail(
            dag_id=link_data.dag_id,
            dag_run_id=link_data.dag_run_id,
            task_id=link_data.task_id,
            session=session,
            try_number=link_data.try_number,
            map_index=link_data.map_index,
        )

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.get(
    "/share-link/{dag_id}/{dag_run_id}/{task_id}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_hitl_shared_links_enabled)],
)
def get_hitl_share_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    payload: str,
    signature: str,
    session: SessionDep,
) -> HITLDetail:
    """
    Get HITL details via shared link (for redirect links).

    This endpoint allows external users to access HITL task details through a secure
    shared link. The link must be a redirect-type link, which provides read-only access
    to the HITL task information for UI rendering or decision-making purposes.

    :param dag_id: The DAG identifier (from URL path)
    :param dag_run_id: The DAG run identifier (from URL path)
    :param task_id: The task identifier (from URL path)
    :param payload: Base64-encoded payload containing link metadata and expiration
    :param signature: HMAC signature for payload verification
    :param session: Database session for data retrieval
    """
    return _get_hitl_share_link_common(payload, signature, session)


@hitl_router.get(
    "/share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_hitl_shared_links_enabled)],
)
def get_mapped_ti_hitl_share_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    payload: str,
    signature: str,
    session: SessionDep,
) -> HITLDetail:
    """
    Get mapped HITL details via shared link (for redirect links).

    This endpoint allows external users to access mapped HITL task details through a secure
    shared link. The link must be a redirect-type link, which provides read-only access
    to the mapped HITL task information for UI rendering or decision-making purposes.

    :param dag_id: The DAG identifier (from URL path)
    :param dag_run_id: The DAG run identifier (from URL path)
    :param task_id: The task identifier (from URL path)
    :param map_index: The map index for the mapped task instance (from URL path)
    :param payload: Base64-encoded payload containing link metadata and expiration
    :param signature: HMAC signature for payload verification
    :param session: Database session for data retrieval
    """
    return _get_hitl_share_link_common(payload, signature, session)


def _execute_hitl_share_link_action_common(
    payload: str,
    signature: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    session: SessionDep,
) -> HITLDetailResponse:
    """Execute HITL actions via shared link using common logic."""
    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)

        if link_data.link_type != "action":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "This link is not an action link",
            )

        return service_update_hitl_detail(
            dag_id=link_data.dag_id,
            dag_run_id=link_data.dag_run_id,
            task_id=link_data.task_id,
            try_number=link_data.try_number,
            session=session,
            update_hitl_detail_payload=update_hitl_detail_payload,
            user=None,
            map_index=link_data.map_index,
        )

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.post(
    "/share-link/{dag_id}/{dag_run_id}/{task_id}/action",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_hitl_shared_links_enabled)],
)
def execute_hitl_share_link_action(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    payload: str,
    signature: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    session: SessionDep,
) -> HITLDetailResponse:
    """
    Execute an action via shared link (for action links).

    This endpoint allows external users to execute HITL task actions through a secure
    shared link. The link must be an action-type link, which enables direct execution
    of predefined actions (e.g., approve, reject) without requiring full Airflow
    authentication. The action is executed immediately and the HITL task is updated
    with the user's response.

    :param dag_id: The DAG identifier (from URL path)
    :param dag_run_id: The DAG run identifier (from URL path)
    :param task_id: The task identifier (from URL path)
    :param payload: Base64-encoded payload containing link metadata and expiration
    :param signature: HMAC signature for payload verification
    :param update_hitl_detail_payload: Payload containing the action response data
    :param session: Database session for data persistence
    """
    return _execute_hitl_share_link_action_common(payload, signature, update_hitl_detail_payload, session)


@hitl_router.post(
    "/share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}/action",
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
            status.HTTP_404_NOT_FOUND,
        ]
    ),
    dependencies=[Depends(requires_hitl_shared_links_enabled)],
)
def execute_mapped_ti_hitl_share_link_action(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    payload: str,
    signature: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    session: SessionDep,
) -> HITLDetailResponse:
    """
    Execute an action via shared link for mapped tasks (for action links).

    This endpoint allows external users to execute mapped HITL task actions through a secure
    shared link. The link must be an action-type link, which enables direct execution
    of predefined actions (e.g., approve, reject) for specific mapped task instances
    without requiring full Airflow authentication. The action is executed immediately
    and the mapped HITL task is updated with the user's response.

    :param dag_id: The DAG identifier (from URL path)
    :param dag_run_id: The DAG run identifier (from URL path)
    :param task_id: The task identifier (from URL path)
    :param map_index: The map index for the mapped task instance (from URL path)
    :param payload: Base64-encoded payload containing link metadata and expiration
    :param signature: HMAC signature for payload verification
    :param update_hitl_detail_payload: Payload containing the action response data
    :param session: Database session for data persistence
    """
    return _execute_hitl_share_link_action_common(payload, signature, update_hitl_detail_payload, session)
