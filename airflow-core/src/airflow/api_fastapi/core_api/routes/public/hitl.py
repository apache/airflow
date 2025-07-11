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

import structlog
from fastapi import Depends, HTTPException, status
from sqlalchemy import select

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
from airflow.models.hitl import HITLDetail as HITLDetailModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.hitl_shared_links import hitl_shared_link_manager

hitl_router = AirflowRouter(tags=["HumanInTheLoop"], prefix="/hitl-details")

log = structlog.get_logger(__name__)


def _get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int | None = None,
) -> TI:
    """
    Get a task instance by its identifiers.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param session: Database session
    :param map_index: Map index for mapped tasks
    """
    query = select(TI).where(
        TI.dag_id == dag_id,
        TI.run_id == dag_run_id,
        TI.task_id == task_id,
    )

    if map_index is not None:
        query = query.where(TI.map_index == map_index)

    task_instance = session.scalar(query)
    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )
    if map_index is None and task_instance.map_index != -1:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "Task instance is mapped, add the map_index value to the URL"
        )

    return task_instance


def _update_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep | None,
    session: SessionDep,
    map_index: int | None = None,
) -> HITLDetailResponse:
    """
    Update a Human-in-the-loop detail.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param update_hitl_detail_payload: Payload containing update data
    :param user: User performing the update
    :param session: Database session
    :param map_index: Map index for mapped tasks
    """
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )
    ti_id_str = str(task_instance.id)
    hitl_detail_model = session.scalar(select(HITLDetailModel).where(HITLDetailModel.ti_id == ti_id_str))
    if not hitl_detail_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop detail does not exist for Task Instance with id {ti_id_str}",
        )

    if hitl_detail_model.response_received:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop detail has already been updated for Task Instance with id {ti_id_str} "
            "and is not allowed to write again.",
        )

    hitl_detail_model.user_id = user.get_id() if user else "shared_link_action"
    hitl_detail_model.response_at = timezone.utcnow()
    hitl_detail_model.chosen_options = update_hitl_detail_payload.chosen_options
    hitl_detail_model.params_input = update_hitl_detail_payload.params_input
    session.add(hitl_detail_model)
    session.commit()
    return HITLDetailResponse.model_validate(hitl_detail_model)


def _get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    map_index: int | None = None,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )
    if task_instance is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}` and map_index: `{map_index}` was not found",
        )

    ti_id_str = str(task_instance.id)
    hitl_detail_model = session.scalar(select(HITLDetailModel).where(HITLDetailModel.ti_id == ti_id_str))
    if not hitl_detail_model:
        log.error("Human-in-the-loop detail not found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Human-in-the-loop detail not found",
            },
        )
    return HITLDetail.model_validate(hitl_detail_model)


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
    return _update_hitl_detail(
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
    return _update_hitl_detail(
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
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    return _get_hitl_detail(
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
    return _get_hitl_detail(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )


@hitl_router.get(
    "/",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def get_hitl_details(
    readable_ti_filter: ReadableTIFilterDep,
    session: SessionDep,
) -> HITLDetailCollection:
    """Get all Human-in-the-loop details."""
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


@hitl_router.post(
    "/api/v2/hitl-details-share-link/{dag_id}/{dag_run_id}/{task_id}",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def create_hitl_share_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
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
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=None,
    )

    ti_id_str = str(task_instance.id)
    hitl_detail_model = session.scalar(select(HITLDetailModel).where(HITLDetailModel.ti_id == ti_id_str))
    if not hitl_detail_model:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Human-in-the-loop detail does not exist for Task Instance with id {ti_id_str}",
        )

    try:
        link_data = hitl_shared_link_manager.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            map_index=None,
            link_type=update_hitl_detail_payload.link_type,
            action=update_hitl_detail_payload.action,
            expires_in_hours=update_hitl_detail_payload.expires_in_hours,
        )

        response = HITLDetailResponse(
            user_id=user.get_id(),
            response_at=timezone.utcnow(),
            chosen_options=update_hitl_detail_payload.chosen_options,
            params_input=update_hitl_detail_payload.params_input,
            task_instance_id=link_data["task_instance_id"],
            link_url=link_data["link_url"],
            expires_at=link_data["expires_at"],
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
    "/api/v2/hitl-details-share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
    dependencies=[Depends(requires_access_dag(method="GET", access_entity=DagAccessEntity.TASK_INSTANCE))],
)
def create_mapped_ti_hitl_share_link(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: GetUserDep,
    session: SessionDep,
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
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
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
        link_data = hitl_shared_link_manager.generate_link(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            map_index=map_index,
            link_type=update_hitl_detail_payload.link_type,
            action=update_hitl_detail_payload.action,
            expires_in_hours=update_hitl_detail_payload.expires_in_hours,
        )

        response = HITLDetailResponse(
            user_id=user.get_id(),
            response_at=timezone.utcnow(),
            chosen_options=update_hitl_detail_payload.chosen_options,
            params_input=update_hitl_detail_payload.params_input,
            task_instance_id=link_data["task_instance_id"],
            link_url=link_data["link_url"],
            expires_at=link_data["expires_at"],
            action=link_data["action"],
            link_type=link_data["link_type"],
        )

        return response

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.get(
    "/api/v2/hitl-details-share-link/{dag_id}/{dag_run_id}/{task_id}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
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
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)

        if link_data.get("link_type") != "redirect":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "This link is not a redirect link",
            )

        return _get_hitl_detail(
            dag_id=link_data["dag_id"],
            dag_run_id=link_data["dag_run_id"],
            task_id=link_data["task_id"],
            session=session,
            map_index=link_data.get("map_index"),
        )

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.get(
    "/api/v2/hitl-details-share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
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
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)

        if link_data.get("link_type") != "redirect":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "This link is not a redirect link",
            )

        return _get_hitl_detail(
            dag_id=link_data["dag_id"],
            dag_run_id=link_data["dag_run_id"],
            task_id=link_data["task_id"],
            session=session,
            map_index=link_data.get("map_index"),
        )

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.post(
    "/api/v2/hitl-details-share-link/{dag_id}/{dag_run_id}/{task_id}/action",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
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
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)

        if link_data.get("link_type") != "action":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "This link is not an action link",
            )

        return _update_hitl_detail(
            dag_id=link_data["dag_id"],
            dag_run_id=link_data["dag_run_id"],
            task_id=link_data["task_id"],
            session=session,
            update_hitl_detail_payload=update_hitl_detail_payload,
            user=None,
            map_index=link_data.get("map_index"),
        )

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )


@hitl_router.post(
    "/api/v2/hitl-details-share-link/{dag_id}/{dag_run_id}/{task_id}/{map_index}/action",
    status_code=status.HTTP_200_OK,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_400_BAD_REQUEST,
            status.HTTP_403_FORBIDDEN,
        ]
    ),
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
    if not hitl_shared_link_manager.is_enabled():
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "HITL shared links are not enabled",
        )

    try:
        link_data = hitl_shared_link_manager.verify_link(payload, signature)

        if link_data.get("link_type") != "action":
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                "This link is not an action link",
            )

        return _update_hitl_detail(
            dag_id=link_data["dag_id"],
            dag_run_id=link_data["dag_run_id"],
            task_id=link_data["task_id"],
            session=session,
            update_hitl_detail_payload=update_hitl_detail_payload,
            user=None,
            map_index=link_data.get("map_index"),
        )

    except ValueError as e:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(e),
        )
