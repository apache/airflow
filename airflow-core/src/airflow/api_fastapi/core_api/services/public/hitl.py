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
"""Services for Human-in-the-Loop (HITL) functionality."""

from __future__ import annotations

import structlog
from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetail,
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.models.hitl import HITLDetail as HITLDetailModel
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone

log = structlog.get_logger(__name__)


def _get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session,
    map_index: int | None = None,
) -> TaskInstance:
    """
    Get a task instance by its identifiers.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param session: Database session
    :param map_index: Map index for mapped tasks
    :return: Task instance
    """
    query = select(TaskInstance).where(
        TaskInstance.dag_id == dag_id,
        TaskInstance.run_id == dag_run_id,
        TaskInstance.task_id == task_id,
    )

    if map_index is not None:
        query = query.where(TaskInstance.map_index == map_index)

    task_instance = session.scalar(query)

    if not task_instance:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"Task instance not found: {dag_id}/{dag_run_id}/{task_id}"
            + (f"/{map_index}" if map_index is not None else ""),
        )

    return task_instance


def service_get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session,
    try_number: int = 1,
    map_index: int | None = None,
) -> HITLDetail:
    """
    Get HITL detail for a task instance.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param session: Database session
    :param try_number: Try number for the task
    :param map_index: Map index for mapped tasks
    :return: HITL detail
    """
    # Get the task instance
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )

    # Get HITL detail
    hitl_detail = (
        session.query(HITLDetailModel).filter(HITLDetailModel.ti_id == str(task_instance.id)).first()
    )

    if not hitl_detail:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"HITL detail not found for task instance {task_instance.id}",
        )

    return HITLDetail.model_validate(hitl_detail)


def service_update_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: Session,
    update_hitl_detail_payload: UpdateHITLDetailPayload,
    user: str | None = None,
    map_index: int | None = None,
    try_number: int = 1,
) -> HITLDetailResponse:
    """
    Update HITL detail for a task instance.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param session: Database session
    :param update_hitl_detail_payload: Update payload
    :param user: User performing the update
    :param map_index: Map index for mapped tasks
    :param try_number: Try number for the task
    :return: Updated HITL detail response
    """
    # Get the task instance
    task_instance = _get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        map_index=map_index,
    )

    # Get HITL detail
    hitl_detail = (
        session.query(HITLDetailModel).filter(HITLDetailModel.ti_id == str(task_instance.id)).first()
    )

    if not hitl_detail:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"HITL detail not found for task instance {task_instance.id}",
        )

    # Check if response already received
    if hitl_detail.response_received:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "HITL detail has already been responded to",
        )

    # Update the HITL detail
    hitl_detail.user_id = user or "shared_link_user"
    hitl_detail.response_at = timezone.utcnow()
    hitl_detail.chosen_options = update_hitl_detail_payload.chosen_options
    hitl_detail.params_input = update_hitl_detail_payload.params_input
    # response_received is a computed property based on response_at, so we don't set it directly

    session.commit()

    # Return the response
    return HITLDetailResponse(
        user_id=hitl_detail.user_id,
        response_at=hitl_detail.response_at,
        chosen_options=hitl_detail.chosen_options,
        params_input=hitl_detail.params_input,
        task_instance_id=str(task_instance.id),
    )
