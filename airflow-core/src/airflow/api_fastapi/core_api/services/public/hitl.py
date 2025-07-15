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
from fastapi import HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetail,
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.models.hitl import HITLDetail as HITLDetailModel
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils import timezone

log = structlog.get_logger(__name__)


def service_get_task_instance(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    try_number: int | None = None,
    map_index: int | None = None,
) -> TI:
    """
    Get a task instance by its identifiers.

    :param dag_id: DAG ID
    :param dag_run_id: DAG run ID
    :param task_id: Task ID
    :param try_number: Try number for the task (optional, will get current if not provided)
    :param session: Database session
    :param map_index: Map index for mapped tasks
    """
    query = select(TI).where(
        TI.dag_id == dag_id,
        TI.run_id == dag_run_id,
        TI.task_id == task_id,
    )

    if try_number is not None:
        query = query.where(TI.try_number == try_number)

    if map_index is not None:
        query = query.where(TI.map_index == map_index)

    task_instance = session.scalar(query)
    if task_instance is None:
        error_msg = f"The Task Instance with dag_id: `{dag_id}`, run_id: `{dag_run_id}`, task_id: `{task_id}`"
        if try_number is not None:
            error_msg += f", try_number: `{try_number}`"
        if map_index is not None:
            error_msg += f", map_index: `{map_index}`"
        error_msg += " was not found"
        raise HTTPException(status.HTTP_404_NOT_FOUND, error_msg)

    if map_index is None and task_instance.map_index != -1:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, "Task instance is mapped, add the map_index value to the URL"
        )

    return task_instance


def service_update_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int,
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
    :param try_number: Try number for the task
    :param update_hitl_detail_payload: Payload containing update data
    :param user: User performing the update (None for shared link actions)
    :param session: Database session
    :param map_index: Map index for mapped tasks
    """
    task_instance = service_get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        try_number=try_number,
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


def service_get_hitl_detail(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    session: SessionDep,
    try_number: int | None = None,
    map_index: int | None = None,
) -> HITLDetail:
    """Get a Human-in-the-loop detail of a specific task instance."""
    task_instance = service_get_task_instance(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        session=session,
        try_number=try_number,
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
