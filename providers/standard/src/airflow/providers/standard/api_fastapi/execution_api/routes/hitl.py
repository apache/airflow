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
from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select
from structlog.contextvars import bind_contextvars

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.api_fastapi.execution_api.datamodels.hitl import (
    HITLInputRequestResponse,
    HITLResponse,
)
from airflow.providers.standard.models import HITLInputRequestModel, HITLResponseModel
from airflow.sdk.execution_time.comms import CreateHITLInputRequestPayload

router = APIRouter()

log = structlog.get_logger(__name__)


@router.post(
    "/input-requests/{task_instance_id}",
    status_code=status.HTTP_201_CREATED,
)
def add_hitl_input_request(
    task_instance_id: UUID,
    payload: CreateHITLInputRequestPayload,
    session: SessionDep,
) -> HITLInputRequestResponse:
    """Get Human-in-the-loop Response for a specific Task Instance."""
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

    hitl_input_request = HITLInputRequestModel(
        ti_id=ti_id_str,
        options=payload.options,
        subject=payload.subject,
        body=payload.body,
        default=payload.default,
        params=payload.params,
        multiple=payload.multiple,
    )
    session.add(hitl_input_request)
    session.commit()
    return HITLInputRequestResponse.model_validate(hitl_input_request)


@router.get(
    "/response/{task_instance_id}",
    status_code=status.HTTP_200_OK,
)
def get_hitl_response(
    task_instance_id: UUID,
    session: SessionDep,
) -> HITLResponse:
    """Get Human-in-the-loop Response for a specific Task Instance."""
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

    hitl_response = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str),
    )
    return HITLResponse(
        ti_id=task_instance_id,
        content=hitl_response.content if hitl_response else None,
    )
