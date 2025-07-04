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

from datetime import datetime, timezone
from uuid import UUID

import structlog
from fastapi import APIRouter, HTTPException, status
from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.execution_api.datamodels.hitl import (
    HITLInputRequestResponse,
    HITLResponseContentDetail,
)
from airflow.providers.standard.models import HITLResponseModel
from airflow.sdk.execution_time.comms import CreateHITLResponsePayload, UpdateHITLResponse

router = APIRouter()

log = structlog.get_logger(__name__)


@router.post(
    "/{task_instance_id}",
    status_code=status.HTTP_201_CREATED,
)
def add_hitl_response(
    task_instance_id: UUID,
    payload: CreateHITLResponsePayload,
    session: SessionDep,
) -> HITLInputRequestResponse:
    """Get Human-in-the-loop Response for a specific Task Instance."""
    ti_id_str = str(task_instance_id)
    hitl_response_model = session.scalar(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    )
    if hitl_response_model:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop Input Request for Task Instance with id {ti_id_str} already exists.",
        )

    hitl_input_request = HITLResponseModel(
        ti_id=ti_id_str,
        options=payload.options,
        subject=payload.subject,
        body=payload.body,
        default=payload.default,
        multiple=payload.multiple,
        params=payload.params,
    )
    session.add(hitl_input_request)
    session.commit()
    return HITLInputRequestResponse.model_validate(hitl_input_request)


@router.patch("/{task_instance_id}")
def update_hitl_response(
    task_instance_id: UUID,
    payload: UpdateHITLResponse,
    session: SessionDep,
) -> HITLResponseContentDetail:
    """Get Human-in-the-loop Response for a specific Task Instance."""
    ti_id_str = str(task_instance_id)
    hitl_response_model = session.execute(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    ).scalar()
    if hitl_response_model.response_received:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop Response Content for Task Instance with id {ti_id_str} already exists.",
        )

    hitl_response_model.user_id = "fallback to default"
    hitl_response_model.response_content = payload.response_content
    hitl_response_model.params_input = payload.params_input
    hitl_response_model.response_at = datetime.now(timezone.utc)
    session.add(hitl_response_model)
    session.commit()
    return HITLResponseContentDetail(
        response_received=hitl_response_model.response_received,
        response_at=hitl_response_model.response_at,
        user_id=hitl_response_model.user_id,
        response_content=hitl_response_model.response_content,
    )


@router.get(
    "/{task_instance_id}",
    status_code=status.HTTP_200_OK,
)
def get_hitl_response(
    task_instance_id: UUID,
    session: SessionDep,
) -> HITLResponseContentDetail:
    """Get Human-in-the-loop Response for a specific Task Instance."""
    ti_id_str = str(task_instance_id)
    hitl_response_model = session.execute(
        select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str)
    ).scalar()
    return HITLResponseContentDetail(
        response_received=hitl_response_model.response_received,
        response_at=hitl_response_model.response_at,
        user_id=hitl_response_model.user_id,
        response_content=hitl_response_model.response_content,
    )
