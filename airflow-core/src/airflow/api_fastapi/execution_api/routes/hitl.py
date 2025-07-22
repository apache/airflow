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
    HITLDetailRequest,
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.models.hitl import HITLDetail

router = APIRouter()

log = structlog.get_logger(__name__)


@router.post(
    "/{task_instance_id}",
    status_code=status.HTTP_201_CREATED,
)
def add_hitl_detail(
    task_instance_id: UUID,
    payload: HITLDetailRequest,
    session: SessionDep,
) -> HITLDetailRequest:
    """Get Human-in-the-loop detail for a specific Task Instance."""
    ti_id_str = str(task_instance_id)
    hitl_detail_model = session.scalar(select(HITLDetail).where(HITLDetail.ti_id == ti_id_str))
    if hitl_detail_model:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop detail for Task Instance with id {ti_id_str} already exists.",
        )

    hitl_detail = HITLDetail(
        ti_id=ti_id_str,
        options=payload.options,
        subject=payload.subject,
        body=payload.body,
        defaults=payload.defaults,
        multiple=payload.multiple,
        params=payload.params,
    )
    session.add(hitl_detail)
    session.commit()
    return HITLDetailRequest.model_validate(hitl_detail)


@router.patch("/{task_instance_id}")
def update_hitl_detail(
    task_instance_id: UUID,
    payload: UpdateHITLDetailPayload,
    session: SessionDep,
) -> HITLDetailResponse:
    """Update the response part of a Human-in-the-loop detail for a specific Task Instance."""
    ti_id_str = str(task_instance_id)
    hitl_detail_model = session.execute(select(HITLDetail).where(HITLDetail.ti_id == ti_id_str)).scalar()
    if hitl_detail_model.response_received:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Human-in-the-loop detail for Task Instance with id {ti_id_str} already exists.",
        )

    hitl_detail_model.user_id = "Fallback to defaults"
    hitl_detail_model.response_at = datetime.now(timezone.utc)
    hitl_detail_model.chosen_options = payload.chosen_options
    hitl_detail_model.params_input = payload.params_input
    session.add(hitl_detail_model)
    session.commit()
    return HITLDetailResponse.from_hitl_detail_orm(hitl_detail_model)


@router.get(
    "/{task_instance_id}",
    status_code=status.HTTP_200_OK,
)
def get_hitl_detail(
    task_instance_id: UUID,
    session: SessionDep,
) -> HITLDetailResponse:
    """Get Human-in-the-loop detail for a specific Task Instance."""
    ti_id_str = str(task_instance_id)
    hitl_detail_model = session.execute(
        select(HITLDetail).where(HITLDetail.ti_id == ti_id_str),
    ).scalar()
    return HITLDetailResponse.from_hitl_detail_orm(hitl_detail_model)
