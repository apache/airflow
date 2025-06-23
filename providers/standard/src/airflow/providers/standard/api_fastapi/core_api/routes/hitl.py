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
from fastapi import HTTPException, status
from sqlalchemy import select
from structlog.contextvars import bind_contextvars

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.api_fastapi.core_api.datamodels.hitl import (
    AddHITLResponsePayload,
    HITLResponse,
)
from airflow.providers.standard.models import HITLResponseModel

hitl_router = AirflowRouter(tags=["HumanInTheLoop"])

log = structlog.get_logger(__name__)


@hitl_router.post(
    "/{task_instance_id}/response",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
)
def write_response(
    task_instance_id: UUID,
    add_response_payload: AddHITLResponsePayload,
    user: GetUserDep,
    session: SessionDep,
) -> HITLResponse:
    """Write an HITLResponse."""
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

    existing_response = session.scalar(select(HITLResponseModel).where(HITLResponseModel.ti_id == ti_id_str))
    if existing_response:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"HITL Response exists for task task_instance id {ti_id_str}",
        )

    hitl_response_model = HITLResponseModel(
        ti_id=ti_id_str,
        content=add_response_payload.content,
        user_id=user.get_id(),
    )
    session.add(hitl_response_model)
    session.commit()
    return HITLResponse.model_validate(hitl_response_model)
