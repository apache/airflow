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
# TODO: move it to provider
from __future__ import annotations

from datetime import datetime
from uuid import UUID

import structlog
from fastapi import HTTPException, status
from sqlalchemy import select
from structlog.contextvars import bind_contextvars

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.core_api.openapi.exceptions import create_openapi_http_exception_doc
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.api_fastapi.core_api.datamodels.interactive import (
    AddInteractiveResponsePayload,
    InteractiveResponse,
)
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.models import InteractiveResponseModel

interactive_router = AirflowRouter(tags=["InteractiveResponse"], prefix="/interactive")

log = structlog.get_logger(__name__)


@interactive_router.post(
    "/{task_instance_id}/response",
    status_code=status.HTTP_201_CREATED,
    responses=create_openapi_http_exception_doc(
        [
            status.HTTP_404_NOT_FOUND,
            status.HTTP_409_CONFLICT,
        ]
    ),
<<<<<<< HEAD
)
def write_response(
    task_instance_id: UUID,
    add_response_payload: AddInteractiveResponsePayload,
    session: SessionDep,
) -> InteractiveResponse:
    status_code=status.HTTP_200_OK,
=======
>>>>>>> a7ed69aee6 (fixup! feat: add write_response api)
)
def write_response(
    task_instance_id: UUID,
    add_response_payload: AddInteractiveResponsePayload,
    session: SessionDep,
) -> InteractiveResponse:
    """Write an InteractiveResponse."""
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

    existing_response = session.scalar(
        select(InteractiveResponseModel).where(InteractiveResponseModel.ti_id == ti_id_str)
    )
    if existing_response:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"Interactive Response exists for task task_instance id {ti_id_str}",
        )

    from airflow.utils import timezone

    interactive_response_model = InteractiveResponseModel(
        id=1,
        ti_id=ti_id_str,
        content=add_response_payload.content,
        created_at=datetime.now(timezone.utc),
    )
    session.add(interactive_response_model)
    session.commit()
    ret = InteractiveResponse(
        ti_id=interactive_response_model.ti_id,
        created_at=interactive_response_model.created_at,
        content=interactive_response_model.content,
    )
    return ret
