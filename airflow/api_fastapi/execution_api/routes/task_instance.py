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

import logging
from uuid import UUID

from fastapi import Body, Depends, HTTPException, status
from sqlalchemy import update
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from typing_extensions import Annotated

from airflow.api_fastapi.common.db.common import get_session
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.api_fastapi.execution_api import schemas
from airflow.models.taskinstance import TaskInstance as TI
from airflow.utils.state import State

# TODO: Add dependency on JWT token
ti_router = AirflowRouter(
    prefix="/task_instance",
    tags=["Task Instance"],
)


log = logging.getLogger(__name__)


@ti_router.patch(
    "/{task_instance_id}/state",
    status_code=status.HTTP_204_NO_CONTENT,
    # TODO: Add Operation ID to control the function name in the OpenAPI spec
    # TODO: Do we need to use create_openapi_http_exception_doc here?
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Task Instance not found"},
        status.HTTP_409_CONFLICT: {"description": "The TI is already in the requested state"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Invalid payload for the state transition"},
    },
)
async def ti_update_state(
    task_instance_id: UUID,
    ti_patch_payload: Annotated[schemas.TIStateUpdate, Body()],
    session: Annotated[Session, Depends(get_session)],
):
    """
    Update the state of a TaskInstance.

    Not all state transitions are valid, and transitioning to some states required extra information to be
    passed along. (Check our the schemas for details, the rendered docs might not reflect this accurately)
    """
    # We only use UUID above for validation purposes
    ti_id_str = str(task_instance_id)

    old = select(TI.state).where(TI.id == ti_id_str).with_for_update()
    try:
        (previous_state,) = session.execute(old).one()
    except NoResultFound:
        log.error("Task Instance %s not found", ti_id_str)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "reason": "not_found",
                "message": "Task Instance not found",
            },
        )

    # We exclude_unset to avoid updating fields that are not set in the payload
    data = ti_patch_payload.model_dump(exclude_unset=True)

    query = update(TI).where(TI.id == ti_id_str).values(data)

    if isinstance(ti_patch_payload, schemas.TIEnterRunningPayload):
        if previous_state != State.QUEUED:
            log.warning(
                "Can not start Task Instance ('%s') in invalid state: %s",
                ti_id_str,
                previous_state,
            )

            # TODO: Pass a RFC 9457 compliant error message in "detail" field
            # https://datatracker.ietf.org/doc/html/rfc9457
            # to provide more information about the error
            # FastAPI will automatically convert this to a JSON response
            # This might be added in FastAPI in https://github.com/fastapi/fastapi/issues/10370
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "reason": "invalid_state",
                    "message": "TI was not in a state where it could be marked as running",
                    "previous_state": previous_state,
                },
            )
        log.info("Task with %s state started on %s ", previous_state, ti_patch_payload.hostname)
        # Ensure there is no end date set.
        query = query.values(
            end_date=None,
            hostname=ti_patch_payload.hostname,
            unixname=ti_patch_payload.unixname,
            pid=ti_patch_payload.pid,
            state=State.RUNNING,
        )
    elif isinstance(ti_patch_payload, schemas.TITerminalStatePayload):
        query = TI.duration_expression_update(ti_patch_payload.end_date, query, session.bind)

    # TODO: Replace this with FastAPI's Custom Exception handling:
    # https://fastapi.tiangolo.com/tutorial/handling-errors/#install-custom-exception-handlers
    try:
        result = session.execute(query)
        log.info("TI %s state updated: %s row(s) affected", ti_id_str, result.rowcount)
    except SQLAlchemyError as e:
        log.error("Error updating Task Instance state: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error occurred"
        )
