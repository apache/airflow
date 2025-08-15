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
"""Services for HITL shared links functionality."""

from __future__ import annotations

from collections.abc import Callable
from functools import wraps
from typing import Any

import structlog
from fastapi import HTTPException, Request, status
from pydantic import HttpUrl

from airflow.api_fastapi.common.db.common import SessionDep
from airflow.api_fastapi.core_api.datamodels.hitl import (
    HITLDetailResponse,
    UpdateHITLDetailPayload,
)
from airflow.api_fastapi.core_api.datamodels.hitl_shared_links import (
    GenerateHITLSharedLinkRequest,
    HITLSharedLinkResponse,
)
from airflow.api_fastapi.core_api.security import GetUserDep
from airflow.api_fastapi.core_api.services.public.hitl import (
    get_task_instnace,
    update_hitl_detail_through_payload,
)
from airflow.exceptions import HITLSharedLinkDisabled, HITLSharedLinkTimeout
from airflow.models.hitl import HITLDetail, HITLSharedLinkConfig, HITLSharedLinkData, hitl_shared_link_enabled

log = structlog.get_logger(__name__)


def hitl_shared_link_endpoint_enabled(func: Callable) -> Callable:
    """Check if 'api.hitl_enable_shared_links' is set to True in the config on endpoint level."""

    @wraps(func)
    def wrapper(*args: tuple[Any, ...], **kwargs: dict[str, Any]) -> Any:
        try:
            return hitl_shared_link_enabled(func)
        except HITLSharedLinkDisabled as err:
            raise HTTPException(status.HTTP_403_FORBIDDEN, str(err))

    return wrapper


def update_hitl_detail_through_token(
    *,
    token: str,
    secret_key: str,
    user: GetUserDep,
    session: SessionDep,
) -> HITLDetailResponse:
    shared_data = HITLSharedLinkData.decode(token=token, secret_key=secret_key)
    shared_link_config = shared_data.shared_link_config
    if (link_type := shared_link_config.link_type) != "respond":
        raise ValueError(f"Unexpected link_type '{link_type}'")

    shared_data.validate_ti_id(session=session)

    return update_hitl_detail_through_payload(
        dag_id=shared_data.dag_id,
        dag_run_id=shared_data.dag_run_id,
        task_id=shared_data.task_id,
        map_index=shared_data.map_index,
        update_hitl_detail_payload=UpdateHITLDetailPayload(
            chosen_options=shared_link_config.chosen_options,
            params_input=shared_link_config.params_input,
        ),
        session=session,
        user=user,
    )


def generate_hitl_shared_link_from_request(
    # task instance identifier
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    map_index: int | None,
    # payload
    generate_shared_link_request: GenerateHITLSharedLinkRequest,
    # utitliy
    request: Request,
    session: SessionDep,
) -> HITLSharedLinkResponse:
    task_instance = get_task_instnace(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        map_index=map_index,
        session=session,
        joinedload_hitl_detail=True,
    )
    hitl_detail: HITLDetail = task_instance.hitl_detail
    if hitl_detail is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "Human in the loop detail does not exist.",
        )

    expires_at = HITLSharedLinkConfig.infer_expires_at(generate_shared_link_request.expires_at)

    try:
        link_url = hitl_detail.generate_shared_link(
            # task instance identifier
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            map_index=map_index,
            # Human-in-the-loop shared link config
            base_url=str(request.base_url),
            link_type=generate_shared_link_request.link_type,
            expires_at=expires_at,
            chosen_options=generate_shared_link_request.chosen_options,
            params_input=generate_shared_link_request.params_input,
            # utility
            secret_key=request.app.state.secret_key,
            session=session,
        )
    except ValueError as err:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            str(err),
        )
    except HITLSharedLinkTimeout:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Set 'expires_at' at a past time is not allowed.",
        )
    return HITLSharedLinkResponse(
        url=HttpUrl(link_url),
        expires_at=expires_at,
    )
