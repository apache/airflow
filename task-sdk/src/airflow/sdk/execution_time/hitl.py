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

from typing import TYPE_CHECKING, Any
from uuid import UUID

from airflow.sdk.execution_time.comms import (
    CreateHITLResponsePayload,
    GetHITLResponseContentDetail,
    UpdateHITLResponse,
)

if TYPE_CHECKING:
    from airflow.api_fastapi.execution_api.datamodels.hitl import HITLResponseContentDetail


def add_hitl_response(
    ti_id: UUID,
    options: list[str],
    subject: str,
    body: str | None = None,
    default: list[str] | None = None,
    multiple: bool = False,
    params: dict[str, Any] | None = None,
) -> None:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    SUPERVISOR_COMMS.send(
        msg=CreateHITLResponsePayload(
            ti_id=ti_id,
            options=options,
            subject=subject,
            body=body,
            default=default,
            params=params,
            multiple=multiple,
        )
    )


def update_htil_response_content_detail(
    ti_id: UUID,
    response_content: list[str],
    params_input: dict[str, Any],
) -> HITLResponseContentDetail:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(
        msg=UpdateHITLResponse(
            ti_id=ti_id,
            response_content=response_content,
            params_input=params_input,
        ),
    )
    if TYPE_CHECKING:
        assert isinstance(response, HITLResponseContentDetail)
    return response


def get_hitl_response_content_detail(ti_id: UUID) -> HITLResponseContentDetail:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(msg=GetHITLResponseContentDetail(ti_id=ti_id))

    if TYPE_CHECKING:
        assert isinstance(response, HITLResponseContentDetail)
    return response
