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

from collections.abc import MutableMapping
from typing import TYPE_CHECKING
from uuid import UUID

from airflow.providers.standard.execution_time.comms import (
    CreateHITLInputRequestPayload,
    GetHITLResponseContentDetail,
    UpdateHITLResponse,
)

if TYPE_CHECKING:
    from airflow.providers.standard.api_fastapi.execution_api.datamodels.hitl import HITLResponseContentDetail


def add_hitl_input_request(
    ti_id: UUID,
    options: list[str],
    subject: str,
    body: str | None = None,
    default: list[str] | None = None,
    multiple: bool = False,
    params: MutableMapping | None = None,
    form_content: MutableMapping | None = None,
) -> None:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    SUPERVISOR_COMMS.send(
        msg=CreateHITLInputRequestPayload(
            ti_id=ti_id,
            options=options,
            subject=subject,
            body=body,
            default=default,
            params=params,
            multiple=multiple,
            form_content=form_content,
        )
    )


def update_htil_response_content_detail(
    ti_id: UUID,
    response_content: str,
) -> HITLResponseContentDetail:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(
        msg=UpdateHITLResponse(ti_id=ti_id, response_content=response_content),
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
