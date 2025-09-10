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

from typing import TYPE_CHECKING, Any, TypedDict
from uuid import UUID

from airflow.sdk.api.datamodels._generated import HITLUser as APIHITLUser
from airflow.sdk.execution_time.comms import (
    CreateHITLDetailPayload,
    GetHITLDetailResponse,
    UpdateHITLDetail,
)

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import HITLDetailResponse


class HITLUser(TypedDict):
    id: str
    name: str


def upsert_hitl_detail(
    ti_id: UUID,
    options: list[str],
    subject: str,
    body: str | None = None,
    defaults: list[str] | None = None,
    multiple: bool = False,
    params: dict[str, Any] | None = None,
    assigned_users: list[HITLUser] | None = None,
) -> None:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    SUPERVISOR_COMMS.send(
        msg=CreateHITLDetailPayload(
            ti_id=ti_id,
            options=options,
            subject=subject,
            body=body,
            defaults=defaults,
            params=params,
            multiple=multiple,
            assigned_users=(
                [APIHITLUser(id=user["id"], name=user["name"]) for user in assigned_users]
                if assigned_users
                else []
            ),
        )
    )


def update_hitl_detail_response(
    ti_id: UUID,
    chosen_options: list[str],
    params_input: dict[str, Any],
) -> HITLDetailResponse:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(
        msg=UpdateHITLDetail(
            ti_id=ti_id,
            chosen_options=chosen_options,
            params_input=params_input,
        ),
    )
    if TYPE_CHECKING:
        assert isinstance(response, HITLDetailResponse)
    return response


def get_hitl_detail_content_detail(ti_id: UUID) -> HITLDetailResponse:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(msg=GetHITLDetailResponse(ti_id=ti_id))

    if TYPE_CHECKING:
        assert isinstance(response, HITLDetailResponse)
    return response
