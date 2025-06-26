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
    FetchHITLResponse,
    HITLResponseResult,
)


def add_input_request(
    ti_id: UUID,
    options: list[str],
    subject: str,
    body: str | None = None,
    default: str | None = None,
    params: MutableMapping | None = None,
    multiple: bool = False,
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
        )
    )


def get_hitl_response_content(ti_id: UUID) -> str | None:
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    response = SUPERVISOR_COMMS.send(msg=FetchHITLResponse(ti_id=ti_id))

    if TYPE_CHECKING:
        assert isinstance(response, HITLResponseResult)
    return response.content
