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

import uuid
from collections.abc import MutableMapping
from typing import TYPE_CHECKING

from airflow.providers.standard.api_fastapi.execution_api.datamodels.hitl import (
    HITLResponseContentDetail,
)
from airflow.providers.standard.execution_time.comms import (
    CreateHITLResponsePayload,
    HITLInputRequestResponseResult,
    UpdateHITLResponse,
)

if TYPE_CHECKING:
    from airflow.sdk.api.client import Client


class HITLOperations:
    """
    Operations related to Human in the loop. Require Airflow 3.1+.

    :meta: private
    """

    __slots__ = ("client",)

    def __init__(self, client: Client) -> None:
        self.client = client

    def add_response(
        self,
        *,
        ti_id: uuid.UUID,
        options: list[str],
        subject: str,
        body: str | None = None,
        default: list[str] | None = None,
        multiple: bool = False,
        params: MutableMapping | None = None,
    ) -> HITLInputRequestResponseResult:
        """Add a Human-in-the-loop response that waits for human response for a specific Task Instance."""
        payload = CreateHITLResponsePayload(
            ti_id=ti_id,
            options=options,
            subject=subject,
            body=body,
            default=default,
            multiple=multiple,
            params=params,
        )
        resp = self.client.post(
            f"/hitl-responses/{ti_id}",
            content=payload.model_dump_json(),
        )
        return HITLInputRequestResponseResult.model_validate_json(resp.read())

    def update_response(
        self,
        *,
        ti_id: uuid.UUID,
        response_content: str,
        params_input: MutableMapping | None = None,
    ) -> HITLResponseContentDetail:
        """Update an existing Human-in-the-loop response."""
        payload = UpdateHITLResponse(
            ti_id=ti_id,
            response_content=response_content,
            params_input=params_input,
        )
        resp = self.client.patch(
            f"/hitl-responses/{ti_id}",
            content=payload.model_dump_json(),
        )
        return HITLResponseContentDetail.model_validate_json(resp.read())

    def get_response_content_detail(self, ti_id: uuid.UUID) -> HITLResponseContentDetail:
        """Get content part of a Human-in-the-loop response for a specific Task Instance."""
        resp = self.client.get(f"/hitl-responses/{ti_id}")
        return HITLResponseContentDetail.model_validate_json(resp.read())
