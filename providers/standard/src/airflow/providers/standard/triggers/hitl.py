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

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.standard.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    raise AirflowOptionalProviderFeatureException("Human in the loop functionality needs Airflow 3.1+.")

import asyncio
from collections.abc import AsyncIterator
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, TypedDict
from uuid import UUID

from asgiref.sync import sync_to_async

from airflow.exceptions import ParamValidationError
from airflow.sdk import Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.sdk.execution_time.hitl import (
    HITLUser,
    get_hitl_detail_content_detail,
    update_hitl_detail_response,
)
from airflow.sdk.timezone import utcnow
from airflow.triggers.base import BaseTrigger, TriggerEvent


class HITLTriggerEventSuccessPayload(TypedDict, total=False):
    """Minimum required keys for a success Human-in-the-loop TriggerEvent."""

    chosen_options: list[str]
    params_input: dict[str, dict[str, Any]]
    responded_by_user: HITLUser | None
    responded_at: datetime
    timedout: bool


class HITLTriggerEventFailurePayload(TypedDict):
    """Minimum required keys for a failed Human-in-the-loop TriggerEvent."""

    error: str
    error_type: Literal["timeout", "unknown", "validation"]


class HITLTrigger(BaseTrigger):
    """A trigger that checks whether Human-in-the-loop responses are received."""

    def __init__(
        self,
        *,
        ti_id: UUID,
        options: list[str],
        params: dict[str, dict[str, Any]],
        defaults: list[str] | None = None,
        multiple: bool = False,
        timeout_datetime: datetime | None,
        poke_interval: float = 5.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.ti_id = ti_id
        self.poke_interval = poke_interval

        self.options = options
        self.multiple = multiple
        self.defaults = defaults
        self.timeout_datetime = timeout_datetime

        self.params = ParamsDict(
            {
                k: Param(
                    v.pop("value"),
                    **v,
                )
                if HITLTrigger._is_param(v)
                else Param(v)
                for k, v in params.items()
            },
        )

    @staticmethod
    def _is_param(value: Any) -> bool:
        return isinstance(value, dict) and all(key in value for key in ("description", "schema", "value"))

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize HITLTrigger arguments and classpath."""
        return (
            "airflow.providers.standard.triggers.hitl.HITLTrigger",
            {
                "ti_id": self.ti_id,
                "options": self.options,
                "defaults": self.defaults,
                "params": {k: self.params.get_param(k).serialize() for k in self.params},
                "multiple": self.multiple,
                "timeout_datetime": self.timeout_datetime,
                "poke_interval": self.poke_interval,
            },
        )

    async def _handle_timeout(self) -> TriggerEvent:
        """Handle HITL timeout logic and yield appropriate event."""
        resp = await sync_to_async(get_hitl_detail_content_detail)(ti_id=self.ti_id)

        # Case 1: Response arrived just before timeout
        if resp.response_received and resp.chosen_options:
            if TYPE_CHECKING:
                assert resp.responded_by_user is not None
                assert resp.responded_at is not None

            chosen_options_list = list(resp.chosen_options or [])
            self.log.info(
                "[HITL] responded_by=%s (id=%s) options=%s at %s (timeout fallback skipped)",
                resp.responded_by_user.name,
                resp.responded_by_user.id,
                chosen_options_list,
                resp.responded_at,
            )
            return TriggerEvent(
                HITLTriggerEventSuccessPayload(
                    chosen_options=chosen_options_list,
                    params_input=resp.params_input or {},
                    responded_at=resp.responded_at,
                    responded_by_user=HITLUser(
                        id=resp.responded_by_user.id,
                        name=resp.responded_by_user.name,
                    ),
                    timedout=False,
                )
            )

        # Case 2: No defaults defined → failure
        if self.defaults is None:
            return TriggerEvent(
                HITLTriggerEventFailurePayload(
                    error="The timeout has passed, and the response has not yet been received.",
                    error_type="timeout",
                )
            )

        # Case 3: Timeout fallback to default
        resp = await sync_to_async(update_hitl_detail_response)(
            ti_id=self.ti_id,
            chosen_options=self.defaults,
            params_input=self.params.dump(),
        )
        if TYPE_CHECKING:
            assert resp.responded_at is not None

        self.log.info(
            "[HITL] timeout reached before receiving response, fallback to default %s",
            self.defaults,
        )
        return TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=self.defaults,
                params_input=self.params.dump(),
                responded_by_user=None,
                responded_at=resp.responded_at,
                timedout=True,
            )
        )

    async def _handle_response(self):
        """Check if HITL response is ready and yield success if so."""
        resp = await sync_to_async(get_hitl_detail_content_detail)(ti_id=self.ti_id)
        if TYPE_CHECKING:
            assert resp.responded_by_user is not None
            assert resp.responded_at is not None

        if not (resp.response_received and resp.chosen_options):
            return None

        # validate input
        if params_input := resp.params_input:
            try:
                for key, value in params_input.items():
                    self.params[key] = value
            except ParamValidationError as err:
                return TriggerEvent(
                    HITLTriggerEventFailurePayload(
                        error=str(err),
                        error_type="validation",
                    )
                )

        chosen_options_list = list(resp.chosen_options or [])
        self.log.info(
            "[HITL] responded_by=%s (id=%s) options=%s at %s",
            resp.responded_by_user.name,
            resp.responded_by_user.id,
            chosen_options_list,
            resp.responded_at,
        )
        return TriggerEvent(
            HITLTriggerEventSuccessPayload(
                chosen_options=chosen_options_list,
                params_input=params_input or {},
                responded_at=resp.responded_at,
                responded_by_user=HITLUser(
                    id=resp.responded_by_user.id,
                    name=resp.responded_by_user.name,
                ),
                timedout=False,
            )
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Loop until the Human-in-the-loop response received or timeout reached."""
        while True:
            if self.timeout_datetime and self.timeout_datetime < utcnow():
                event = await self._handle_timeout()
                yield event
                return

            event = await self._handle_response()
            if event:
                yield event
                return

            await asyncio.sleep(self.poke_interval)
