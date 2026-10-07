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
"""Operators for invoking Databricks agents."""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any
from uuid import NAMESPACE_URL, UUID, uuid5

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.databricks.exceptions import (
    DatabricksAgentInvocationError,
    DatabricksAgentInvocationTimeout,
)
from airflow.providers.databricks.hooks.agent import DatabricksAgentHook
from airflow.providers.databricks.triggers.agent import DatabricksAgentInvocationTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class DatabricksAgentInvokeOperator(BaseOperator):
    """
    Invoke a DurableAgentServer on Databricks Apps and optionally wait for its result.

    :param app_url: HTTPS base URL of the deployed app. (templated)
    :param input: JSON-serializable agent input. (templated)
    :param databricks_conn_id: Databricks connection using service principal OAuth.
        Defaults to ``databricks_default``. (templated)
    :param session_id: Conversation ID. CLI template agents require this field.
        Defaults to ``None``. (templated)
    :param invocation_id: Idempotency UUID. Defaults to a stable UUID for this task instance,
        shared across retries and clears within the same Dag run. (templated)
    :param wait_for_termination: Wait for completion or interruption. If false, return the submission response.
        Defaults to ``True``.
    :param polling_period_seconds: Seconds between status checks. Defaults to ``10``.
    :param timeout: Maximum seconds to wait after submission. Timing out does not cancel the remote invocation.
        Defaults to ``3600``.
    :param deferrable: Release the worker while waiting. Defaults to ``[operators] default_deferrable``,
        with a fallback of ``False``.
    """

    template_fields: Sequence[str] = ("app_url", "input", "databricks_conn_id", "session_id", "invocation_id")
    template_fields_renderers = {"input": "json"}
    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    def __init__(
        self,
        *,
        app_url: str,
        input: Any,
        databricks_conn_id: str = "databricks_default",
        session_id: str | None = None,
        invocation_id: str | None = None,
        wait_for_termination: bool = True,
        polling_period_seconds: float = 10,
        timeout: float = 3600,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if polling_period_seconds <= 0 or timeout <= 0:
            raise ValueError("polling_period_seconds and timeout must be positive")
        self.app_url = app_url
        self.input = input
        self.databricks_conn_id = databricks_conn_id
        self.session_id = session_id
        self.invocation_id = invocation_id
        self.wait_for_termination = wait_for_termination
        self.polling_period_seconds = polling_period_seconds
        self.timeout = timeout
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> DatabricksAgentHook:
        return DatabricksAgentHook(self.app_url, self.databricks_conn_id)

    def _get_invocation_id(self, context: Context) -> str:
        if self.invocation_id is not None:
            return str(UUID(self.invocation_id))
        ti = context["ti"]
        identity = json.dumps([self.app_url, ti.dag_id, ti.task_id, ti.run_id, ti.map_index])
        return str(uuid5(NAMESPACE_URL, identity))

    def _get_result(self, result: dict[str, Any], invocation_id: str) -> dict[str, Any] | None:
        status = result.get("status")
        if status == "failed":
            raise DatabricksAgentInvocationError(
                f"Databricks agent invocation {invocation_id} failed; inspect the app logs"
            )
        if status in ("completed", "interrupted"):
            return result
        if not status:
            raise DatabricksAgentInvocationError(
                f"Databricks agent invocation {invocation_id} response is missing its status"
            )
        return None

    def execute(self, context: Context) -> dict[str, Any]:
        invocation_id = self._get_invocation_id(context)
        submitted = self.hook.invoke_agent(invocation_id, self.input, self.session_id)
        if not self.wait_for_termination:
            return submitted
        result = self._get_result(submitted, invocation_id) if submitted.get("status") else None
        if result is not None:
            return result
        if self.deferrable:
            self.defer(
                trigger=DatabricksAgentInvocationTrigger(
                    app_url=self.app_url,
                    invocation_id=invocation_id,
                    databricks_conn_id=self.databricks_conn_id,
                    session_id=self.session_id,
                    polling_period_seconds=self.polling_period_seconds,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.timeout),
                kwargs={"invocation_id": invocation_id},
            )
        deadline = time.monotonic() + self.timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DatabricksAgentInvocationTimeout(
                    f"Timed out waiting for Databricks agent invocation {invocation_id} after {self.timeout} seconds"
                )
            response = self.hook.get_invocation(invocation_id, self.session_id, timeout_seconds=remaining)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DatabricksAgentInvocationTimeout(
                    f"Timed out waiting for Databricks agent invocation {invocation_id} after {self.timeout} seconds"
                )
            result = self._get_result(response, invocation_id)
            if result is not None:
                return result
            time.sleep(min(self.polling_period_seconds, remaining))

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None, *, invocation_id: str
    ) -> dict[str, Any]:
        if (
            not event
            or event.get("invocation_id") != invocation_id
            or event.get("status") not in ("success", "error")
        ):
            raise DatabricksAgentInvocationError(
                f"Databricks agent invocation {invocation_id} trigger returned an invalid event"
            )
        if event["status"] == "error":
            error_type = event.get("error_type")
            if error_type not in ("api_error", "invalid_response", "unexpected_error"):
                error_type = "unexpected_error"
            raise DatabricksAgentInvocationError(
                f"Polling Databricks agent invocation {invocation_id} failed ({error_type}); inspect trigger logs"
            )
        result = self.hook.get_invocation(invocation_id, self.session_id)
        status = result.get("status")
        if status in ("completed", "interrupted"):
            return result
        if status == "failed":
            raise DatabricksAgentInvocationError(
                f"Databricks agent invocation {invocation_id} failed; inspect the app logs"
            )
        if not status:
            raise DatabricksAgentInvocationError(
                f"Databricks agent invocation {invocation_id} response is missing its status"
            )
        raise DatabricksAgentInvocationError(
            f"Databricks agent invocation {invocation_id} has not reached a terminal state"
        )
