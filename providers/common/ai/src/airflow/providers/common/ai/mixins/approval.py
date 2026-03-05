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

from datetime import timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.sdk import Context


class LLMApprovalMixin:
    """
    Mixin that pauses an LLM operator for human review before returning output.
    """

    APPROVE = "Approve"
    REJECT = "Reject"
    allow_modifications: bool = False
    approval_timeout: timedelta | None = None
    prompt: str | None = None

    @staticmethod
    def _approval_subject(task_id: str | None = None) -> str:
        """Return the headline shown on the Required Actions page."""
        return f"Review LLM output for task '{task_id}'"


    def _approval_body(self) -> str:
        """Return the Markdown body shown below the headline."""
        return f"\n\nPrompt: {self.prompt}"

    @staticmethod
    def _approval_params(output: Any) -> dict[str, dict[str, Any]]:
        """Return editable param definitions for the reviewer form."""

        return {
            "output": {
                "value": str(output),
                "description": "Edit the output before approving (optional).",
                "schema": {"type": "string"},
            },
        }

    def defer_for_approval(self,
                           context: Context,
                           output: Any,
                           allow_modifications: bool = False
                           ) -> None:
        """Write HITL detail, then defer to HITLTrigger for human review."""

        from airflow.providers.standard.triggers.hitl import HITLTrigger
        from airflow.sdk.execution_time.hitl import upsert_hitl_detail
        from airflow.sdk.timezone import utcnow

        self.allow_modifications = allow_modifications
        ti_id = context["task_instance"].id
        timeout_datetime = utcnow() + self.approval_timeout if self.approval_timeout else None

        hitl_params: dict[str, dict[str, Any]] = {}
        if self.allow_modifications:
            hitl_params = self._approval_params(output)

        upsert_hitl_detail(
            ti_id=ti_id,
            options=[self.APPROVE, self.REJECT],
            subject=self._approval_subject(task_id=context["task_instance"].task_id),
            body=self._approval_body(),
            params=hitl_params,
        )

        self.defer(  # type: ignore[attr-defined]
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=[self.APPROVE, self.REJECT],
                params=hitl_params,
                timeout_datetime=timeout_datetime,
            ),
            method_name="execute_complete",
            kwargs={"generated_output": str(output)},
            timeout=self.approval_timeout,
        )

    def execute_complete(self, context: Context, generated_output: str, event: dict[str, Any]) -> str:
        """
        Resume after human review.
        """
        if "error" in event:
            error_type = event.get("error_type", "unknown")
            error = event.get("error")
            if error_type == "timeout":
                raise RuntimeError("Approval timed out: %s", error)
            raise RuntimeError(f"Approval failed: %s", error)

        chosen = event.get("chosen_options", [])
        approver_details = event.get("responded_by_user")
        if self.APPROVE not in chosen:
            raise RuntimeError("LLM output was rejected by the reviewer. %s", approver_details)

        output = generated_output
        params_input: dict[str, Any] = event.get("params_input") or {}
        if self.allow_modifications and params_input:
            return params_input.get("output")
        return output
