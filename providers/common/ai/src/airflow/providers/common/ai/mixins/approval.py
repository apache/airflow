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
from datetime import timedelta
from logging import Logger
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel

from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context

class ApprovalFailedException(AirflowException):
    """Failed to approve"""
    pass

class ApprovalRejectionException(AirflowException):
    """Rejected by the reviewer"""

class DeferForApprovalProtocol(Protocol):
    approval_timeout: timedelta | None
    allow_modifications: bool
    prompt: str
    task_id: str
    defer: Any

class ExecuteCompleteProtocol(Protocol):
    allow_modifications: bool
    log: Logger


class LLMApprovalMixin:
    """
    Mixin that pauses an operator for human review before returning output.
    """

    APPROVE = "Approve"
    REJECT = "Reject"

    def defer_for_approval(
        self: DeferForApprovalProtocol,
        context: Context,
        output: Any,
        *,
        subject: str | None = None,
        body: str | None = None,
        params: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        """
        Write HITL detail, then defer to HITLTrigger for human review.

        :param context: Airflow task context.
        :param output: The generated output to present for review.
        :param subject: Headline showed on the Required Actions page.
            Defaults to a generic message including the task ID.
        :param body: Markdown body shown below the headline.
            Defaults to the prompt and output wrapped in a code block.
        :param params: Editable param definitions for the reviewer form.
            Only used when ``allow_modifications=True``. When ``None``,
            defaults to a single ``"output"`` text field.
        """

        from airflow.providers.standard.triggers.hitl import HITLTrigger
        from airflow.sdk.execution_time.hitl import upsert_hitl_detail
        from airflow.sdk.timezone import utcnow

        if isinstance(output, BaseModel):
            output = output.model_dump()
        else:
            # Always make string output so that when comparing in the execute_complete matches
            output = str(output)


        ti_id = context["task_instance"].id
        timeout_datetime = utcnow() + self.approval_timeout if self.approval_timeout else None

        if subject is None:
            subject = f"Review output for task `{self.task_id}`"

        if body is None:
            body = f"```\nPrompt: {self.prompt}\n\n{output}\n```"

        hitl_params: dict[str, dict[str, Any]] = {}
        if self.allow_modifications:
            if params is not None:

                # making it standard always so that in the execute_complete we can look for this field and return them.
                if not params.get("output"):
                    raise ValueError(
                        "When `allow_modifications=True`, `params` must contain an `output` field."
                    )

                hitl_params = params
            else:
                hitl_params = {
                    "output": {
                        "value": output,
                        "description": "Edit the output before approving (optional).",
                        "schema": {"type": "string"},
                    },
                }

        upsert_hitl_detail(
            ti_id=ti_id,
            options=[LLMApprovalMixin.APPROVE, LLMApprovalMixin.REJECT],
            subject=subject,
            body=body,
            defaults=None,
            multiple=False,
            params=hitl_params,
        )

        self.defer(
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=[LLMApprovalMixin.APPROVE, LLMApprovalMixin.REJECT],
                defaults=None,
                params=hitl_params,
                multiple=False,
                timeout_datetime=timeout_datetime,
            ),
            method_name="execute_complete",
            kwargs={"generated_output": output, "allow_modifications": self.allow_modifications},
            timeout=self.approval_timeout,
        )

    def execute_complete(self, context: Context, generated_output: str, allow_modifications: bool, event: dict[str, Any]) -> str:
        """Resume after human review."""

        if "error" in event:
            error_type = event.get("error_type", "unknown")
            if error_type == "timeout":
                raise TimeoutError(f"Approval timed out: {event['error']}")
            raise ApprovalFailedException(f"Approval failed: {event['error']}")

        responded_by_user = event.get("responded_by_user")
        chosen = event.get("chosen_options", [])
        if self.APPROVE not in chosen:
            raise ApprovalRejectionException(f"Output was rejected by the reviewer {responded_by_user}.")

        output = generated_output
        params_input: dict[str, Any] = event.get("params_input") or {}

        if allow_modifications and params_input:
            modified = params_input.get("output")
            if not modified:
                logging.info("No output modified by the reviewer=%s", responded_by_user)
                return output

            if modified != generated_output:
                logging.info("output=%s modified by the reviewer=%s ", modified, responded_by_user)
                return modified

        return output
