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

import json
import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel, TypeAdapter

from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    # On Airflow 3.3+ the review parks the task in the first-class AWAITING_INPUT state instead
    # of deferring to a trigger. On older cores this name is absent and defer() is used.
    from airflow.sdk.exceptions import TaskAwaitingInput

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.sdk import Context


class DeferForApprovalProtocol(Protocol):
    """Protocol for defer for approval mixin."""

    approval_timeout: timedelta | None
    allow_modifications: bool
    prompt: str
    task_id: str
    defer: Any

    def validate_approval_prompt(self) -> None: ...


class LLMApprovalMixin:
    """
    Mixin that pauses an operator for human review before returning output.

    When ``require_approval=True`` on the operator, the generated output is
    presented to a human reviewer via the Airflow Human-in-the-Loop (HITL)
    interface.  The task waits (``awaiting_input`` on Airflow 3.3+, deferred on
    older versions) until the reviewer approves or rejects.

    If ``allow_modifications=True``, the reviewer can also edit the output
    before approving.  The (possibly modified) output is then returned as the
    task result.

    Operators that use this mixin must set the following attributes:

    - ``require_approval`` (``bool``)
    - ``allow_modifications`` (``bool``)
    - ``approval_timeout`` (``timedelta | None``)
    - ``prompt`` (``str``)
    """

    APPROVE = "Approve"
    REJECT = "Reject"

    def validate_approval_prompt(self: DeferForApprovalProtocol) -> None:
        """Fail fast when the prompt cannot be rendered as text in the approval review body."""
        if not isinstance(self.prompt, str):
            raise TypeError(
                f"{type(self).__name__}: require_approval=True is not supported "
                f"with a non-string prompt (got {type(self.prompt).__name__}). "
                "The approval review body renders the prompt as text; passing a "
                "Sequence[UserContent] would expose object reprs (and any embedded "
                "bytes) in the human review UI. Return a str prompt, or disable "
                "require_approval."
            )

    def defer_for_approval(
        self: DeferForApprovalProtocol,
        context: Context,
        output: Any,
        *,
        subject: str | None = None,
        body: str | None = None,
        modification_schema: dict[str, Any] | None = None,
    ) -> None:
        """
        Write HITL detail, then pause the task for human review.

        On Airflow 3.3+ the task parks in the ``awaiting_input`` state (no trigger or triggerer
        involved); on older versions it defers to :class:`HITLTrigger`. Either way it resumes in
        ``execute_complete`` once a response (or timeout default) arrives.

        :param context: Airflow task context.
        :param output: The generated output to present for review.
        :param subject: Headline shown on the Required Actions page.
            Defaults to ``"Review output for task `<task_id>`"``.
        :param body: Markdown body shown below the headline.
            Defaults to the prompt and output wrapped in a code block.
        :param modification_schema: JSON schema for the editable ``output`` param
            when ``allow_modifications=True``. Defaults to ``{"type": "string"}``.
            Pass e.g. ``{"type": "string", "enum": [...]}`` to render a dropdown
            of valid values in the review form, or ``{"type": "array", "items":
            {"type": "string", "enum": [...]}, "examples": [...]}`` to render a
            multi-select (JSON Schema forbids ``enum`` at the array level, so the
            options come from ``examples``); a list submitted by the reviewer is
            returned from ``execute_complete`` re-serialized as a compact JSON string.
        """
        from airflow.providers.standard.triggers.hitl import HITLTrigger
        from airflow.sdk.execution_time.hitl import upsert_hitl_detail
        from airflow.sdk.timezone import utcnow

        self.validate_approval_prompt()

        raw_output = output
        if isinstance(output, BaseModel):
            output = output.model_dump_json()
        elif not isinstance(output, str):
            # JSON round-trip: execute_complete validates the string back into output_type.
            output = TypeAdapter(type(output)).dump_json(output).decode()

        ti_id = context["task_instance"].id

        if subject is None:
            subject = f"Review output for task `{self.task_id}`"

        if body is None:
            body = f"```\nPrompt: {self.prompt}\n\n{output}\n```"

        hitl_params: dict[str, dict[str, Any]] = {}
        if self.allow_modifications:
            # The multi-select rendered for an array schema needs the list, not its JSON string
            param_value: Any = output
            if (
                modification_schema is not None
                and modification_schema.get("type") == "array"
                and isinstance(raw_output, list)
            ):
                param_value = raw_output
            hitl_params = {
                "output": {
                    "value": param_value,
                    "description": "Edit the output before approving (optional).",
                    "schema": modification_schema or {"type": "string"},
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

        if AIRFLOW_V_3_3_PLUS:
            # New core (3.3+): park the task in AWAITING_INPUT -- no trigger, no triggerer. The
            # task is resumed by the Core API response handler or the scheduler timeout sweep.
            raise TaskAwaitingInput(
                method_name="execute_complete",
                kwargs={"generated_output": output},
                timeout=self.approval_timeout,
            )

        # Fallback for cores < 3.3: defer the response check to HITLTrigger on the triggerer.
        self.defer(
            trigger=HITLTrigger(
                ti_id=ti_id,
                options=[LLMApprovalMixin.APPROVE, LLMApprovalMixin.REJECT],
                defaults=None,
                params=hitl_params,
                multiple=False,
                timeout_datetime=utcnow() + self.approval_timeout if self.approval_timeout else None,
            ),
            method_name="execute_complete",
            kwargs={"generated_output": output},
            timeout=self.approval_timeout,
        )

    def execute_complete(self, context: Context, generated_output: str, event: dict[str, Any]) -> str:
        """
        Resume after human review.

        Called automatically by Airflow when the HITL trigger fires.
        Returns the original or reviewer-modified output on approval.

        :param context: Airflow task context.
        :param generated_output: The output that was deferred for review.
        :param event: Trigger event payload containing ``chosen_options``,
            ``params_input``, and ``responded_by_user``.
        :raises HITLRejectException: If the reviewer rejected the output.
        :raises HITLTriggerEventError: If the trigger reported an error.
        :raises HITLTimeoutError: If the approval timed out.
        """
        from airflow.providers.standard.exceptions import (
            HITLRejectException,
            HITLTimeoutError,
            HITLTriggerEventError,
        )

        if "error" in event:
            error_type = event.get("error_type", "unknown")
            if error_type == "timeout":
                raise HITLTimeoutError(f"Approval timed out: {event['error']}")
            raise HITLTriggerEventError(event)

        responded_by_user = event.get("responded_by_user")
        chosen = event["chosen_options"]
        if self.APPROVE not in chosen:
            raise HITLRejectException(f"Output was rejected by the reviewer {responded_by_user}.")

        output = generated_output
        params_input: dict[str, Any] = event.get("params_input") or {}

        # Only accept modified output when the operator explicitly allows modifications.
        # Without this guard a reviewer could craft a request with params_input even
        # when allow_modifications=False, bypassing the read-only approval flow.
        if getattr(self, "allow_modifications", False) and params_input:
            modified = params_input.get("output")
            if "output" in params_input and modified is None:
                raise HITLTriggerEventError(
                    {
                        "error": "Modified output must not be empty; edit it or reject instead.",
                        "error_type": "validation",
                    }
                )
            if isinstance(modified, list):
                for item in modified:
                    if not isinstance(item, str):
                        raise HITLTriggerEventError(
                            {
                                "error": f"Modified output list items must be strings, "
                                f"got {type(item).__name__}.",
                                "error_type": "validation",
                            }
                        )
                # Compact so an unchanged selection compares equal to generated_output
                modified = json.dumps(modified, separators=(",", ":"))
            if modified is not None and not isinstance(modified, str):
                # On the awaiting_input path nothing upstream schema-validates params_input
                # (HITLTrigger did on the legacy path), so enforce the string contract here
                # rather than returning a non-string as the task's output.
                raise HITLTriggerEventError(
                    {
                        "error": f"Modified output must be a string or a list of strings, "
                        f"got {type(modified).__name__}.",
                        "error_type": "validation",
                    }
                )
            if modified is not None and modified != generated_output:
                log.info("output=%s modified by the reviewer=%s ", modified, responded_by_user)
                return modified

        return output
