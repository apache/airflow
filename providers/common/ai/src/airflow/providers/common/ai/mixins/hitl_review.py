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
import time
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol

from pydantic import BaseModel

from airflow.providers.common.ai.exceptions import HITLMaxIterationsError
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_ACTION,
    AgentSessionData,
    HumanActionData,
    SessionStatus,
)

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.sdk import Context


class HITLReviewProtocol(Protocol):
    """Attributes that the host operator must provide."""

    enable_hitl_review: bool
    max_hitl_iterations: int
    hitl_timeout: timedelta | None
    hitl_poll_interval: float
    prompt: str
    task_id: str
    log: Any


class HITLReviewMixin:
    """
    Mixin that drives an iterative HITL review loop inside ``execute()``.

    After the operator generates its first output, the mixin:

    1. Pushes session metadata and the first agent output to XCom.
    2. Polls the human action XCom (``airflow_hitl_review_human_action``) at ``hitl_poll_interval`` seconds.
    3. When a human sets action to ``changes_requested`` (via the plugin API),
       calls :meth:`regenerate_with_feedback` and pushes the new agent output.
    4. When a human sets action to ``approved``, returns the output.
    5. When a human sets action to ``rejected``, raises a `HITLRejectException`

    The loop stops after ``hitl_timeout`` or ``max_hitl_iterations``.

    **Max iterations:** ``iteration`` counts outputs shown to the reviewer
    (1 = initial, 2 = first regeneration, etc.). When the reviewer requests
    changes at ``iteration >= max_hitl_iterations``, the mixin raises
    ``HITLMaxIterationsError`` without calling the LLM. With
    ``max_hitl_iterations=5``, the reviewer can request changes at most 4
    times (iterations 1–4); the fifth output is the last chance to approve or
    reject.

    All agent outputs and human feedback are persisted as iteration-keyed
    XCom entries (``airflow_hitl_review_agent_output_1``, ``airflow_hitl_review_human_feedback_1``, etc.)
    for full auditability.

    Operators using this mixin must set:

    - ``enable_hitl_review`` (``bool``)
    - ``hitl_timeout`` (``timedelta | None``)
    - ``hitl_poll_interval`` (``float``, seconds)
    - ``prompt`` (``str``)

    And must implement: meth:`regenerate_with_feedback`.
    """

    def run_hitl_review(
        self: HITLReviewProtocol,
        context: Context,
        output: Any,
        *,
        message_history: Any = None,
    ) -> str:
        """
        Execute the full HITL review loop.

        :param context: Airflow task context.
        :param output: Initial LLM output (str or BaseModel).
        :param message_history: Provider-specific conversation state (e.g.
            pydantic-ai ``list[ModelMessage]``).  Passed to
            :meth:`regenerate_with_feedback` on each iteration.
        :returns: The final approved output as a string.
        :raises HITLMaxIterationsError: When max iterations reached without approval.
        :raises HITLRejectException: When the reviewer rejects the output.
        :raises HITLTimeoutError: When hitl_timeout elapses with no response.
        """
        output_str = self._to_string(output)  # type: ignore[attr-defined]
        ti = context["task_instance"]

        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            max_iterations=self.max_hitl_iterations,
            prompt=self.prompt,
            current_output=output_str,
        )

        ti.xcom_push(key=XCOM_AGENT_SESSION, value=session.model_dump(mode="json"))
        ti.xcom_push(key=f"{XCOM_AGENT_OUTPUT_PREFIX}1", value=output_str)

        self.log.info(
            "Feedback session created for %s/%s/%s (poll every %ds).",
            ti.dag_id,
            ti.run_id,
            ti.task_id,
            self.hitl_poll_interval,
        )

        deadline = time.monotonic() + self.hitl_timeout.total_seconds() if self.hitl_timeout else None

        return self._poll_loop(  # type: ignore[attr-defined]
            ti=ti,
            session=session,
            message_history=message_history,
            deadline=deadline,
        )

    def _poll_loop(
        self: HITLReviewProtocol,
        *,
        ti: Any,
        session: AgentSessionData,
        message_history: Any,
        deadline: float | None,
    ) -> str:
        """
        Block until the session reaches a terminal state.

        This loops until the human approves, rejects, or the timeout/max iterations is reached.
        """
        from airflow.providers.standard.exceptions import (
            HITLRejectException,
            HITLTimeoutError,
        )

        last_seen_iteration = 0
        first_poll = True

        while True:
            if deadline is not None and time.monotonic() > deadline:
                _session_timeout = AgentSessionData(
                    status=SessionStatus.TIMEOUT_EXCEEDED,
                    iteration=session.iteration,
                    max_iterations=session.max_iterations,
                    prompt=session.prompt,
                    current_output=session.current_output,
                )
                ti.xcom_push(key=XCOM_AGENT_SESSION, value=_session_timeout.model_dump(mode="json"))
                raise HITLTimeoutError("Task exceeded timeout.")

            if not first_poll:
                time.sleep(self.hitl_poll_interval)
            first_poll = False

            try:
                action_raw = ti.xcom_pull(
                    key=XCOM_HUMAN_ACTION, task_ids=ti.task_id, map_indexes=ti.map_index
                )
            except Exception:
                self.log.warning("Failed to pull XCom", exc_info=True)
                continue

            if action_raw is None:
                # Human action may take some time to propagate; it must be performed in the UI,
                # after which the plugin updates XCom with this XCOM_HUMAN_ACTION. Until then,
                # continue looping.
                continue

            try:
                if isinstance(action_raw, str):
                    action = HumanActionData.model_validate_json(action_raw)
                else:
                    action = HumanActionData.model_validate(action_raw)
            except Exception:
                self.log.warning("Malformed human action XCom: %r", action_raw)
                continue

            if action.iteration <= last_seen_iteration:
                continue

            last_seen_iteration = action.iteration

            if action.action == "approve":
                self.log.info("Output approved at iteration %d.", session.iteration)
                return session.current_output

            if action.action == "reject":
                raise HITLRejectException(f"Output rejected at iteration {session.iteration}.")

            if action.action == "changes_requested":
                feedback_text = action.feedback or ""
                if not feedback_text:
                    self.log.info("Empty feedback with 'Request Changes' — treating as approve.")
                    return session.current_output

                if session.iteration >= self.max_hitl_iterations:
                    _session_max = AgentSessionData(
                        status=SessionStatus.MAX_ITERATIONS_EXCEEDED,
                        iteration=session.iteration,
                        max_iterations=self.max_hitl_iterations,
                        prompt=self.prompt,
                        current_output=session.current_output,
                    )
                    ti.xcom_push(key=XCOM_AGENT_SESSION, value=_session_max.model_dump(mode="json"))
                    raise HITLMaxIterationsError("Task exceeded max iterations.")

                self.log.info(
                    "Feedback received (iteration %d): %s",
                    session.iteration,
                    feedback_text,
                )

                new_output, message_history = self.regenerate_with_feedback(  # type: ignore[attr-defined]
                    feedback=feedback_text,
                    message_history=message_history,
                )

                new_iteration = session.iteration + 1

                session = AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=new_iteration,
                    max_iterations=self.max_hitl_iterations,
                    prompt=self.prompt,
                    current_output=new_output,
                )

                ti.xcom_push(key=XCOM_AGENT_SESSION, value=session.model_dump(mode="json"))
                ti.xcom_push(key=f"{XCOM_AGENT_OUTPUT_PREFIX}{new_iteration}", value=new_output)

                self.log.info("Regenerated output (iteration %d) — waiting for review.", new_iteration)

    def regenerate_with_feedback(self, *, feedback: str, message_history: Any) -> tuple[str, Any]:
        """
        Re-run the LLM with reviewer feedback.

        :param feedback: The reviewer's feedback text.
        :param message_history: Provider-specific conversation state.
        :returns: ``(new_output_string, updated_message_history)``.
        """
        raise NotImplementedError(
            "Operators using HITLReviewMixin must implement regenerate_with_feedback()."
        )

    @staticmethod
    def _to_string(output: Any) -> str:
        if isinstance(output, BaseModel):
            return output.model_dump_json()
        if not isinstance(output, str):
            return str(output)
        return output
