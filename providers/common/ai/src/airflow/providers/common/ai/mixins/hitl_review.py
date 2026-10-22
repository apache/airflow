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

import hashlib
import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, NoReturn, Protocol

from airflow.providers.common.ai.exceptions import HITLMaxIterationsError
from airflow.providers.common.ai.utils.hitl_review import (
    HITL_FEEDBACK_PARAM,
    HITL_OPTION_APPROVE,
    HITL_OPTION_REJECT,
    HITL_OPTION_REQUEST_CHANGES,
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_ACTION,
    XCOM_HUMAN_FEEDBACK_PREFIX,
    AgentSessionData,
    HumanActionData,
    SessionStatus,
)
from airflow.providers.common.ai.utils.output_type import dump_output_for_display
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS
from airflow.providers.standard.exceptions import (
    HITLRejectException,
    HITLTimeoutError,
    HITLTriggerEventError,
)

if AIRFLOW_V_3_3_PLUS:
    # Airflow 3.3+ parks the task in AWAITING_INPUT between review rounds, so a review that takes
    # hours holds no worker or pool slot. Older Airflow versions poll XCom inside execute().
    from airflow.sdk.exceptions import TaskAwaitingInput
    from airflow.sdk.execution_time.context import NEVER_EXPIRE
    from airflow.sdk.execution_time.hitl import upsert_hitl_detail
    from airflow.sdk.timezone import utcnow

log = logging.getLogger(__name__)

# ``hitl_timeout`` may be ``None``, so a persistently failing poll would otherwise loop forever.
_MAX_CONSECUTIVE_XCOM_PULL_FAILURES = 10

# Task state store key holding what the operator needs to regenerate after the task resumes
# (the conversation so far), written by ``dump_review_state`` on every round.
_REVIEW_STATE_KEY = "common_ai_hitl_review_state"
# How long the review state outlives a timed review, so a resume that runs late still finds it.
_REVIEW_STATE_RETENTION_MARGIN = timedelta(days=1)

if TYPE_CHECKING:
    from airflow.sdk import Context


def _read_review_state(context: Context, state_sha256: str) -> str:
    """Return the review state saved before the park, refusing one that is gone or changed."""
    state = context["task_state_store"].get(_REVIEW_STATE_KEY)
    if not isinstance(state, str) or hashlib.sha256(state.encode()).hexdigest() != state_sha256:
        raise HITLTriggerEventError(
            {
                "error": "The saved review conversation is missing or changed while the task waited.",
                "error_type": "validation",
            }
        )
    return state


def _delete_review_state(context: Context, logger: logging.Logger) -> None:
    # Best-effort: failing the task over its cleanup would be worse, and the row expires anyway.
    try:
        context["task_state_store"].delete(_REVIEW_STATE_KEY)
    except Exception:
        logger.warning("Could not delete the saved HITL review conversation", exc_info=True)


@contextmanager
def _review_state_dropped_unless_parked(context: Context, logger: logging.Logger) -> Iterator[None]:
    """Delete the saved conversation when the review ends any way but parking for the next round."""
    try:
        yield
    except TaskAwaitingInput:
        raise
    except BaseException:
        # The conversation can hold tool results, and nothing will resume to read it.
        _delete_review_state(context, logger)
        raise
    else:
        _delete_review_state(context, logger)


def _pull_session(ti: Any) -> AgentSessionData:
    raw = ti.xcom_pull(key=XCOM_AGENT_SESSION, task_ids=ti.task_id, map_indexes=ti.map_index)
    if isinstance(raw, str):
        return AgentSessionData.model_validate_json(raw)
    return AgentSessionData.model_validate(raw)


def _push_session(ti: Any, session: AgentSessionData, status: SessionStatus) -> None:
    ended = session.model_copy(update={"status": status})
    ti.xcom_push(key=XCOM_AGENT_SESSION, value=ended.model_dump(mode="json"))


class HITLReviewProtocol(Protocol):
    """The host operator's attributes and hooks, and the mixin methods that call each other."""

    enable_hitl_review: bool
    max_hitl_iterations: int
    hitl_timeout: timedelta | None
    hitl_poll_interval: float
    prompt: str
    task_id: str
    log: Any

    def regenerate_with_feedback(self, *, feedback: str, message_history: Any) -> tuple[str, Any]: ...

    def dump_review_state(self, message_history: Any) -> str: ...

    def load_review_state(self, context: Context, state: str) -> Any: ...

    def complete_hitl_review(self, context: Context, approved_output: str, *, state: str | None) -> Any: ...

    def _parks_between_rounds(self) -> bool: ...

    def _start_review_round(
        self, context: Context, session: AgentSessionData, *, message_history: Any, deadline: datetime | None
    ) -> NoReturn: ...

    def _park_for_review(
        self, context: Context, session: AgentSessionData, *, deadline: datetime | None, state_sha256: str
    ) -> NoReturn: ...


class HITLReviewMixin:
    """
    Mixin that drives an iterative HITL review after the operator generates its first output.

    Each round, the mixin pushes session metadata and the agent output to XCom and waits for the
    reviewer to act in the ``hitl_review`` plugin:

    - ``changes_requested``: calls :meth:`regenerate_with_feedback` and starts the next round.
    - ``approved``: returns the output.
    - ``rejected``: raises ``HITLRejectException``.

    **How it waits:** on Airflow 3.3+ each round is also a Human-in-the-loop request on the task
    instance, and the task parks in the ``awaiting_input`` state, holding no worker or pool slot.
    The plugin answers the request and resumes the task in :meth:`resume_hitl_review`, which runs
    the next round in a new process; the conversation needed to regenerate is kept in the task
    state store through :meth:`dump_review_state` and :meth:`load_review_state`. On older Airflow
    the task stays running and polls the human action XCom (``airflow_hitl_review_human_action``)
    every ``hitl_poll_interval`` seconds.

    The review stops after ``hitl_timeout``, one wall-clock budget for all rounds combined, or
    ``max_hitl_iterations``. When polling, it also stops, by re-raising the underlying exception,
    when pulling the human action XCom fails ``_MAX_CONSECUTIVE_XCOM_PULL_FAILURES`` times in a row,
    even if ``hitl_timeout`` is ``None``.

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

    And must implement :meth:`regenerate_with_feedback`. To park between rounds on Airflow 3.3+,
    it also implements :meth:`dump_review_state` and :meth:`load_review_state`; without them the
    review polls, as on older Airflow.
    """

    def _parks_between_rounds(self) -> bool:
        """Whether the review parks between rounds; it needs Airflow 3.3+ and a saved conversation."""
        return AIRFLOW_V_3_3_PLUS and type(self).dump_review_state is not HITLReviewMixin.dump_review_state

    def run_hitl_review(
        self: HITLReviewProtocol,
        context: Context,
        output: Any,
        *,
        message_history: Any = None,
    ) -> str:
        """
        Start the HITL review of *output*.

        On Airflow 3.3+ this parks the task and never returns; the review continues in
        :meth:`resume_hitl_review`. Otherwise it blocks until the review ends.

        :param context: Airflow task context.
        :param output: Initial LLM output (any pydantic-serializable value).
        :param message_history: Provider-specific conversation state (e.g.
            pydantic-ai ``list[ModelMessage]``).  Passed to
            :meth:`regenerate_with_feedback` on each iteration.
        :returns: The approved output as a string (JSON for non-str values pydantic can serialize).
        :raises TaskAwaitingInput: When the review parks the task until the reviewer acts.
        :raises HITLMaxIterationsError: When max iterations reached without approval.
        :raises HITLRejectException: When the reviewer rejects the output.
        :raises HITLTimeoutError: When hitl_timeout elapses with no response.
        :raises Exception: The last XCom pull exception, when pulling the human action
            fails ``_MAX_CONSECUTIVE_XCOM_PULL_FAILURES`` times in a row.
        """
        output_str = self._to_string(output)  # type: ignore[attr-defined]
        ti = context["task_instance"]
        parks = self._parks_between_rounds()

        session = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            max_iterations=self.max_hitl_iterations,
            prompt=self.prompt,
            current_output=output_str,
            parked=parks,
        )

        if parks:
            self.log.info("Feedback session created for %s/%s/%s.", ti.dag_id, ti.run_id, ti.task_id)
            with _review_state_dropped_unless_parked(context, self.log):
                self._start_review_round(
                    context,
                    session,
                    message_history=message_history,
                    deadline=utcnow() + self.hitl_timeout if self.hitl_timeout else None,
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

    def _start_review_round(
        self: HITLReviewProtocol,
        context: Context,
        session: AgentSessionData,
        *,
        message_history: Any,
        deadline: datetime | None,
    ) -> NoReturn:
        """Save the conversation, show ``session`` to the reviewer, and park until they act."""
        state = self.dump_review_state(message_history)
        retention = (
            deadline - utcnow() + _REVIEW_STATE_RETENTION_MARGIN if deadline is not None else NEVER_EXPIRE
        )
        context["task_state_store"].set(_REVIEW_STATE_KEY, state, retention=retention)
        self._park_for_review(
            context, session, deadline=deadline, state_sha256=hashlib.sha256(state.encode()).hexdigest()
        )

    def _park_for_review(
        self: HITLReviewProtocol,
        context: Context,
        session: AgentSessionData,
        *,
        deadline: datetime | None,
        state_sha256: str,
    ) -> NoReturn:
        """
        Open the Human-in-the-loop request for ``session``'s round and park the task on it.

        The request is written before the session XCom, so the plugin, which accepts an action only
        while the session is ``pending_review``, always has a request to answer. An answer that
        lands before the task has parked is not lost: the park finds it and resumes straight away.
        """
        ti = context["task_instance"]
        timeout = None
        if deadline is not None:
            timeout = deadline - utcnow()
            if timeout <= timedelta(0):
                # Regeneration used up what was left of hitl_timeout.
                _push_session(ti, session, SessionStatus.TIMEOUT_EXCEEDED)
                raise HITLTimeoutError("Task exceeded timeout.")

        # The same subject, body, options and params every round: on Airflow 3.3 a repeat
        # request keeps the first round's, so nothing here may go stale. The output is read in
        # the HITL Review tab, which also shows the earlier rounds.
        upsert_hitl_detail(
            ti_id=ti.id,
            options=[HITL_OPTION_APPROVE, HITL_OPTION_REQUEST_CHANGES, HITL_OPTION_REJECT],
            subject=f"Review the output of task `{self.task_id}`",
            body=(
                "Read the output and answer in the **HITL Review** tab of this task instance, "
                "which shows every round of the review. Choosing *Request changes* here needs "
                "feedback for the model."
            ),
            defaults=None,
            multiple=False,
            params={
                HITL_FEEDBACK_PARAM: {
                    "value": "",
                    "description": "What the model should change (with Request changes).",
                    "schema": {"type": ["string", "null"]},
                },
            },
        )
        ti.xcom_push(key=XCOM_AGENT_SESSION, value=session.model_dump(mode="json"))
        ti.xcom_push(key=f"{XCOM_AGENT_OUTPUT_PREFIX}{session.iteration}", value=session.current_output)
        self.log.info("Waiting for review of iteration %d.", session.iteration)
        raise TaskAwaitingInput(
            method_name="resume_hitl_review",
            kwargs={
                "iteration": session.iteration,
                "deadline": deadline.isoformat() if deadline is not None else None,
                "state_sha256": state_sha256,
            },
            timeout=timeout,
        )

    def resume_hitl_review(
        self: HITLReviewProtocol,
        context: Context,
        iteration: int,
        deadline: str | None,
        state_sha256: str,
        event: dict[str, Any],
    ) -> Any:
        """
        Continue a review parked by :meth:`run_hitl_review` with the reviewer's answer.

        Approving returns :meth:`complete_hitl_review`; requesting changes regenerates and parks
        the task again for the next round.

        :raises HITLRejectException: When the reviewer rejects the output.
        :raises HITLMaxIterationsError: When changes are requested on the last allowed iteration.
        :raises HITLTimeoutError: When ``hitl_timeout`` elapsed before the reviewer answered.
        :raises HITLTriggerEventError: When the resume event reports another error.
        """
        with _review_state_dropped_unless_parked(context, self.log):
            ti = context["task_instance"]
            session = _pull_session(ti)
            if session.iteration != iteration:
                raise HITLTriggerEventError(
                    {
                        "error": f"The review session is at iteration {session.iteration}, but the task "
                        f"resumed from iteration {iteration}.",
                        "error_type": "validation",
                    }
                )

            if "error" in event:
                if event.get("error_type") == "timeout":
                    _push_session(ti, session, SessionStatus.TIMEOUT_EXCEEDED)
                    raise HITLTimeoutError("Task exceeded timeout.")
                raise HITLTriggerEventError(event)

            # An answer given on the Required Actions page reaches only the worker, leaving the
            # session pending; the plugin records its own answers itself. Recording the answer
            # before anything slow makes the plugin refuse a second one for this round.
            answered_elsewhere = session.status == SessionStatus.PENDING_REVIEW
            chosen = event.get("chosen_options") or []

            if HITL_OPTION_APPROVE in chosen:
                self.log.info("Output approved at iteration %d.", iteration)
                if answered_elsewhere:
                    _push_session(ti, session, SessionStatus.APPROVED)
                return self.complete_hitl_review(
                    context, session.current_output, state=_read_review_state(context, state_sha256)
                )

            if HITL_OPTION_REJECT in chosen:
                if answered_elsewhere:
                    _push_session(ti, session, SessionStatus.REJECTED)
                raise HITLRejectException(f"Output rejected at iteration {iteration}.")

            if HITL_OPTION_REQUEST_CHANGES not in chosen:
                raise HITLTriggerEventError(
                    {"error": f"Unexpected review answer {chosen!r}.", "error_type": "validation"}
                )

            parsed_deadline = datetime.fromisoformat(deadline) if deadline is not None else None
            feedback = (event.get("params_input") or {}).get(HITL_FEEDBACK_PARAM)
            feedback = feedback.strip() if isinstance(feedback, str) else ""
            if not feedback:
                # The plugin refuses empty feedback; the Required Actions form does not. Approving
                # would surprise a reviewer who asked for changes, so ask again for the same round.
                self.log.warning("Changes were requested without feedback; waiting for review again.")
                session.status = SessionStatus.PENDING_REVIEW
                self._park_for_review(context, session, deadline=parsed_deadline, state_sha256=state_sha256)

            if answered_elsewhere:
                # The HITL Review tab shows the feedback from this XCom.
                ti.xcom_push(key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}{iteration}", value=feedback)
            if iteration >= self.max_hitl_iterations:
                _push_session(ti, session, SessionStatus.MAX_ITERATIONS_EXCEEDED)
                raise HITLMaxIterationsError("Task exceeded max iterations.")
            if answered_elsewhere:
                _push_session(ti, session, SessionStatus.CHANGES_REQUESTED)

            message_history = self.load_review_state(context, _read_review_state(context, state_sha256))
            self.log.info("Feedback received (iteration %d): %s", iteration, feedback)
            new_output, message_history = self.regenerate_with_feedback(
                feedback=feedback,
                message_history=message_history,
            )
            next_session = session.model_copy(
                update={
                    "status": SessionStatus.PENDING_REVIEW,
                    "iteration": iteration + 1,
                    "current_output": new_output,
                }
            )
            self.log.info("Regenerated output (iteration %d).", next_session.iteration)
            self._start_review_round(
                context, next_session, message_history=message_history, deadline=parsed_deadline
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

        This loops until the human approves, rejects, the timeout/max iterations is reached,
        or pulling the human action XCom fails ``_MAX_CONSECUTIVE_XCOM_PULL_FAILURES`` times in a row.
        """
        last_seen_iteration = 0
        first_poll = True
        consecutive_pull_failures = 0

        while True:
            if deadline is not None and time.monotonic() > deadline:
                _push_session(ti, session, SessionStatus.TIMEOUT_EXCEEDED)
                raise HITLTimeoutError("Task exceeded timeout.")

            if not first_poll:
                time.sleep(self.hitl_poll_interval)
            first_poll = False

            try:
                action_raw = ti.xcom_pull(
                    key=XCOM_HUMAN_ACTION, task_ids=ti.task_id, map_indexes=ti.map_index
                )
            except Exception:
                consecutive_pull_failures += 1
                if consecutive_pull_failures >= _MAX_CONSECUTIVE_XCOM_PULL_FAILURES:
                    self.log.error(
                        "Giving up HITL review after %d consecutive XCom pull failures",
                        consecutive_pull_failures,
                    )
                    raise
                self.log.warning("Failed to pull XCom", exc_info=True)
                continue
            consecutive_pull_failures = 0

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
                    _push_session(ti, session, SessionStatus.MAX_ITERATIONS_EXCEEDED)
                    raise HITLMaxIterationsError("Task exceeded max iterations.")

                self.log.info(
                    "Feedback received (iteration %d): %s",
                    session.iteration,
                    feedback_text,
                )

                new_output, message_history = self.regenerate_with_feedback(
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

    def dump_review_state(self, message_history: Any) -> str:
        """
        Serialize what :meth:`load_review_state` needs to regenerate after the task resumes.

        Called on Airflow 3.3+ before every park; the string is kept in the task state store.

        :param message_history: Provider-specific conversation state.
        """
        raise NotImplementedError("Operators using HITLReviewMixin must implement dump_review_state().")

    def load_review_state(self, context: Context, state: str) -> Any:
        """
        Restore what :meth:`dump_review_state` saved, in the resumed task.

        Called before each regeneration.

        :returns: The ``message_history`` to pass to :meth:`regenerate_with_feedback`.
        """
        raise NotImplementedError("Operators using HITLReviewMixin must implement load_review_state().")

    def complete_hitl_review(self, context: Context, approved_output: str, *, state: str | None) -> Any:
        """
        Return the task's result once the review is approved.

        :param approved_output: The approved output as the reviewer read it.
        :param state: What :meth:`dump_review_state` saved for the approved round, when the task
            resumed after parking; ``None`` when the review ran in this process.
        """
        return approved_output

    @staticmethod
    def _to_string(output: Any) -> str:
        """Serialize an output for the reviewer to read as JSON, or ``str()`` when pydantic cannot."""
        return dump_output_for_display(output)
