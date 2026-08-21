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

import time
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.anthropic.exceptions import AnthropicAgentSessionTimeout
from airflow.providers.anthropic.hooks.anthropic import (
    AnthropicHook,
    _create_session_error,
    build_budget,
    validate_execute_complete_event,
)
from airflow.providers.anthropic.triggers.agent import AnthropicAgentSessionTrigger
from airflow.providers.common.compat.sdk import BaseOperator, conf

if TYPE_CHECKING:
    from anthropic.types.beta import BetaManagedAgentsSession

    from airflow.providers.anthropic.hooks.anthropic import BudgetSpec
    from airflow.providers.common.compat.sdk import Context


class AnthropicAgentSessionOperator(BaseOperator):
    """
    Run a Managed Agents session against a pre-created agent and environment.

    Anthropic runs the agent loop server-side; the worker creates a session, sends the
    initial instruction, and waits for the session to reach a terminal status. In
    deferrable mode it releases the worker slot while a trigger polls the session status.

    Provide exactly one of ``message`` (a single user turn) or ``outcome`` (a
    ``user.define_outcome`` rubric that the agent iterates against until satisfied).

    .. important::
        This operator is for **autonomous** agents — configure the agent without
        client-side custom tools or an ``always_ask`` permission policy.

        Completion is detected accurately for both modes. A ``message`` run reads the
        terminal ``session.status_idle`` event's ``stop_reason`` (correlated against the
        kickoff event to avoid a start-race false positive): ``end_turn`` succeeds, while
        ``requires_action`` (the agent is blocked on input), ``retries_exhausted`` and
        ``budget_reached`` raise an error rather than silently passing. An ``outcome`` run
        is judged from the session's ``outcome_evaluations`` verdict (``satisfied`` vs.
        ``failed``/``max_iterations_reached``/``interrupted``).

        Agents and environments are created once (see
        :meth:`~airflow.providers.anthropic.hooks.anthropic.AnthropicHook.create_agent`),
        not per task run.

    Outputs the agent writes to ``/mnt/session/outputs/`` are retrieved afterwards via the
    Files API (``scope_id=<session_id>``); the operator returns the **session ID only**.

    The session's token counts and list cost are pushed to XCom under ``usage`` on both
    success and failure, so cost per Dag run can be queried. This matters because a budget is
    a stop trigger rather than a cap: the ceiling is checked between model requests, so it
    does not tell you what the session actually spent.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:AnthropicAgentSessionOperator`

    :param agent_id: ID of a pre-created agent.
    :param environment_id: ID of a pre-created environment.
    :param message: A single user message to start the session. Mutually exclusive with ``outcome``.
    :param outcome: A ``user.define_outcome`` payload (``description``, ``rubric``,
        optional ``max_iterations``). Mutually exclusive with ``message``.
    :param conn_id: The Anthropic connection ID to use.
    :param deferrable: Run the operator in deferrable mode.
    :param poll_interval: Seconds between session status checks (both paths).
    :param timeout: Seconds to wait for a terminal status. Defaults to 24 hours. In
        deferrable mode the trigger enforces this and tears the session down on timeout;
        set ``execution_timeout`` only for a shorter hard cap (which preempts that
        graceful teardown).
    :param vault_ids: Vault IDs providing MCP/credential access to the session.
    :param session_resources: Session resources (files, GitHub repos, memory stores). Named
        ``session_resources`` to avoid colliding with the reserved ``BaseOperator.resources``;
        forwarded to ``sessions.create`` as ``resources``.
    :param budget: Spend ceiling for the session. A number or numeric string is read as
        **US dollars** (``budget=25`` is $25.00); a mapping is passed through as the raw
        API payload. On a ``message`` run, a session that stops against it raises
        :class:`~airflow.providers.anthropic.exceptions.AnthropicSessionBudgetExceeded`.
        On an ``outcome`` run completion is judged from ``outcome_evaluations`` before the
        idle event is consulted, so a budget stop surfaces as the outcome verdict and the
        generic ``AnthropicAgentSessionError`` instead.

        .. warning::
            The ceiling is a stop trigger, not a cap. It is checked between model requests,
            so a request already in flight runs to completion and the session can finish
            well above the limit. Airflow ``retries`` also each start a new session with a
            fresh budget, so prefer ``retries=0`` when a budget is set.
    :param session_kwargs: Extra keyword arguments forwarded to ``sessions.create``. Setting
        ``budget`` here as well as via the ``budget`` argument is rejected.
    """

    template_fields: Sequence[str] = ("agent_id", "environment_id", "message", "outcome", "budget")

    def __init__(
        self,
        *,
        agent_id: str,
        environment_id: str,
        message: str | None = None,
        outcome: dict[str, Any] | None = None,
        conn_id: str = AnthropicHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: float = 30,
        timeout: float = 24 * 60 * 60,
        budget: BudgetSpec | None = None,
        vault_ids: list[str] | None = None,
        session_resources: list[dict[str, Any]] | None = None,
        session_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.agent_id = agent_id
        self.environment_id = environment_id
        self.message = message
        self.outcome = outcome
        self.conn_id = conn_id
        self.deferrable = deferrable
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.budget = budget
        self.vault_ids = vault_ids
        self.session_resources = session_resources
        self.session_kwargs = session_kwargs or {}
        self.session_id: str | None = None

    @cached_property
    def hook(self) -> AnthropicHook:
        """Return an instance of the AnthropicHook."""
        return AnthropicHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str | None:
        if (self.message is None) == (self.outcome is None):
            raise ValueError("Provide exactly one of 'message' or 'outcome'.")
        if self.outcome is not None and not {"description", "rubric"} <= self.outcome.keys():
            raise ValueError("'outcome' must include both 'description' and 'rubric'.")
        create_kwargs: dict[str, Any] = dict(self.session_kwargs)
        if self.budget is not None:
            if "budget" in create_kwargs:
                raise ValueError(
                    "Set the budget either via 'budget' or via session_kwargs['budget'], not both."
                )
            # Normalize eagerly so a malformed amount fails before a session (and its
            # server-side container) is allocated.
            create_kwargs["budget"] = build_budget(self.budget)
        if self.vault_ids:
            create_kwargs["vault_ids"] = self.vault_ids
        if self.session_resources:
            create_kwargs["resources"] = self.session_resources
        session = self.hook.create_session(
            agent=self.agent_id, environment_id=self.environment_id, **create_kwargs
        )
        self.session_id = session.id
        context["ti"].xcom_push(key="session_id", value=session.id)
        self.log.info("Started Anthropic session %s for agent %s", session.id, self.agent_id)

        try:
            if self.outcome is not None:
                response = self.hook.send_event(session.id, {"type": "user.define_outcome", **self.outcome})
            else:
                response = self.hook.send_event(
                    session.id,
                    {"type": "user.message", "content": [{"type": "text", "text": self.message}]},
                )
        except Exception:
            # send_event failed after create_session allocated the container; tear it down.
            # The event may still have been accepted server-side, so the agent can already be
            # spending -- record usage here too rather than leaving this the one failure path
            # with no cost trail.
            self._tear_down(context, session.id)
            raise
        # Correlate completion against the kickoff event so a message run is not fooled by a
        # just-created idle session (the start race).
        sent = response.data
        kickoff_event_id = sent[-1].id if sent else None

        expect_outcome = self.outcome is not None
        if self.deferrable:
            # Backstop the deferral slightly beyond the trigger's own end_time so the
            # trigger's clean "timeout" event (which tears the session down) wins the race
            # rather than a generic AirflowTaskTimeout. A user-set execution_timeout still
            # applies as a shorter hard cap (and then teardown is skipped — documented).
            self.defer(
                timeout=self.execution_timeout or timedelta(seconds=self.timeout + self.poll_interval + 60),
                trigger=AnthropicAgentSessionTrigger(
                    conn_id=self.conn_id,
                    session_id=session.id,
                    poll_interval=self.poll_interval,
                    end_time=time.time() + self.timeout,
                    expect_outcome=expect_outcome,
                    kickoff_event_id=kickoff_event_id,
                ),
                method_name="execute_complete",
            )

        self.log.info("Waiting for session %s to complete", session.id)
        try:
            self.hook.wait_for_session(
                session.id,
                expect_outcome=expect_outcome,
                kickoff_event_id=kickoff_event_id,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            )
        except Exception:
            # Any failure after the session starts (timeout, SDK 5xx, auth expiry) leaves
            # the server-side container running; archive it best-effort before failing.
            # Teardown goes first: it is the time-critical call, and its response carries
            # the usage, so reporting spend costs no extra request.
            self._tear_down(context, session.id)
            raise
        self._push_usage(context, session.id)
        return session.id

    def execute_complete(self, context: Context, event: Any = None) -> str:
        event = validate_execute_complete_event(event)
        # The deferred task is a fresh instance; restore the session id from the event.
        self.session_id = event["session_id"]
        status = event["status"]
        if status == "timeout":
            self._tear_down(context, self.session_id)
            raise AnthropicAgentSessionTimeout(event["message"])
        if status == "error":
            # The trigger yields "error" when polling gives up while the session may still
            # be running; archive it best-effort so its container does not linger.
            self._tear_down(context, self.session_id)
            raise _create_session_error(event["message"], event.get("stop_reason"))
        self.log.info("Session %s completed.", self.session_id)
        self._push_usage(context, self.session_id)
        return self.session_id

    def _tear_down(self, context: Context, session_id: str | None) -> None:
        """
        Archive the session and record what it spent, in that order.

        Every failure path needs both. Teardown goes first because it is the
        time-critical call, and ``sessions.archive`` returns the session carrying its final
        usage, so recording spend costs no extra request.
        """
        archived = self._archive_session(session_id)
        self._push_usage(context, session_id, session=archived)

    def _push_usage(self, context: Context, session_id: str | None, session: Any = None) -> None:
        """
        Push the session's token/cost usage to XCom under ``usage``, best effort.

        Runs on failure as well as success: a session that stopped against its budget is
        exactly the one whose spend you want recorded. Never allowed to raise -- a usage
        read that fails must not mask the task's real outcome.

        On the failure paths the session has just been archived, and ``sessions.archive``
        returns the session with its final usage; pass it as ``session`` so teardown is not
        delayed by an extra retrieve against an API that may be why the task is failing.
        """
        if not session_id:
            return
        # The whole body is guarded, not just the API call: this runs on the failure path
        # immediately before re-raising, so anything that throws here -- an unavailable
        # session, a serialization error, a context without a task instance -- would
        # replace the exception the task should actually fail with.
        try:
            # XCom is cleared at the start of every attempt, so this key only ever holds
            # the last one. Stamping the attempt makes that visible rather than silently
            # under-reporting total spend across retries. Built as a new dict rather than
            # mutating what the hook returned.
            summary = (
                self.hook.summarize_usage(session)
                if session is not None
                else self.hook.get_session_usage(session_id)
            )
            usage = {**summary, "try_number": getattr(context["ti"], "try_number", None)}
            context["ti"].xcom_push(key="usage", value=usage)
            cost = usage.get("list_cost")
            self.log.info(
                "Session %s used %s input / %s output tokens; list cost %s",
                session_id,
                usage.get("input_tokens"),
                usage.get("output_tokens"),
                f"{cost['currency']} {cost['amount']} (minor units)" if cost else "unavailable",
            )
        except Exception:
            self.log.exception("Could not record usage for session %s", session_id)

    def _archive_session(
        self, session_id: str | None, **archive_kwargs: Any
    ) -> BetaManagedAgentsSession | None:
        """
        Best-effort teardown of the server-side session (frees its container).

        Returns the archived session, which carries its final usage, or ``None`` if the
        archive call failed. ``archive_kwargs`` tightens the hook's interrupt-and-retry
        budget for callers that do not have its full ~25s.
        """
        if not session_id:
            return None
        try:
            return self.hook.archive_session(session_id, **archive_kwargs)
        except Exception as e:
            self.log.warning("Failed to archive session %s: %s", session_id, e)
            return None

    def on_kill(self) -> None:
        """
        Archive the session if the (non-deferred) task is killed.

        Only fires while the worker process is alive, i.e. the synchronous path
        (``deferrable=False``). On Airflow 3.3+ a killed deferred task is archived by the
        trigger's ``on_kill``. On older Airflow the session of a killed deferred task is not
        archived automatically; archive it manually via the hook.

        The supervisor escalates to SIGKILL a few seconds after asking the task to stop, so
        the hook's default retry budget would never finish here -- the process dies inside
        the first sleep and the session is never released. This budget gives the interrupt a
        beat to land (the status change is not instant) while still fitting inside that
        window: two attempts, one second apart.
        """
        self._archive_session(self.session_id, attempts=2, wait_seconds=1)
