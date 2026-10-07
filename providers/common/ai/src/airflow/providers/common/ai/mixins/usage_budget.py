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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.common.ai.utils.usage_budget import (
    TaskStateStoreUsageBudget,
    copy_run_usage,
    subtract_run_usage,
)
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    from logging import Logger

    from pydantic_ai import Agent
    from pydantic_ai.agent import AgentRunResult
    from pydantic_ai.usage import RunUsage, UsageLimits

    from airflow.sdk import Context


class UsageBudgetHostProtocol(Protocol):
    """Protocol for the operator attributes and methods :class:`UsageBudgetMixin` relies on."""

    log: Logger
    usage_limits: UsageLimits | dict[str, Any] | None
    _usage_budget: TaskStateStoreUsageBudget | None

    def run_agent_sync(
        self, agent: Agent[Any, Any], user_prompt: Any, **run_kwargs: Any
    ) -> AgentRunResult[Any]: ...

    def _build_usage_budget(
        self, context: Context, usage_limits: UsageLimits | None, *, ti: Any
    ) -> TaskStateStoreUsageBudget | None: ...

    def _get_usage_budget(
        self, context: Context, usage_limits: UsageLimits | None
    ) -> TaskStateStoreUsageBudget | None: ...

    def _settle_tracked_usage(self) -> None: ...


class UsageBudgetMixin:
    """
    Mixin that bounds ``usage_limits`` across every attempt of a task instance.

    On Airflow >= 3.3 with ``usage_limits`` set, cumulative usage is persisted in the
    task state store (see :class:`~airflow.providers.common.ai.utils.usage_budget.TaskStateStoreUsageBudget`)
    so a task retry resumes from the usage earlier attempts already spent instead of starting a
    fresh count.

    Operators that use this mixin must provide ``usage_limits``, ``log`` and
    ``run_agent_sync`` (see :class:`~airflow.providers.common.ai.mixins.cancellable_run.CancellableAgentRunMixin`).
    """

    _usage_budget: TaskStateStoreUsageBudget | None = None

    def _build_usage_budget(
        self, context: Context, usage_limits: UsageLimits | None, *, ti: Any
    ) -> TaskStateStoreUsageBudget | None:
        """
        Return the cross-attempt usage-budget accessor, or ``None`` when it should not apply.

        Gated like ``_build_durable_storage``: only on Airflow >= 3.3, where the task
        state store survives retries. Also gated on ``usage_limits is not None`` --
        with ``usage_limits=None`` turning this on would silently impose pydantic-ai's
        default ``request_limit=50`` across every attempt of every operator on
        3.3+, which nobody asked for.

        To switch the gate off in a test, patch ``AIRFLOW_V_3_3_PLUS`` in this module.
        """
        if not (AIRFLOW_V_3_3_PLUS and usage_limits is not None):
            return None
        return TaskStateStoreUsageBudget(context["task_state_store"], max_tries=ti.max_tries)

    def _get_usage_budget(
        self, context: Context, usage_limits: UsageLimits | None
    ) -> TaskStateStoreUsageBudget | None:
        """Return the usage budget for ``context``, reading the task instance only when a budget can apply."""
        # Contexts built without a ``task_instance`` stay valid for runs that have no ``usage_limits``.
        ti = context["task_instance"] if usage_limits is not None else None
        return self._build_usage_budget(context, usage_limits, ti=ti)

    def _settle_tracked_usage(self) -> None:
        """Do nothing; override to run after a tracked run and before its usage is persisted."""

    def _run_tracked(
        self: UsageBudgetHostProtocol,
        agent: Agent[Any, Any],
        prompt: Any,
        *,
        run_usage: RunUsage,
        before_run: Callable[[], None] | None = None,
        **run_kwargs: Any,
    ) -> tuple[AgentRunResult[Any], RunUsage]:
        """
        Run the agent, persisting cumulative usage after every attempt (success or failure).

        ``run_usage`` is the (possibly cross-attempt) cumulative total the run is seeded
        with; the returned ``RunUsage`` is what this attempt alone added to it.
        ``before_run`` runs after the snapshot is taken, so anything it credits to
        ``run_usage`` never shows up in this attempt's delta.
        """
        base = copy_run_usage(run_usage)
        try:
            if before_run is not None:
                before_run()
            result = self.run_agent_sync(agent, prompt, usage=run_usage, **run_kwargs)
        finally:
            self._settle_tracked_usage()
            if self._usage_budget:
                self._usage_budget.save(run_usage)
        return result, subtract_run_usage(run_usage, base)

    def _log_cumulative_usage(self: UsageBudgetHostProtocol, run_usage: RunUsage) -> None:
        """Log the usage accumulated across all attempts of this task instance."""
        self.log.info(
            "Cumulative usage across attempts: requests=%s, tool_calls=%s, input_tokens=%s, "
            "output_tokens=%s, total_tokens=%s",
            run_usage.requests,
            run_usage.tool_calls,
            run_usage.input_tokens,
            run_usage.output_tokens,
            run_usage.total_tokens,
        )
        if run_usage.cost is not None:
            self.log.info(
                "Cumulative cost across attempts: $%s (USD, best-effort)",
                format(run_usage.cost, "f"),
            )

    def _clear_usage_budget(self: UsageBudgetHostProtocol, context: Context) -> None:
        """
        Drop the persisted usage so the next task instance starts with a fresh budget.

        A resumed operator (``execute_complete``) is a new instance whose ``_usage_budget``
        was never set, so the budget is rebuilt from ``usage_limits`` in that case.
        """
        budget = self._usage_budget
        if budget is None:
            budget = self._get_usage_budget(context, coerce_usage_limits(self.usage_limits))
        if budget is not None:
            budget.clear()
