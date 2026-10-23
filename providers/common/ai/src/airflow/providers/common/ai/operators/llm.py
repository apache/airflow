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
"""Operator for general-purpose LLM calls."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from pydantic_ai.usage import RunUsage

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.mixins.approval import LLMApprovalMixin, normalize_assigned_users
from airflow.providers.common.ai.mixins.cancellable_run import CancellableAgentRunMixin
from airflow.providers.common.ai.mixins.usage_budget import UsageBudgetMixin
from airflow.providers.common.ai.observability import (
    build_run_identity_attributes,
    make_task_instance_run_key,
    stamp_identity_on_agent_spans,
)
from airflow.providers.common.ai.policies.decision import DecisionPolicy
from airflow.providers.common.ai.utils.decision import (
    DECISION_XCOM_KEY,
    ModelConfidence,
    ReviewReason,
    check_uncertain_action,
    decision_record,
    describe_confidence,
    finalize_record,
    initial_decided_by,
    policy_record,
    review_reason,
    timed_out_record,
    validate_decision_policy,
)
from airflow.providers.common.ai.utils.logging import (
    MODEL_NAME_XCOM_KEY,
    format_usage_for_xcom,
    log_run_summary,
    log_run_usage,
)
from airflow.providers.common.ai.utils.output_type import (
    dump_output_for_xcom,
    dump_output_to_json,
    rehydrate_pydantic_output,
)
from airflow.providers.common.ai.utils.usage import coerce_usage_limits
from airflow.providers.common.ai.utils.usage_budget import copy_run_usage, subtract_run_usage
from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException, BaseOperator
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.standard.exceptions import HITLRejectException, HITLTimeoutError

try:
    # The worker-side DAG walk registers operator-declared ``output_type`` classes
    # for XCom deserialization. ``apache-airflow-task-sdk`` versions with this walk
    # send model instances unchanged; older versions dump them to a dict instead.
    from airflow.sdk.serde import SUPPORTS_OPERATOR_DESERIALIZATION_WALKER as _CORE_WALKER
except ImportError:  # pragma: no cover - missing ``apache-airflow-task-sdk`` walker
    _CORE_WALKER = False

if TYPE_CHECKING:
    from types import UnionType

    from pydantic_ai import Agent
    from pydantic_ai.agent import AgentRunResult
    from pydantic_ai.settings import ModelSettings
    from pydantic_ai.usage import UsageLimits

    from airflow.sdk import Context
    from airflow.sdk.execution_time.hitl import HITLUser


__all__ = ["DecisionPolicy", "LLMOperator"]


# CancellableAgentRunMixin must precede BaseOperator so its on_kill overrides BaseOperator's
# no-op. The other mixins only add methods, so they can trail BaseOperator. See the MRO guard
# test in tests/unit/common/ai/mixins/test_cancellable_run.py.
class LLMOperator(CancellableAgentRunMixin, BaseOperator, LLMApprovalMixin, UsageBudgetMixin):
    """
    Call an LLM with a prompt and return the output.

    Uses a :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    for LLM access. Supports plain string output (default) and structured output
    via a Pydantic ``BaseModel``. When ``output_type`` is a ``BaseModel`` subclass,
    the model instance is returned to XCom unchanged so downstream tasks can
    type-hint it directly (e.g. ``def downstream(result: MyModel) -> None``).
    Inside a list of types (``[MyModel, int]``) the model is returned as a ``dict`` instead.
    The class is auto-registered for deserialization in each process that parses
    the DAG, so no edit to ``[core] allowed_deserialization_classes`` is required.
    The Pydantic class must be defined at module scope: classes nested inside
    a function or ``@dag``-decorated body cannot be deserialized from XCom.

    No tool runs in the task: the operator accepts no function tools, toolsets or
    capabilities, makes one model call (repeated only when the output fails validation)
    and returns its output. For an agent that calls tools, use
    :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`.

    Once ``_clear_usage_budget`` has run, nothing may fail the attempt (raising
    ``DownstreamTasksSkipped`` to signal success is fine): a failure would be retried
    with a fresh budget. This applies to subclasses that override ``execute`` or
    ``execute_complete``.

    Alongside the returned output, the run's ``run_id`` and token ``usage``
    are pushed to XCom under the ``run_id`` and ``usage`` keys, the same shape
    :class:`~airflow.providers.common.ai.operators.agent.AgentOperator` uses,
    so a downstream task can reference the run and what it cost. ``usage`` is this
    attempt's own usage, not the cross-attempt cumulative total described under
    ``usage_limits`` below. Both are pushed on a failed run too, so a downstream
    ``all_done`` task or failure callback can read what was spent, and which run,
    before it raised. The ``run_id`` also ties the task to its GenAI trace (see the
    provider's observability docs).

    :param prompt: The prompt to send to the LLM.
    :param llm_conn_id: Connection ID for the LLM provider.
    :param model_id: Model identifier (e.g. ``"openai:gpt-5"``).
        Overrides the model stored in the connection's extra field.
    :param fallback_conn_ids: Connection IDs to fail over to, in order, when
        the primary provider is unavailable. Overrides the ``fallback_conn_ids``
        set in the connection's extra field. ``None`` (default) reads the
        connection's own extra field; an explicit ``[]`` disables a chain
        configured there. See
        :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
        for how blank entries in the list are dropped.
    :param system_prompt: System-level instructions for the LLM agent.
    :param output_type: Expected output type. Default ``str``. A single type
        (``str``, ``int``, ``list[str]``, a Pydantic ``BaseModel`` subclass) or a
        union (``A | B``). A list of types (``[str, int]``) is also accepted and treated
        as alternatives (a union). For a ``BaseModel`` subclass, the model instance is
        returned to XCom unchanged so downstream tasks can type-hint it
        directly, except inside a list of types, where it is returned as a ``dict``
        because the worker registers only the classes it finds in a single type or a union.
        The class must be defined at module scope -- nested classes
        cannot be deserialized from XCom.
    :param model_settings: Pydantic AI :class:`~pydantic_ai.settings.ModelSettings`
        for the model call, such as ``temperature`` or ``max_tokens``, including
        provider-specific settings. ``None`` (default) uses the model's defaults.
    :param output_retries: How many times the model is asked again when its output
        fails validation against ``output_type``. ``None`` (default) uses Pydantic AI's
        default.
    :param usage_limits: Optional pydantic-ai
        :class:`~pydantic_ai.usage.UsageLimits` enforced on the run, or a dict
        of the same fields (e.g.
        ``{"cost_limit": "{{ params.budget }}", "request_limit": 5}``). The dict
        form is templated: each value is rendered by Jinja like any other
        ``template_fields`` entry, then coerced to that field's type (``Decimal``,
        ``int``, or ``bool``). A value that cannot be coerced -- a Variable
        that exists but is empty renders to ``""``, a typo renders to a
        non-numeric string -- fails the task with a ``ValueError`` naming the
        field and the rendered value, instead of silently disabling the
        limit. A ``UsageLimits`` instance passed directly is used as-is and
        is not templated or validated. ``None`` (default) sets no token, cost,
        or tool-call limits, but pydantic-ai still caps the run at its default
        ``request_limit`` of ``50`` requests.

        A dict that omits ``request_limit`` gets the same default of ``50``
        requests -- pass ``"request_limit": None`` explicitly for no request
        cap. This matches building a ``UsageLimits`` directly.

        On Airflow >= 3.3, a set ``usage_limits`` bounds the whole task instance:
        usage from earlier attempts counts against it. See
        :ref:`howto/operator:llm` for the full set of caveats.
    :param require_approval: If ``True``, the task pauses after generating
        output (``awaiting_input`` on Airflow 3.3+, deferred on 3.1 and 3.2)
        and waits for a human reviewer to approve or reject via the HITL
        interface.  Default ``False``. Needs Airflow 3.1+.
    :param approval_timeout: Maximum time to wait for a review.  When
        exceeded, ``on_approval_timeout`` decides the outcome.
    :param on_approval_timeout: What to do when ``approval_timeout`` expires
        without a review.  ``"fail"`` (default) fails the task with
        ``HITLTimeoutError``; ``"approve"`` and ``"reject"`` answer the review
        with that option, so the task resumes as if a reviewer had chosen it.
        The chosen option is also pre-highlighted for the reviewer in the HITL
        form.  Requires a review path (``require_approval=True`` or a
        ``decision_policy`` that reviews) and a positive
        ``approval_timeout``.
    :param allow_modifications: If ``True``, the reviewer can edit the output
        before approving.  The edit is converted back into ``output_type`` and returned
        as the task result.  For a type pydantic can validate, an edit that does not validate
        fails the task with ``ReviewedOutputValidationError``; a retry runs the model again.
        Default ``False``.
    :param approval_notifiers: Notifiers called once the review is open, so a
        reviewer is told about it.  Only takes effect when a review is
        opened.  A retry re-notifies with the regenerated output.  On
        Airflow 3.4+ the reopened review shows it too; on Airflow 3.1 to 3.3
        the review keeps the original subject, body and editable output, so
        approving it unedited returns the first attempt's output.  Default
        ``None``.
    :param approval_assigned_users: Users allowed to answer the review, as
        ``{"id": ..., "name": ...}`` dicts where ``id`` is the auth manager's
        user id.  ``None`` (default) lets any user with the permission respond.
        On Airflow 3.1 to 3.3 the list is fixed when the review is first
        created; on 3.4+ a cleared or retried task replaces it.  Needs Airflow 3.1+.
    :param decision_policy: Experimental. A
        :class:`~airflow.providers.common.ai.policies.decision.DecisionPolicy`
        saying how confident the model has to be for the operator to return its answer by
        itself (``min_confidence``) and what happens otherwise (``on_uncertain``: ``"review"``
        or ``"fail"``). Confidence comes from models that report one per output field, such as
        a decision model, in ``provider_details``. A structured output is judged by its least
        confident field among the fields that reported one; a field whose type reports none (a
        bounded float, where the probability is the answer) is not gated, and the record's
        ``confidence`` map shows which fields were compared. When no field reports
        any confidence, as with a text model, the output counts as uncertain, so swapping the
        connection does not silently switch off a control the author set. Independent of
        ``require_approval``, which always asks. ``on_uncertain="review"`` needs Airflow 3.1+,
        like ``require_approval``. Default ``None``: no gate, behaviour unchanged.
    :param serialize_output: If ``True``, a Pydantic ``BaseModel`` output, or a
        list, tuple, set or dict holding models, is dumped to JSON-compatible
        values with ``pydantic_core.to_jsonable_python`` before being pushed to
        XCom. A model the worker cannot register from ``output_type`` (for example
        one in a list of output types) is dumped even when this is ``False``.
        Default ``False`` -- a registered Pydantic instance flows through XCom
        unchanged. Set to ``True`` when a downstream consumer needs the dict shape
        (e.g. sending to an external system that expects JSON-style payloads).
    """

    deserialization_allowed_class_fields: ClassVar[tuple[str, ...]] = ("output_type",)

    # Subclasses that run their own ``execute`` without the confidence gate set this to False so a
    # ``decision_policy`` is rejected at construction rather than accepted and ignored.
    supports_decision_policy: ClassVar[bool] = True

    template_fields: Sequence[str] = (
        "prompt",
        "llm_conn_id",
        "model_id",
        "fallback_conn_ids",
        "system_prompt",
        "model_settings",
        "usage_limits",
    )

    def __init__(
        self,
        *,
        prompt: str,
        llm_conn_id: str,
        model_id: str | None = None,
        fallback_conn_ids: list[str] | None = None,
        system_prompt: str = "",
        output_type: type | UnionType | Sequence[type] = str,
        model_settings: ModelSettings | None = None,
        output_retries: int | None = None,
        usage_limits: UsageLimits | dict[str, Any] | None = None,
        require_approval: bool = False,
        approval_timeout: timedelta | None = None,
        on_approval_timeout: Literal["fail", "approve", "reject"] = "fail",
        allow_modifications: bool = False,
        approval_notifiers: BaseNotifier | Iterable[BaseNotifier] | None = None,
        approval_assigned_users: HITLUser | Iterable[HITLUser] | None = None,
        decision_policy: DecisionPolicy | None = None,
        serialize_output: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.decision_policy = validate_decision_policy(decision_policy)
        if self.decision_policy.gates and not self.supports_decision_policy:
            raise ValueError(
                f"{type(self).__name__} does not support decision_policy yet; it runs its own execute() "
                "without the confidence gate. Use require_approval=True for an unconditional review."
            )
        self.prompt = prompt
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.fallback_conn_ids = fallback_conn_ids
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.serialize_output = serialize_output
        # Return the Pydantic instance when ``apache-airflow-task-sdk`` can register ``output_type``
        # for deserialization (its worker-side DAG walk); otherwise, or when the
        # user opts in, dump to a dict so the value is deserializable anywhere.
        self._serialize_model_output = serialize_output or not _CORE_WALKER
        self.model_settings = model_settings
        self.output_retries = output_retries
        # No validation here -- see coerce_usage_limits() docstring for why.
        self.usage_limits = usage_limits
        if on_approval_timeout not in ("fail", *LLMApprovalMixin.TIMEOUT_DEFAULTS):
            raise ValueError(
                f"on_approval_timeout must be 'fail', 'approve', or 'reject', got {on_approval_timeout!r}."
            )
        # Checked before the combination rule so an older Airflow version reports the Airflow version
        # rather than sending the user to drop an argument that was never the problem.
        if require_approval and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException("require_approval=True needs Airflow 3.1+.")
        if self.decision_policy.reviews and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException(
                "DecisionPolicy(on_uncertain='review') needs Airflow 3.1+; use on_uncertain='fail' on Airflow versions older than 3.1."
            )

        # A review can open either way; both settings make the approval flow reachable.
        self._may_review = require_approval or self.decision_policy.reviews
        if on_approval_timeout != "fail" and not (
            self._may_review and approval_timeout is not None and approval_timeout > timedelta(0)
        ):
            raise ValueError(
                f"on_approval_timeout={on_approval_timeout!r} needs a review path (require_approval=True or "
                "a decision_policy with on_uncertain='review') and a positive approval_timeout to fire. "
                "Set both, or leave on_approval_timeout as 'fail'."
            )
        self.require_approval = require_approval
        self.approval_timeout = approval_timeout
        self.on_approval_timeout = on_approval_timeout
        self.allow_modifications = allow_modifications
        if approval_notifiers is None:
            approval_notifiers = []
        elif isinstance(approval_notifiers, BaseNotifier):
            approval_notifiers = [approval_notifiers]
        elif isinstance(approval_notifiers, str) or not isinstance(approval_notifiers, Iterable):
            raise TypeError(
                "approval_notifiers must be a BaseNotifier or an iterable of BaseNotifier instances, "
                f"got {approval_notifiers!r}"
            )
        self.approval_notifiers = list(approval_notifiers)
        for notifier in self.approval_notifiers:
            if not isinstance(notifier, BaseNotifier):
                raise TypeError(f"approval_notifiers must contain BaseNotifier instances, got {notifier!r}")
        assigned_users = normalize_assigned_users(approval_assigned_users, param="approval_assigned_users")
        if assigned_users and not AIRFLOW_V_3_1_PLUS:
            raise AirflowOptionalProviderFeatureException("approval_assigned_users needs Airflow 3.1+.")
        self.approval_assigned_users: list[HITLUser] = assigned_users

    @cached_property
    def llm_hook(self) -> PydanticAIHook:
        """
        Return the correct PydanticAIHook subclass for the configured connection.

        Delegates to :meth:`~PydanticAIHook.get_hook` which looks up
        the connection's ``conn_type`` and instantiates the matching subclass
        (e.g. :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIAzureHook`
        for ``pydanticai_azure`` connections).
        """
        hook_params = {
            "model_id": self.model_id,
            "fallback_conn_ids": self.fallback_conn_ids,
        }
        return PydanticAIHook.get_hook(self.llm_conn_id, hook_params=hook_params)

    def _create_agent(self, *, output_type: Any, instructions: str) -> Agent[object, Any]:
        """
        Build the agent for this operator's model call.

        The only way any LLM operator builds its agent. Only ``model_settings`` and
        ``output_retries`` reach it, so no function tool, toolset or capability is ever
        registered; an agent with tools is what ``AgentOperator`` is for.
        """
        return self.llm_hook.create_agent(
            output_type=output_type,
            instructions=instructions,
            model_settings=self.model_settings,
            retries=self.output_retries,
        )

    def execute(self, context: Context) -> Any:
        if self._may_review:
            self.validate_approval_prompt()  # type: ignore[misc]

        # Coerced first so a bad rendered value fails before the expensive setup below.
        usage_limits = coerce_usage_limits(self.usage_limits)

        agent = self._create_agent(output_type=self.output_type, instructions=self.system_prompt)

        # A per-attempt key (the task-instance id on Airflow 3, which is regenerated on each
        # retry; dag/run/task/map/try on Airflow 2) ties this run to its GenAI trace, the same
        # join key AgentOperator uses. A hand-built context with no task instance runs untracked.
        ti = self._resolve_task_instance(context)
        run_kwargs: dict[str, Any] = {}
        if ti is not None:
            stamp_identity_on_agent_spans(agent, build_run_identity_attributes(ti))
            run_kwargs["run_id"] = make_task_instance_run_key(ti)

        attempt_usage = RunUsage()
        try:
            result = self._run_llm(
                context, agent, self.prompt, usage_limits=usage_limits, run_usage=attempt_usage, **run_kwargs
            )
        except BaseException:
            # A run that raises partway through (e.g. a cost_limit-triggered UsageLimitExceeded)
            # still reports what it spent: the capped run is the one cost tracking most needs.
            try:
                log_run_usage(self.log, attempt_usage, outcome="failed")
            except Exception:
                self.log.warning("Failed to log partial usage for the failed run", exc_info=True)
            try:
                self._push_xcom(context, "usage", format_usage_for_xcom(attempt_usage))
            except Exception:
                self.log.warning("Failed to push usage XCom for the failed run", exc_info=True)
            if ti is not None:
                try:
                    self._push_xcom(context, "run_id", run_kwargs["run_id"])
                except Exception:
                    self.log.warning("Failed to push run_id XCom for the failed run", exc_info=True)
            raise
        output = result.output

        self._push_xcom(context, "usage", format_usage_for_xcom(attempt_usage))
        if ti is not None:
            self._push_xcom(context, "run_id", result.run_id)
        model_confidence = ModelConfidence.from_result(result)
        if model_confidence.model is not None:
            self._push_xcom(context, MODEL_NAME_XCOM_KEY, model_confidence.model)
        # Gated by the least confident of the fields that reported a confidence; a field whose type
        # reports none is not gated. A bare output type is the one field ``response``.
        fields = list(model_confidence.confidence) or ["response"]
        policy = self.decision_policy
        threshold = policy.min_confidence
        confidence = model_confidence.lowest(fields)
        review = review_reason(
            require_approval=self.require_approval, threshold=threshold, confidence=confidence
        )
        record = decision_record(
            model_confidence=model_confidence,
            proposed=None,
            action=None,
            threshold=threshold,
            review=review,
            decided_by=initial_decided_by(review, policy.on_uncertain),
            policy=policy_record(policy),
        )
        self._push_decision(context, record)
        check_uncertain_action(
            review,
            policy.on_uncertain,
            what=f"task {self.task_id!r}",
            confidence=confidence,
            threshold=threshold,
        )

        if review:
            self._log_review(review, confidence, threshold)
            body = None
            if review != "require_approval":
                # The same JSON text the default body and the edit field show, not the object's repr.
                body = f"```\nPrompt: {self.prompt}\n\n{dump_output_to_json(output)}\n```\n\n" + "\n\n".join(
                    describe_confidence(model_confidence, name, threshold) for name in fields
                )
            self.defer_for_approval(context, output, body=body, decision=record)  # type: ignore[misc]

        output = dump_output_for_xcom(output, self.output_type, serialize_output=self._serialize_model_output)

        self._clear_usage_budget(context)
        return output

    def execute_complete(
        self,
        context: Context,
        generated_output: str,
        event: dict[str, Any],
        decision: dict[str, Any] | None = None,
    ) -> Any:
        """
        Resume after human review and convert the output back into ``output_type``.

        :raises ReviewedOutputValidationError: If a reviewer's edit does not validate against ``output_type``.
        """
        output = self._complete_review(context, generated_output, event, decision)
        self._clear_usage_budget(context)
        return output

    def _complete_review(
        self,
        context: Context,
        generated_output: str,
        event: dict[str, Any],
        decision: dict[str, Any] | None,
    ) -> Any:
        """
        Resolve the review and rehydrate the output, leaving the usage budget for the caller to clear.

        :raises ReviewedOutputValidationError: If a reviewer's edit does not validate against ``output_type``.
        """
        output = self._resume_after_review(context, generated_output, event, decision)
        self._finalize_decision(context, event, decision, action=None)
        return rehydrate_pydantic_output(
            self.output_type,
            output,
            serialize_output=self._serialize_model_output,
            # An unedited approval hands back ``generated_output`` as is, and only an edit may fail the task.
            is_edited=output != generated_output,
        )

    def _run_llm(
        self,
        context: Context,
        agent: Agent[Any, Any],
        user_prompt: Any,
        *,
        usage_limits: UsageLimits | None,
        run_usage: RunUsage | None = None,
        **run_kwargs: Any,
    ) -> AgentRunResult[Any]:
        """
        Run the agent, bounding ``usage_limits`` across task retries when the usage budget applies.

        ``run_usage``, when given, receives this attempt's own usage, also when the run raises
        partway through. With the usage budget, pydantic-ai is handed the cross-attempt total
        instead, so the attempt's share is credited to ``run_usage`` once the run ends.
        """
        # Contexts built without a ``task_instance`` stay valid for runs that have no ``usage_limits``.
        ti = context["task_instance"] if usage_limits is not None else None
        self._usage_budget = self._build_usage_budget(context, usage_limits, ti=ti)
        if self._usage_budget is None:
            if run_usage is not None:
                run_kwargs["usage"] = run_usage
            result = self.run_agent_sync(agent, user_prompt, usage_limits=usage_limits, **run_kwargs)
            log_run_summary(self.log, result)
            return result
        cumulative_usage = self._usage_budget.load()
        base = copy_run_usage(cumulative_usage)
        try:
            result, attempt_usage = self._run_tracked(
                agent, user_prompt, run_usage=cumulative_usage, usage_limits=usage_limits, **run_kwargs
            )
        finally:
            if run_usage is not None:
                run_usage.incr(subtract_run_usage(cumulative_usage, base))
        log_run_summary(self.log, result, usage=attempt_usage)
        self._log_cumulative_usage(cumulative_usage)
        return result

    def _log_review(self, review: ReviewReason, confidence: float | None, threshold: float | None) -> None:
        if review == "below_threshold":
            self.log.info(
                "Sending the output to review: confidence %.2f is below min_confidence=%.2f.",
                confidence,
                threshold,
            )
        elif review == "missing_confidence":
            self.log.info(
                "Sending the output to review: min_confidence=%.2f is set but the model reported no "
                "confidence for its answer.",
                threshold,
            )

    def _resume_after_review(
        self, context: Context, generated_output: str, event: dict[str, Any], decision: dict[str, Any] | None
    ) -> str:
        """
        Run the mixin's resume and, when it raises, finalize the record before the exception leaves.

        A rejection and a timeout with no default both end in an exception from the mixin; without
        this the ``decision`` XCom would stay pending, indistinguishable from a review still open.
        """
        try:
            return LLMApprovalMixin.execute_complete(self, context, generated_output, event, decision)
        except HITLRejectException:
            self._finalize_decision(context, event, decision, action=None)
            raise
        except HITLTimeoutError:
            if decision is not None:
                self._push_decision(context, timed_out_record(decision))
            raise

    def _resolve_task_instance(self, context: Context) -> Any:
        """
        Return ``context["task_instance"]`` if it can push XComs, else ``None``.

        A hand-built context (old tests, custom runners) may carry no task instance, ``None``, or a
        dict stand-in, none of which has the attributes the run identity is built from either.
        """
        try:
            ti = context["task_instance"]
        except (KeyError, TypeError):
            return None
        return ti if callable(getattr(ti, "xcom_push", None)) else None

    def _push_xcom(self, context: Context, key: str, value: Any) -> None:
        """Push ``value`` under ``key`` on XCom, honoring ``do_xcom_push`` and a missing task instance."""
        if not self.do_xcom_push:
            return
        ti = self._resolve_task_instance(context)
        if ti is None:
            # A hand-built context (a dict, or no task instance at all) has nowhere to push to; the
            # value is inspection output, so the run goes on without it.
            self.log.warning("No task instance in the context; %r was not pushed to XCom.", key)
            return
        ti.xcom_push(key=key, value=value)

    def _push_decision(self, context: Context, record: dict[str, Any]) -> None:
        """Expose what the model proposed, its confidence, and what the gate decided, on XCom."""
        self._push_xcom(context, DECISION_XCOM_KEY, record)

    def _finalize_decision(
        self, context: Context, event: dict[str, Any], decision: dict[str, Any] | None, *, action: Any
    ) -> None:
        """
        After a review, overwrite the pending ``decision`` XCom with who decided and what was done.

        ``decision`` is the record carried in the continuation since the pause, so the final record
        does not depend on the pending XCom still being there or unchanged.
        """
        if decision is None:
            return
        self._push_decision(context, finalize_record(decision, event, action=action))
