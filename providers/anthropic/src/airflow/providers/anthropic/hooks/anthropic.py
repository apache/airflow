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
from collections.abc import Mapping
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from anthropic import (
    Anthropic,
    AnthropicAWS,
    AnthropicBedrock,
    AnthropicFoundry,
    AnthropicVertex,
    BadRequestError,
    IdentityTokenFile,
    WorkloadIdentityCredentials,
)

from airflow.providers.anthropic.exceptions import (
    AnthropicAgentSessionError,
    AnthropicAgentSessionTimeout,
    AnthropicBatchJobError,
    AnthropicBatchTimeout,
    AnthropicError,
    AnthropicSessionBudgetExceeded,
    AnthropicTriggerEventError,
)
from airflow.providers.common.compat.sdk import AirflowSkipException, BaseHook

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from anthropic.types import Message
    from anthropic.types.beta import (
        BetaEnvironment,
        BetaManagedAgentsAgent,
        BetaManagedAgentsBudgetLimitParam,
        BetaManagedAgentsSession,
        agent_create_params,
        environment_create_params,
    )
    from anthropic.types.beta.sessions import BetaManagedAgentsEventParams
    from anthropic.types.messages import MessageBatch, MessageBatchIndividualResponse
    from anthropic.types.messages.batch_create_params import Request

#: Default model used when an operator or hook caller does not specify one.
#: Prefer configuring the model on the connection so it can be updated without
#: a provider release when this model ID is retired.
DEFAULT_MODEL = "claude-opus-4-8"

#: Platforms that serve the first-party-only endpoints (Message Batches, token
#: counting, the Models API). Amazon Bedrock, Google Vertex AI and Microsoft
#: Foundry do not serve these, so the hook fails fast rather than surfacing a
#: raw ``404`` from the SDK.
FIRST_PARTY_PLATFORMS = frozenset({"anthropic", "aws"})

AnthropicClient = Anthropic | AnthropicBedrock | AnthropicVertex | AnthropicAWS | AnthropicFoundry

#: Consecutive failed polls tolerated in the synchronous wait helpers before giving up
#: (transient errors). Mirrors the deferrable triggers' tolerance so a single blip does
#: not fail (and cancel/archive) a still-healthy batch or session.
MAX_CONSECUTIVE_POLL_FAILURES = 5


class BatchStatus(str, Enum):
    """Top-level ``processing_status`` of an Anthropic Message Batch."""

    IN_PROGRESS = "in_progress"
    CANCELING = "canceling"
    ENDED = "ended"

    @classmethod
    def is_in_progress(cls, status: str) -> bool:
        """
        Return ``True`` while the batch has not reached the terminal ``ended`` status.

        This is broader than the ``in_progress`` value: a ``canceling`` batch is also
        non-terminal (cancellation is in flight but the batch has not ended yet), so it
        returns ``True`` too. Read the name as "not yet terminal", not "equals the
        ``in_progress`` status".
        """
        return status != cls.ENDED


class SessionStatus(str, Enum):
    """Status of a Managed Agents session."""

    RESCHEDULING = "rescheduling"
    RUNNING = "running"
    IDLE = "idle"
    TERMINATED = "terminated"

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        """
        Return ``True`` once the session has stopped working.

        ``idle`` means the agent finished its turn (done, for an autonomous run);
        ``terminated`` is an unrecoverable failure. Both stop the wait.
        """
        return status in (cls.IDLE, cls.TERMINATED)


#: ``outcome_evaluations[].result`` values that mean the outcome did NOT succeed.
OUTCOME_FAILURE_RESULTS = frozenset({"failed", "max_iterations_reached", "interrupted"})

# ``session.status_idle`` stop reason emitted when a session stops against its budget.
BUDGET_REACHED = "budget_reached"

# What a caller may pass as a session budget: an amount in USD, or the raw API payload.
BudgetSpec = str | int | float | Decimal | Mapping[str, Any]


def build_budget(budget: BudgetSpec) -> BetaManagedAgentsBudgetLimitParam:
    """
    Normalize a session budget into the API's ``max_list_cost`` payload.

    A scalar is read as **US dollars** (``25``, ``25.0`` and ``"25.00"`` all mean $25.00).
    The API wants minor units as an integer decimal string, so the conversion runs through
    :class:`~decimal.Decimal` (never binary float) and rejects an amount finer than a cent
    rather than silently rounding money. A mapping is deep-copied and otherwise returned
    unchanged, so a raw payload the provider has not caught up with stays usable without a
    provider release.

    .. warning::
        The ceiling is a stop trigger, not a cap: it is checked between model requests, so
        a request already in flight can carry the session well past it.
    """
    if isinstance(budget, Mapping):
        # Deep, not ``dict()``: a shallow copy leaves the nested ``max_list_cost`` aliased
        # to the caller's object, so a later edit of the returned payload would reach back
        # into a templated operator field.
        # Cast, not validate: a raw payload is accepted precisely so a caller can reach a
        # field this provider does not model yet, so its shape cannot be checked here.
        return cast("BetaManagedAgentsBudgetLimitParam", deepcopy(dict(budget)))
    try:
        dollars = Decimal(str(budget))
    except (InvalidOperation, ValueError) as e:
        raise ValueError(f"Invalid budget {budget!r}: not a decimal amount in USD.") from e
    if not dollars.is_finite() or dollars <= 0:
        raise ValueError(f"Invalid budget {budget!r}: must be a positive USD amount.")
    minor_units = dollars * 100
    if minor_units != minor_units.to_integral_value():
        raise ValueError(
            f"Invalid budget {budget!r}: amounts finer than a cent are not representable. "
            "Pass a mapping if you need the raw payload."
        )
    return {
        "type": "limit",
        "max_list_cost": {"amount": str(int(minor_units)), "currency": "USD"},
    }


def _create_session_error(message: str, stop_reason: str | None) -> AnthropicAgentSessionError:
    """
    Return the session error class matching an idle ``stop_reason``.

    Keyed on the SDK's own ``stop_reason`` value rather than on the message text, so the
    synchronous path and the deferrable path (which only carries the reason as a string
    through the trigger event) raise the same type for the same cause.
    """
    if stop_reason == BUDGET_REACHED:
        return AnthropicSessionBudgetExceeded(message)
    return AnthropicAgentSessionError(message)


class SessionPollResult(NamedTuple):
    """
    Verdict from one poll of a session; see :meth:`AnthropicHook.poll_session_completion`.

    Named rather than a bare tuple because ``error_message`` and ``stop_reason`` are both
    ``str | None``, so transposing them at a call site would still type-check.
    """

    done: bool
    error_message: str | None
    stop_reason: str | None


def evaluate_session_state(
    session: BetaManagedAgentsSession, *, expect_outcome: bool
) -> tuple[bool, str | None, bool]:
    """
    Judge a polled session from its object fields alone.

    Returns ``(done, error_message, needs_event_check)``. ``done=False`` means keep
    polling. ``needs_event_check=True`` means the session is ``idle`` on a ``message``
    run and the object can't say *why* — the caller must inspect the event log (see
    :meth:`AnthropicHook.poll_session_completion`).

    The ``status`` field can't distinguish a genuine ``end_turn`` from ``requires_action``
    or ``retries_exhausted``, nor a just-created ``idle``. For an outcome run the true
    verdict is in ``outcome_evaluations`` (judged here, which also defeats the start race).
    """
    if session.status == SessionStatus.TERMINATED:
        return True, f"Session {session.id} terminated.", False
    if session.status != SessionStatus.IDLE:
        return False, None, False
    if not expect_outcome:
        return False, None, True
    for evaluation in session.outcome_evaluations:
        if evaluation.result == "satisfied":
            return True, None, False
        if evaluation.result in OUTCOME_FAILURE_RESULTS:
            return True, f"Outcome not satisfied for session {session.id}: {evaluation.result}.", False
    # idle but no terminal outcome verdict yet (e.g. the run has not started)
    return False, None, False


#: Statuses the provider's triggers emit in their terminal event.
TRIGGER_EVENT_STATUSES = frozenset({"success", "error", "timeout"})


def validate_execute_complete_event(event: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Validate the event a deferred task resumes with, returning it if well-formed.

    The event crosses the triggerer/worker boundary through the metadata DB, so a
    resuming task can receive ``None`` or a status its handlers do not recognize
    (version skew, a custom trigger). Both must fail loudly: the ``execute_complete``
    handlers raise on ``timeout``/``error`` and treat everything else as success, so
    an unrecognized status would otherwise silently succeed.
    """
    if event is None:
        raise AnthropicTriggerEventError("Trigger error: event is None")
    if event.get("status") not in TRIGGER_EVENT_STATUSES:
        raise AnthropicTriggerEventError(
            f"Unexpected trigger event status {event.get('status')!r}: {event!r}"
        )
    return event


def evaluate_batch_counts(
    *,
    batch_id: str | None,
    canceled: int,
    errored: int,
    expired: int,
    succeeded: int,
    fail_on_partial_error: bool,
) -> None:
    """
    Apply the success/skip/fail policy for a terminal batch's request counts.

    Lives in the hook module so both :class:`AnthropicBatchOperator` and
    :class:`~airflow.providers.anthropic.sensors.batch.AnthropicBatchSensor` share it
    without an operator/sensor cross-import. Raises ``AirflowSkipException`` for a
    fully-cancelled batch, ``AnthropicBatchJobError`` when ``fail_on_partial_error`` and any
    request failed, otherwise returns (logging a warning for partial failures).
    """
    total = canceled + errored + expired + succeeded
    if total and canceled == total:
        raise AirflowSkipException(f"Batch {batch_id} was fully cancelled.")
    failed = errored + expired
    if failed:
        message = (
            f"Batch {batch_id} ended with {failed} failed request(s) "
            f"(errored={errored}, expired={expired}, succeeded={succeeded})."
        )
        if fail_on_partial_error:
            raise AnthropicBatchJobError(message)
        logger.warning("%s Successful results are still available.", message)


class AnthropicHook(BaseHook):
    """
    Use the Anthropic SDK to interact with the Claude API.

    The connection's ``password`` is used as the API key and ``host`` as an optional
    base URL (for gateways/proxies). The ``extra`` field selects the platform client
    and passes platform-specific configuration:

    - ``platform``: one of ``anthropic`` (default), ``bedrock``, ``vertex``, ``aws``, ``foundry``.
    - ``model``: default model id used when an operator/hook call omits ``model`` (lets you
      change the model without editing Dags); falls back to :data:`DEFAULT_MODEL`.
    - ``aws_region``: region for the ``bedrock`` and ``aws`` platforms.
    - ``project_id`` / ``region``: project and region for the ``vertex`` platform.
    - ``resource``: Azure resource name for the ``foundry`` platform.
    - ``anthropic_client_kwargs``: extra keyword arguments forwarded to the client
      constructor (e.g. ``timeout``, ``max_retries``, ``default_headers``).
    - ``workload_identity``: configure `Workload Identity Federation
      <https://platform.claude.com/docs/en/manage-claude/workload-identity-federation>`__
      (keyless OIDC auth) with ``identity_token_file``, ``federation_rule_id``,
      ``organization_id``, ``service_account_id`` and optional ``workspace_id`` / ``scope``.

    When the ``anthropic`` platform has no API Key and no ``workload_identity`` block, the
    client is built with no static credential so the SDK resolves them from the environment
    — supporting env-driven Workload Identity Federation and ``ant`` profiles.

    .. seealso:: https://docs.claude.com/en/api/client-sdks

    :param conn_id: :ref:`Anthropic connection id <howto/connection:anthropic>`.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "anthropic_default"
    conn_type = "anthropic"
    hook_name = "Anthropic"

    def __init__(self, conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @cached_property
    def _connection(self):
        return self.get_connection(self.conn_id)

    @cached_property
    def platform(self) -> str:
        """Return the configured platform (defaults to ``anthropic``)."""
        return (self._connection.extra_dejson.get("platform") or "anthropic").lower()

    @cached_property
    def default_model(self) -> str:
        """Default model id — connection ``extra['model']`` if set, else :data:`DEFAULT_MODEL`."""
        return self._connection.extra_dejson.get("model") or DEFAULT_MODEL

    @cached_property
    def conn(self) -> AnthropicClient:
        """Return the Anthropic client for the configured platform."""
        return self.get_conn()

    def get_conn(self) -> AnthropicClient:
        """Build and return the Anthropic client for the configured platform."""
        conn = self._connection
        extras = conn.extra_dejson
        client_kwargs = dict(extras.get("anthropic_client_kwargs", {}))
        platform = self.platform
        self.log.debug("Building Anthropic client for platform %r (conn_id=%s)", platform, self.conn_id)
        if platform == "bedrock":
            return AnthropicBedrock(aws_region=extras.get("aws_region"), **client_kwargs)
        if platform == "vertex":
            return AnthropicVertex(
                project_id=extras.get("project_id"), region=extras.get("region"), **client_kwargs
            )
        if platform == "aws":
            return AnthropicAWS(aws_region=extras.get("aws_region"), **client_kwargs)
        if platform == "foundry":
            api_key = client_kwargs.pop("api_key", None) or conn.password
            return AnthropicFoundry(api_key=api_key, resource=extras.get("resource"), **client_kwargs)
        if platform != "anthropic":
            raise AnthropicError(
                f"Unknown Anthropic platform {platform!r}. "
                "Expected one of: anthropic, bedrock, vertex, aws, foundry."
            )
        base_url = client_kwargs.pop("base_url", None) or conn.host or None
        wif = extras.get("workload_identity")
        if wif:
            return Anthropic(
                credentials=self._workload_identity_credentials(wif), base_url=base_url, **client_kwargs
            )
        api_key = client_kwargs.pop("api_key", None) or conn.password
        if api_key:
            return Anthropic(api_key=api_key, base_url=base_url, **client_kwargs)
        # No static key and no explicit federation config: let the SDK resolve credentials
        # from the environment, which supports env-driven Workload Identity Federation
        # (ANTHROPIC_FEDERATION_RULE_ID etc.) and ``ant`` profiles.
        return Anthropic(base_url=base_url, **client_kwargs)

    @staticmethod
    def _workload_identity_credentials(wif: dict[str, Any]) -> WorkloadIdentityCredentials:
        """
        Build a WIF credential from the connection ``extra['workload_identity']`` mapping.

        Exchanges a short-lived OIDC token (read from ``identity_token_file``) for an
        Anthropic access token. See
        https://platform.claude.com/docs/en/manage-claude/workload-identity-federation.
        """
        kwargs: dict[str, Any] = {
            "identity_token_provider": IdentityTokenFile(wif["identity_token_file"]),
            "federation_rule_id": wif["federation_rule_id"],
            "organization_id": wif["organization_id"],
            "service_account_id": wif["service_account_id"],
        }
        if wif.get("workspace_id"):
            kwargs["workspace_id"] = wif["workspace_id"]
        if wif.get("scope"):
            kwargs["scope"] = wif["scope"]
        return WorkloadIdentityCredentials(**kwargs)

    def _resolve_model(self, model: str | None) -> str:
        """Resolve the effective model id; Bedrock rejects a bare id, so require its prefix."""
        resolved = model or self.default_model
        # Valid Bedrock ids either start with the ``anthropic.`` provider prefix or carry a
        # region/profile prefix as a dotted component (e.g. ``us.anthropic.``, ``global.anthropic.``).
        is_bedrock_model_id = resolved.startswith("anthropic.") or ".anthropic." in resolved
        if self.platform == "bedrock" and not is_bedrock_model_id:
            raise AnthropicError(
                f"Model {resolved!r} is not a valid Amazon Bedrock model id. Bedrock ids carry a "
                "provider/region prefix (e.g. 'global.anthropic.claude-opus-4-6-v1'); set one via "
                "the 'model' argument or the connection's extra['model']."
            )
        return resolved

    def _require_first_party(self, feature: str) -> None:
        if self.platform not in FIRST_PARTY_PLATFORMS:
            raise AnthropicError(
                f"{feature} is not available on the {self.platform!r} platform. "
                "Use the first-party Anthropic API (platform='anthropic') or "
                "Claude Platform on AWS (platform='aws')."
            )

    @property
    def _first_party_conn(self) -> Anthropic:
        """
        Client cast to the first-party type for endpoints only it exposes.

        Callers must guard with :meth:`_require_first_party` first; the Bedrock/Vertex/
        Foundry clients don't expose ``beta.agents``/``beta.sessions``/``models``.
        """
        return cast("Anthropic", self.conn)

    def test_connection(self) -> tuple[bool, str]:
        """Test the Anthropic connection."""
        try:
            if self.platform in FIRST_PARTY_PLATFORMS:
                # Narrowed by the platform guard: only the first-party / AWS clients,
                # which expose the Models API, reach this branch.
                self._first_party_conn.models.list()
                return True, "Connection established!"
            # models.list() is not served on bedrock/vertex/foundry; building the
            # client validates the configuration without a paid request.
            self.get_conn()
            return True, f"Connection configured for platform {self.platform!r} (no live check available)."
        except Exception as e:
            return False, str(e)

    def create_message(
        self,
        messages: list[dict[str, Any]],
        model: str | None = None,
        max_tokens: int = 1024,
        system: str | None = None,
        **kwargs: Any,
    ) -> Message:
        """
        Create a single message response (one-shot ``messages.create``).

        :param messages: The conversation so far, as a list of message dicts.
        :param model: Model ID to use. Defaults to :attr:`default_model` (the connection's
            ``extra['model']`` or :data:`DEFAULT_MODEL`).
        :param max_tokens: Maximum number of tokens to generate.
        :param system: Optional system prompt.
        """
        params: dict[str, Any] = {
            "model": self._resolve_model(model),
            "max_tokens": max_tokens,
            "messages": messages,
            **kwargs,
        }
        if system is not None:
            params["system"] = system
        return self.conn.messages.create(**params)

    def count_tokens(
        self,
        messages: list[dict[str, Any]],
        model: str | None = None,
        system: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Return the number of input tokens the given request would consume."""
        self._require_first_party("Token counting")
        params: dict[str, Any] = {"model": model or self.default_model, "messages": messages, **kwargs}
        if system is not None:
            params["system"] = system
        return self.conn.messages.count_tokens(**params).input_tokens

    @staticmethod
    def _apply_default_model(request: dict[str, Any], default_model: str) -> dict[str, Any]:
        """
        Fill ``params['model']`` from ``default_model`` when the request omits it.

        The input dict is never mutated, and a request that sets its own ``model`` is
        returned unchanged, so a single batch can still mix models across requests.
        """
        params = request.get("params")
        if not isinstance(params, dict) or params.get("model"):
            return request
        return {**request, "params": {**params, "model": default_model}}

    def create_batch(self, requests: list[dict[str, Any]], model: str | None = None) -> MessageBatch:
        """
        Submit a Message Batch.

        :param requests: A list of ``{"custom_id": str, "params": {...}}`` dicts, where
            ``params`` is a ``messages.create`` payload (``model``, ``max_tokens``,
            ``messages``, ...). A request that omits ``model`` inherits ``model`` below,
            or the connection's ``default_model`` (``extra['model']``) when that is unset too.
        :param model: Default model id for requests that do not set their own. Falls back
            to the connection's :attr:`default_model`.
        """
        self._require_first_party("The Message Batches API")
        default_model = model or self.default_model
        prepared = [self._apply_default_model(request, default_model) for request in requests]
        # ``Request`` is a TypedDict, so the plain dicts callers build match structurally.
        return self.conn.messages.batches.create(requests=cast("Iterable[Request]", prepared))

    def get_batch(self, batch_id: str) -> MessageBatch:
        """Retrieve a Message Batch by ID."""
        self._require_first_party("The Message Batches API")
        return self.conn.messages.batches.retrieve(batch_id)

    def cancel_batch(self, batch_id: str) -> MessageBatch:
        """Request cancellation of a Message Batch."""
        self._require_first_party("The Message Batches API")
        return self.conn.messages.batches.cancel(batch_id)

    def list_batches(self, **kwargs: Any) -> Any:
        """Return a (paginated) list of Message Batches."""
        self._require_first_party("The Message Batches API")
        return self.conn.messages.batches.list(**kwargs)

    def stream_batch_results(self, batch_id: str) -> Iterator[MessageBatchIndividualResponse]:
        """
        Return a streaming iterator of per-request results, keyed by ``custom_id``.

        Results stream from the API and arrive in **arbitrary order** — key them by
        ``result.custom_id``, never by position. Results are available for 29 days
        after the batch is created. The result set can be very large: iterate and
        persist to object storage; do not materialize it into XCom.
        """
        # Return (don't ``yield``) so the platform guard fails fast at call time
        # rather than only when the caller starts iterating.
        self._require_first_party("The Message Batches API")
        return self.conn.messages.batches.results(batch_id)

    def wait_for_batch(
        self, batch_id: str, wait_seconds: float = 3, timeout: float = 24 * 60 * 60
    ) -> MessageBatch:
        """
        Poll a batch synchronously until it reaches the terminal ``ended`` status.

        :param batch_id: The batch to wait for.
        :param wait_seconds: Seconds to sleep between polls.
        :param timeout: Maximum seconds to wait before raising :class:`AnthropicBatchTimeout`.
        :return: The terminal :class:`~anthropic.types.messages.MessageBatch`.
        """
        start = time.monotonic()
        consecutive_failures = 0
        while True:
            try:
                batch = self.get_batch(batch_id)
            except Exception as e:
                # Tolerate transient poll errors (as the deferrable trigger does) so a
                # single blip does not fail — and cancel — a still-running batch whose
                # results remain recoverable for 29 days.
                consecutive_failures += 1
                if (
                    consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES
                    or time.monotonic() - start > timeout
                ):
                    raise
                self.log.warning("Polling batch %s failed (%s); retrying.", batch_id, e)
                time.sleep(wait_seconds)
                continue
            consecutive_failures = 0
            self.log.debug("Batch %s status=%s", batch_id, batch.processing_status)
            if not BatchStatus.is_in_progress(batch.processing_status):
                return batch
            if time.monotonic() - start > timeout:
                raise AnthropicBatchTimeout(
                    f"Batch {batch_id} did not reach a terminal status within {timeout} seconds."
                )
            time.sleep(wait_seconds)

    # --- Managed Agents -------------------------------------------------------
    # Agents and environments are persisted, reusable resources: create them once
    # (these helpers, the ``ant`` CLI, or a setup script) and store the IDs. The
    # operator references those IDs; it never creates an agent per run.

    def create_agent(
        self, name: str, model: str | dict[str, Any] | None = None, **kwargs: Any
    ) -> BetaManagedAgentsAgent:
        """
        Create a (reusable, versioned) Managed Agents agent. One-time setup.

        ``model`` defaults to :attr:`default_model` (the connection's ``extra['model']``
        or :data:`DEFAULT_MODEL`). Pass a mapping instead of a bare id to set the model
        config, e.g. ``{"id": "claude-opus-5", "inference_geo": "us"}``.
        """
        self._require_first_party("Managed Agents")
        agent = self._first_party_conn.beta.agents.create(
            name=name,
            model=cast("agent_create_params.Model", model or self.default_model),
            **kwargs,
        )
        self.log.debug("Created agent %s (name=%r, model=%s)", agent.id, name, model or self.default_model)
        return agent

    def create_environment(
        self, name: str, config: dict[str, Any] | None = None, **kwargs: Any
    ) -> BetaEnvironment:
        """Create a (reusable) environment for agent sessions. One-time setup."""
        self._require_first_party("Managed Agents")
        if config is None:
            config = {"type": "cloud", "networking": {"type": "unrestricted"}}
        environment = self._first_party_conn.beta.environments.create(
            name=name, config=cast("environment_create_params.Config", config), **kwargs
        )
        self.log.debug("Created environment %s (name=%r)", environment.id, name)
        return environment

    def create_session(self, agent: str, environment_id: str, **kwargs: Any) -> BetaManagedAgentsSession:
        """
        Start a session against a pre-created agent + environment.

        A ``budget`` keyword accepts an amount in USD as well as the raw API payload; see
        :func:`build_budget`.
        """
        self._require_first_party("Managed Agents")
        if "budget" in kwargs:
            if kwargs["budget"] is None:
                # ``SessionCreateParams.budget`` is not Optional -- unlike the update
                # params, create has no "clear the ceiling" semantics, and sending
                # ``"budget": null`` is rejected. Treat None as "no budget".
                kwargs.pop("budget")
            else:
                kwargs["budget"] = build_budget(kwargs["budget"])
        return self._first_party_conn.beta.sessions.create(
            agent=agent, environment_id=environment_id, **kwargs
        )

    def update_session(self, session_id: str, **kwargs: Any) -> BetaManagedAgentsSession:
        """
        Update a live session -- raise or clear its ``budget``, swap ``agent`` tools, retitle.

        Only the keywords you pass are sent, which matters because the API distinguishes
        *omitted* (preserve) from ``None`` (clear). So ``update_session(sid)`` changes
        nothing, while ``update_session(sid, budget=None)`` removes the ceiling -- the
        escape hatch for a session stopped by a model with no list price, which raising the
        budget cannot unblock.

        ``budget`` accepts an amount in USD or the raw payload (see :func:`build_budget`).
        ``agent={"tools": [...]}`` is a **full replacement** of the tool list, not a merge,
        and needs the ``mid-conversation-tool-changes-2026-07-01`` beta.
        """
        self._require_first_party("Managed Agents")
        if kwargs.get("budget") is not None:
            kwargs["budget"] = build_budget(kwargs["budget"])
        return self._first_party_conn.beta.sessions.update(session_id, **kwargs)

    def get_session(self, session_id: str) -> BetaManagedAgentsSession:
        """Retrieve a session (carries its current ``status``)."""
        self._require_first_party("Managed Agents")
        return self._first_party_conn.beta.sessions.retrieve(session_id)

    def get_session_usage(self, session_id: str) -> dict[str, Any]:
        """
        Return a JSON-serializable token/cost summary for a session.

        Plain scalars and a nested ``list_cost`` mapping rather than SDK models, so the
        result survives XCom serialization and can be queried across runs. ``amount`` is
        kept as the API's **minor-unit string** (``"44"`` is $0.44) rather than converted to
        a float, so no rounding is applied to a cost figure.

        Every field is optional server-side -- ``list_cost`` is absent when usage includes a
        model with no list price -- so missing values come back as ``None``. That absence is
        why every billable dimension is reported and not just the token totals: it is
        exactly when a caller has to reconstruct cost from usage that the breakdown must be
        complete. Cache *writes* (``cache_creation``) are billed above base input, and
        server tool calls are billed per request.
        """
        return self.summarize_usage(self.get_session(session_id))

    @staticmethod
    def summarize_usage(session: BetaManagedAgentsSession) -> dict[str, Any]:
        """
        Flatten an already-retrieved session's usage; see :meth:`get_session_usage`.

        Split out because ``sessions.archive`` also returns the session, so a caller that
        is tearing a session down can report its usage without a second request.

        Dumps the model rather than copying a fixed list of fields. The usage model sets
        ``extra="allow"``, so a billable dimension added by the API is kept on the object --
        an allowlist here would drop it silently, which is worst precisely when ``list_cost``
        is ``None`` and a caller has to price the run from the breakdown. ``mode="json"``
        keeps the result XCom-safe and leaves ``amount`` a minor-unit string.
        """
        return session.usage.model_dump(mode="json")

    def send_event(self, session_id: str, event: dict[str, Any]) -> Any:
        """Send a single event (e.g. a ``user.message`` or ``user.define_outcome``)."""
        self._require_first_party("Managed Agents")
        # Event dicts callers build match the SDK's TypedDict union structurally.
        return self._first_party_conn.beta.sessions.events.send(
            session_id, events=cast("list[BetaManagedAgentsEventParams]", [event])
        )

    def interrupt_session(self, session_id: str) -> Any:
        """
        Send ``user.interrupt`` to pause a running session.

        The API refuses to archive or delete a session while it is ``running``, so this is
        the only way to release one that is not going to stop on its own -- see
        :meth:`archive_session`.
        """
        self._require_first_party("Managed Agents")
        return self.send_event(session_id, {"type": "user.interrupt"})

    def archive_session(
        self, session_id: str, *, attempts: int = 6, wait_seconds: float = 5
    ) -> BetaManagedAgentsSession:
        """
        Archive a session (frees the server-side container). Best-effort teardown.

        Returns the archived session, which carries its final ``usage`` -- so a caller
        tearing a session down does not need a separate retrieve to report what it spent.

        A ``running`` session cannot be archived (nor deleted): the API rejects both with a
        400. Only then does this interrupt the session and retry, because a session that
        will not stop on its own otherwise accrues billable runtime with no way to release
        it. Any other failure is re-raised untouched, so a transient 5xx does not send
        ``user.interrupt`` to a session that was working fine.

        Retrying costs up to ``attempts`` further calls with ``wait_seconds`` between them
        (about 25s at the defaults), which is longer than some callers have: a killed task's
        ``on_kill`` is SIGKILLed a few seconds in, so it passes a much tighter budget.
        """
        self._require_first_party("Managed Agents")
        try:
            return self._first_party_conn.beta.sessions.archive(session_id)
        except BadRequestError as e:
            # Catching the SDK's published error type, not matching on message text: a 400
            # here is the documented "cannot archive while running" rejection.
            self.log.info("Archiving session %s failed (%s); interrupting and retrying.", session_id, e)
            self.interrupt_session(session_id)
            return self._wait_for_archive(session_id, attempts=attempts, wait_seconds=wait_seconds)

    def _wait_for_archive(
        self, session_id: str, attempts: int = 6, wait_seconds: float = 5
    ) -> BetaManagedAgentsSession:
        """Retry archiving while the interrupt takes effect; the status change is not instant."""
        for attempt in range(attempts):
            try:
                return self._first_party_conn.beta.sessions.archive(session_id)
            except Exception:
                if attempt == attempts - 1:
                    raise
                time.sleep(wait_seconds)
        raise AnthropicError(f"Could not archive session {session_id}.")  # pragma: no cover

    def _latest_idle_reason(self, session_id: str, kickoff_event_id: str | None) -> str | None:
        """
        Return the ``stop_reason`` of the newest ``session.status_idle`` event, or ``None``.

        Walks the event log newest-first. Returns ``None`` if the kickoff event is the most
        recent event (the agent has not responded yet — defeats the start race) or no idle
        event is found in the scan window.
        """
        # The SDK cursor auto-paginates (page size 20); cap the walk at 100 events so a
        # long event log can't make one poll iterate unboundedly.
        examined = 0
        for event in self._first_party_conn.beta.sessions.events.list(session_id, order="desc", limit=20):
            if kickoff_event_id is not None and event.id == kickoff_event_id:
                return None
            if event.type == "session.status_idle":
                return event.stop_reason.type
            examined += 1
            if examined >= 100:
                break
        return None

    def poll_session_completion(
        self, session_id: str, *, expect_outcome: bool = False, kickoff_event_id: str | None = None
    ) -> SessionPollResult:
        """
        Return the :class:`SessionPollResult` for one poll of a session.

        Combines the session object (status / outcome verdict) with the event log
        (``stop_reason`` of the latest idle) so a ``message`` run distinguishes genuine
        ``end_turn`` completion from ``requires_action`` / ``retries_exhausted`` /
        ``budget_reached``.

        ``stop_reason`` is the SDK's own idle stop reason, or ``None`` when the verdict did
        not come from an idle event (a ``terminated`` session, or an outcome verdict). It
        exists so callers can pick an error class without matching on the message text; pass
        it to :func:`_create_session_error`.

        .. note::
            A budget stop is classified on ``message`` runs only. An ``outcome`` run is
            judged from ``outcome_evaluations`` before the event log is consulted, so a
            budget stop there surfaces as whatever verdict the outcome recorded.
        """
        session = self.get_session(session_id)
        done, error_message, needs_event_check = evaluate_session_state(
            session, expect_outcome=expect_outcome
        )
        self.log.debug(
            "Session %s status=%s done=%s needs_event_check=%s",
            session_id,
            session.status,
            done,
            needs_event_check,
        )
        if not needs_event_check:
            return SessionPollResult(done=done, error_message=error_message, stop_reason=None)
        reason = self._latest_idle_reason(session_id, kickoff_event_id)
        if reason is None:
            return SessionPollResult(done=False, error_message=None, stop_reason=None)
        if reason == "end_turn":
            return SessionPollResult(done=True, error_message=None, stop_reason=reason)
        if reason == BUDGET_REACHED:
            # Both causes are worth naming: a session also stops with ``budget_reached``
            # when its usage includes a model with no list price, because the budget cannot
            # measure that spend -- and then raising the ceiling does not unblock it.
            return SessionPollResult(
                done=True,
                error_message=(
                    f"Session {session_id} stopped against its budget: the tracked list cost "
                    "reached the configured ceiling, or its usage included a model with no "
                    "list price (which a budget cannot measure). The operator archives the "
                    "session on this path, so it cannot be resumed -- raise the ceiling for "
                    "the next run, or drop the budget if a model has no list price."
                ),
                stop_reason=reason,
            )
        return SessionPollResult(
            done=True,
            error_message=(
                f"Session {session_id} is idle but did not complete ({reason}); "
                "configure an autonomous agent or use an outcome run."
            ),
            stop_reason=reason,
        )

    def wait_for_session(
        self,
        session_id: str,
        expect_outcome: bool = False,
        kickoff_event_id: str | None = None,
        poll_interval: float = 30,
        timeout: float = 24 * 60 * 60,
    ) -> None:
        """
        Poll a session synchronously until it completes.

        :param session_id: The session to wait for.
        :param expect_outcome: Whether the session is running a ``user.define_outcome`` loop
            (completion judged from ``outcome_evaluations``).
        :param kickoff_event_id: ID of the kickoff event, used to correlate the terminal
            idle event on a ``message`` run (defeats the start race).
        :param poll_interval: Seconds to sleep between polls.
        :param timeout: Maximum seconds to wait before raising :class:`AnthropicAgentSessionTimeout`.
        :raises AnthropicSessionBudgetExceeded: If the session stopped against its budget.
        """
        start = time.monotonic()
        consecutive_failures = 0
        while True:
            try:
                poll_result = self.poll_session_completion(
                    session_id, expect_outcome=expect_outcome, kickoff_event_id=kickoff_event_id
                )
            except Exception as e:
                # Tolerate transient poll errors (as the deferrable trigger does) so a
                # single blip does not fail — and archive — a still-running session.
                consecutive_failures += 1
                if (
                    consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES
                    or time.monotonic() - start > timeout
                ):
                    raise
                self.log.warning("Polling session %s failed (%s); retrying.", session_id, e)
                time.sleep(poll_interval)
                continue
            consecutive_failures = 0
            if poll_result.done:
                if poll_result.error_message:
                    raise _create_session_error(poll_result.error_message, poll_result.stop_reason)
                return
            if time.monotonic() - start > timeout:
                raise AnthropicAgentSessionTimeout(
                    f"Session {session_id} did not reach a terminal status within {timeout} seconds."
                )
            time.sleep(poll_interval)
