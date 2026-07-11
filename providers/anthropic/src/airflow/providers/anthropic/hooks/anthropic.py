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
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from anthropic import (
    Anthropic,
    AnthropicAWS,
    AnthropicBedrock,
    AnthropicFoundry,
    AnthropicVertex,
    IdentityTokenFile,
    WorkloadIdentityCredentials,
)

from airflow.providers.anthropic.exceptions import (
    AnthropicAgentSessionError,
    AnthropicAgentSessionTimeout,
    AnthropicBatchJobError,
    AnthropicBatchTimeout,
    AnthropicError,
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
        BetaManagedAgentsSession,
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

    def create_agent(self, name: str, model: str | None = None, **kwargs: Any) -> BetaManagedAgentsAgent:
        """
        Create a (reusable, versioned) Managed Agents agent. One-time setup.

        ``model`` defaults to :attr:`default_model` (the connection's ``extra['model']``
        or :data:`DEFAULT_MODEL`).
        """
        self._require_first_party("Managed Agents")
        agent = self._first_party_conn.beta.agents.create(
            name=name, model=model or self.default_model, **kwargs
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
        """Start a session against a pre-created agent + environment."""
        self._require_first_party("Managed Agents")
        return self._first_party_conn.beta.sessions.create(
            agent=agent, environment_id=environment_id, **kwargs
        )

    def get_session(self, session_id: str) -> BetaManagedAgentsSession:
        """Retrieve a session (carries its current ``status``)."""
        self._require_first_party("Managed Agents")
        return self._first_party_conn.beta.sessions.retrieve(session_id)

    def send_event(self, session_id: str, event: dict[str, Any]) -> Any:
        """Send a single event (e.g. a ``user.message`` or ``user.define_outcome``)."""
        self._require_first_party("Managed Agents")
        # Event dicts callers build match the SDK's TypedDict union structurally.
        return self._first_party_conn.beta.sessions.events.send(
            session_id, events=cast("list[BetaManagedAgentsEventParams]", [event])
        )

    def archive_session(self, session_id: str) -> Any:
        """Archive a session (frees the server-side container). Best-effort teardown."""
        self._require_first_party("Managed Agents")
        return self._first_party_conn.beta.sessions.archive(session_id)

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
    ) -> tuple[bool, str | None]:
        """
        Return ``(done, error_message)`` for one poll of a session.

        Combines the session object (status / outcome verdict) with the event log
        (``stop_reason`` of the latest idle) so a ``message`` run distinguishes genuine
        ``end_turn`` completion from ``requires_action`` / ``retries_exhausted``.
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
            return done, error_message
        reason = self._latest_idle_reason(session_id, kickoff_event_id)
        if reason is None:
            return False, None
        if reason == "end_turn":
            return True, None
        return True, (
            f"Session {session_id} is idle but did not complete ({reason}); "
            "configure an autonomous agent or use an outcome run."
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
        """
        start = time.monotonic()
        consecutive_failures = 0
        while True:
            try:
                done, error_message = self.poll_session_completion(
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
            if done:
                if error_message:
                    raise AnthropicAgentSessionError(error_message)
                return
            if time.monotonic() - start > timeout:
                raise AnthropicAgentSessionTimeout(
                    f"Session {session_id} did not reach a terminal status within {timeout} seconds."
                )
            time.sleep(poll_interval)
