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
"""
Provider-agnostic types and the adapter contract for ``@task.llm_batch``.

This module must never import a provider SDK (``openai``, ``anthropic``, ...):
``batch/dispatch.py`` and the provider-yaml validation check import every
registered module, so a top-level SDK import here would make them fail in any
environment without that SDK installed.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from typing_extensions import Required, TypedDict

from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, LLMBatchModelMismatchError

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.output_schema import OutputSpec


class BatchRequest(TypedDict, total=False):
    """
    One input item for ``@task.llm_batch``.

    A bare string ``"foo"`` is shorthand for ``{"prompt": "foo"}``; the operator
    normalizes to this shape before anything else sees the input.
    """

    prompt: Required[str]
    #: Per-request model override, written as ``"<provider>:<model>"`` (the same form the
    #: operator's own ``model_id`` uses). ``None`` inherits the batch-level ``model_id``.
    #: Anthropic allows a different model per request in the same batch; OpenAI requires
    #: every request in a batch to resolve to the same model.
    model: str | None
    system_prompt: str | None
    max_tokens: int | None
    params: dict[str, Any]


@dataclass(frozen=True)
class SubmitResult:
    """Returned by :meth:`BatchAdapter.submit` once the provider has accepted the batch."""

    batch_id: str
    #: OpenAI's uploaded input file id, kept for cleanup; ``None`` for adapters
    #: (e.g. Anthropic) that submit requests inline without an upload step.
    provider_input_ref: str | None


#: The small status vocabulary every adapter collapses its provider's job statuses into.
BatchStatus = Literal["in_progress", "completed", "failed", "expired", "cancelled"]


@dataclass(frozen=True)
class BatchState:
    """The current status of a provider-native batch, as returned by :meth:`BatchAdapter.get_batch`."""

    status: BatchStatus
    #: Provider-reported per-status counts, when the provider exposes them
    #: before results are downloaded. ``None`` if the provider only reports
    #: an aggregate status with no breakdown until results are streamed.
    counts: Mapping[str, int] | None
    #: Populated when ``status == "failed"``; the provider's own explanation.
    error_message: str | None


@dataclass(frozen=True)
class RawResultItem:
    """
    One provider-native per-request result, before output extraction or validation.

    ``iter_results`` yields these keyed by position (``index``), parsed back
    out of the ``custom_id`` each adapter wrote at submit time
    (``f"{key16}-{index}"``). ``raw`` is opaque here; only the adapter's own
    :meth:`BatchAdapter.extract_output` knows how to read it.

    ``provider_status`` keeps ``"expired"`` and ``"cancelled"`` distinct from
    ``"errored"``: both providers report per-item expiry (Anthropic as a result
    type, OpenAI as an error-file line with code ``batch_expired``), and folding
    it into ``"errored"`` would make an SLA lapse indistinguishable from a rate
    limit or a bad request.
    """

    custom_id: str
    index: int
    provider_status: Literal["success", "errored", "expired", "cancelled"]
    model: str | None
    usage: Mapping[str, int] | None
    finish_reason: str | None
    #: Provider-reported detail; populated when ``provider_status != "success"``.
    error: Mapping[str, Any] | None
    raw: Any


@dataclass(frozen=True)
class ExtractedOutput:
    """
    The provider-agnostic shape adapters translate their native response into.

    ``extract_output`` picks exactly one ``kind``:

    - ``"text"``: plain-text output (``output_type is str``); ``text`` set.
    - ``"json_text"``: a JSON document as a string that still needs parsing
      (e.g. OpenAI's ``response_format`` content); ``text`` set.
    - ``"json_value"``: an already-parsed JSON-native value (e.g. Anthropic's
      tool-use ``input``, which arrives as a dict); ``value`` set.
    - ``"absent"``: the model did not produce structured output at all (e.g.
      no ``tool_use`` block came back even though one was requested); ``text``
      may still carry leftover plain-text content for diagnostics.
    """

    kind: Literal["text", "json_text", "json_value", "absent"]
    text: str | None = None
    value: Any | None = None


class BatchAdapter(ABC):
    """
    Adapter contract between the common.ai batch surface and one provider's batch API.

    ``batch/dispatch.py`` selects a concrete subclass by the ``model_id`` prefix
    (request shape) and checks the connection type against
    :attr:`conn_types` (auth). Operator, trigger, and the state/results layers
    depend on this interface only, so another package can register its own
    adapter (see :func:`~airflow.providers.common.ai.batch.dispatch.register_adapter`)
    without touching them.
    """

    #: The ``model_id`` prefix this adapter serves (``"openai"`` in ``"openai:gpt-5"``).
    name: ClassVar[str]
    #: Airflow connection types whose ``password``/``host`` this adapter can turn into
    #: credentials. Dispatch rejects any other connection type with a clear error.
    conn_types: ClassVar[frozenset[str]] = frozenset({"pydanticai"})
    max_requests: ClassVar[int]
    max_payload_bytes: ClassVar[int]
    allows_per_request_model: ClassVar[bool]

    def __init__(
        self, *, api_key: str | None = None, base_url: str | None = None, client: Any | None = None
    ) -> None:
        """
        Build the adapter from an Airflow connection's credentials.

        Dispatch constructs every adapter this way (``password`` as ``api_key``,
        ``host`` as ``base_url``), so a subclass must accept these keyword
        arguments. ``client`` injects a pre-built SDK client or a test double.
        """
        self._client: Any = client

    @abstractmethod
    def validate_requests(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        output_spec: OutputSpec,
        idempotency_key: str,
        **kwargs: Any,
    ) -> None:
        """
        Reject an over-limit or otherwise invalid batch before any network call.

        Must check ``len(requests)`` against ``max_requests`` and the serialized
        request size (including the per-request output directive built from
        ``output_spec``, which a large schema repeats in every request) against
        ``max_payload_bytes``, and call :meth:`check_custom_id_length`. Raise a
        subclass of ``LLMBatchInputError`` naming the limit, the actual
        count/size, and the ``.expand()`` remedy.

        ``**kwargs`` carries the same batch-level request-building context as
        :meth:`submit` (``system_prompt``, ``max_tokens``, ``request_params``)
        so the serialized-size estimate matches what ``submit`` sends.
        """

    @abstractmethod
    def submit(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        input_fingerprint: str,
        output_spec: OutputSpec,
        **kwargs: Any,
    ) -> SubmitResult:
        """
        Upload/submit the batch and return the provider's batch id.

        Where the provider offers batch-level metadata (OpenAI), record both
        ``idempotency_key`` and ``input_fingerprint`` there so
        :meth:`find_orphaned_batch` can read them back. The key alone identifies
        the task instance, not this submission of it: it is stable across a
        ``clear`` even when the prompts changed.
        """

    @abstractmethod
    def get_batch(self, batch_id: str) -> BatchState:
        """Return the current status of a submitted batch. Synchronous; the trigger wraps it in ``to_thread``."""

    @abstractmethod
    def cancel_batch(self, batch_id: str) -> None:
        """Request cancellation of a batch."""

    @abstractmethod
    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        """
        Return (not ``yield``) a streaming iterator of per-request results.

        Returning a plain iterator rather than using ``yield`` in this method
        makes an invalid ``batch_id`` (or any other call-time failure) raise
        immediately, instead of only once the caller starts iterating.
        """

    @abstractmethod
    def build_output_directive(self, spec: OutputSpec) -> dict[str, Any]:
        """
        Translate ``spec.json_schema`` into this provider's structured-output request fields.

        Returns ``{}`` when ``spec.is_structured`` is ``False``. The result is
        merged into every request's body/params by the adapter itself.
        """

    @abstractmethod
    def extract_output(self, raw: RawResultItem, spec: OutputSpec) -> ExtractedOutput:
        """Pull the model's output out of a provider-native result, in the shape ``ExtractedOutput`` describes."""

    @abstractmethod
    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        """
        Best-effort recovery of a batch whose submit response was never recorded.

        Return the provider's batch id if a batch exists whose recorded
        ``idempotency_key`` **and** ``input_fingerprint`` both match and which
        was created at or after ``not_before`` (an ISO 8601 timestamp, the
        intent record's own write time); prefer the most recently created
        candidate. All three conditions are needed: the key is stable across a
        ``clear`` with different prompts, and an unordered listing could
        otherwise return an older submission of identical content.

        Return ``None`` if nothing matches. An adapter with no way to correlate
        an orphan back to a batch must return ``None`` unconditionally and say
        so in its docstring. Raise if the lookup itself failed (network,
        auth); the operator treats that as "unknown", never as "absent".
        """

    def close(self) -> None:
        """
        Release the underlying SDK client's connection pool.

        The default closes ``self._client`` if the adapter set one and it has
        a ``close()`` method, which both the OpenAI and Anthropic clients do.
        Callers wrap adapter use in ``try``/``finally`` so the pool is freed
        deterministically rather than by garbage collection.
        """
        client = getattr(self, "_client", None)
        close = getattr(client, "close", None)
        if callable(close):
            close()

    def resolve_request_model(
        self, request_model: str | None, *, default_bare_model: str, request_index: int
    ) -> str:
        """
        Resolve a per-request ``model`` override to this adapter's bare model name.

        ``request_model`` must be ``None`` (inherit the batch-level default,
        already bare) or a ``"<provider>:<model>"`` string whose prefix equals
        :attr:`name`. A request naming a different provider is rejected before
        any network call rather than sent to the wrong provider's model
        namespace.
        """
        if request_model is None:
            return default_bare_model
        prefix, sep, bare = request_model.partition(":")
        if not sep or prefix != self.name or not bare:
            raise LLMBatchModelMismatchError(
                f"Request {request_index} sets model={request_model!r}, which does not resolve "
                f"to the {self.name!r} adapter this batch is using. Per-request model overrides "
                f"must be written as '{self.name}:<model>', matching the batch's own model_id prefix."
            )
        return bare

    def check_custom_id_length(
        self, idempotency_key: str, request_count: int, *, max_length: int = 64
    ) -> None:
        """
        Reject a batch whose worst-case ``custom_id`` would exceed ``max_length``.

        Anthropic documents ``custom_id`` as ``^[a-zA-Z0-9_-]{1,64}$``. A
        16-character key plus a separator plus a 5-digit index is at most 22
        characters, so this is not reachable today; it exists so a future
        change to the key length or ``max_requests`` fails clearly before
        submit instead of producing an invalid ``custom_id``.
        """
        if request_count <= 0:
            return
        worst_case = f"{idempotency_key}-{request_count - 1}"
        if len(worst_case) > max_length:
            raise LLMBatchLimitExceededError(
                f"The custom_id this batch would generate ({worst_case!r}, {len(worst_case)} "
                f"characters) exceeds the {max_length}-character limit. Split the input with "
                ".expand() so each instance submits fewer requests."
            )


def evaluate_batch_counts(counts: Mapping[str, int]) -> Literal["succeeded", "partial"]:
    """
    Reduce a per-status count breakdown to a single terminal reason.

    Shared by the trigger (provider-reported counts) and the results layer's
    manifest assembly (the full breakdown, including ``invalid_output`` and
    ``missing``) so both describe "some requests did not produce a usable
    result" the same way.
    """
    total = sum(counts.values())
    succeeded = counts.get("succeeded", 0)
    return "succeeded" if total > 0 and succeeded == total else "partial"


#: ``BatchState.status`` values that mean "still running"; everything else is terminal.
IN_PROGRESS_STATUSES = frozenset({"in_progress"})

#: ``BatchState.status`` -> the event vocabulary the trigger and the operator's sync poll
#: loop share (``"success"``/``"failed"``/``"expired"``/``"cancelled"``, plus ``"timeout"``
#: and ``"error"`` which the poll loop itself produces).
#:
#: ``"expired"`` is its own event status, not folded into ``"timeout"``: a provider-side
#: expiry is terminal and already billed, and typically carries partial results that must
#: still be fetched and landed. ``"timeout"`` is reserved for our own wall-clock budget
#: running out while the batch is still ``in_progress`` on the provider side.
TERMINAL_STATUS_MAP: dict[str, str] = {
    "completed": "success",
    "failed": "failed",
    "expired": "expired",
    "cancelled": "cancelled",
}
