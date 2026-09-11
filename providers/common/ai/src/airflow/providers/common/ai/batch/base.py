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

This module must never import a provider SDK (``openai``, ``anthropic``, ...) --
``batch/dispatch.py`` imports every registered adapter module to build the
dispatch table, and ``check-provider-yaml-valid`` imports every registered
module in turn. A stray top-level SDK import here would make that check (and
any environment without the SDK installed) fail for reasons unrelated to
dispatch.
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

    ``build_prompts()`` may return ``list[str]`` as a shorthand for
    ``list[BatchRequest]`` -- a bare string ``"foo"`` is equivalent to
    ``{"prompt": "foo"}``. Normalization to this shape happens once, as the
    first step of ``execute()``; everything downstream only sees
    ``list[BatchRequest]``.
    """

    prompt: Required[str]
    #: Per-request model override (D3). Must be written as ``"<provider>:<model>"`` (e.g.
    #: ``"anthropic:claude-3-opus"``) -- the *same* ``"<provider>:<model>"`` form the
    #: operator/decorator's own ``model_id`` uses -- never a bare model name; a bare name is
    #: rejected by :meth:`BatchAdapter.resolve_request_model` (N6). ``None`` (the default)
    #: inherits the batch-level ``model_id``. Anthropic allows a different model per request in
    #: the same batch (``allows_per_request_model = True``); OpenAI requires every request in a
    #: batch to resolve to the same model and rejects a batch with more than one distinct value.
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


@dataclass(frozen=True)
class BatchState:
    """The current status of a provider-native batch, as returned by :meth:`BatchAdapter.get_batch`."""

    status: Literal["in_progress", "completed", "failed", "expired", "cancelled"]
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
    (``f"{key16}-{index}"``, see the state-layer key derivation). ``raw`` is
    opaque here -- only the adapter's own :meth:`BatchAdapter.extract_output`
    knows how to read it.

    ``provider_status`` has four values, not two: a provider that reports
    per-item outcomes for a batch that timed out or was cancelled (Anthropic
    does; OpenAI does not -- see each adapter's ``_parse_result_item``/
    ``_parse_result_line``) must be able to say "this specific item expired/was
    cancelled" rather than folding it into ``"errored"``, which the results
    layer would otherwise count as an ordinary provider-side failure
    indistinguishable from rate limiting or a bad request.
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

    Keeping ``json_text`` and ``json_value`` distinct avoids a needless
    ``json.dumps``/``json.loads`` round trip for adapters that already get a
    parsed value.
    """

    kind: Literal["text", "json_text", "json_value", "absent"]
    text: str | None = None
    value: Any | None = None


class BatchAdapter(ABC):
    """
    Adapter contract between the common.ai batch surface and one provider's batch API.

    ``batch/dispatch.py`` selects a concrete subclass by ``conn_type`` (auth)
    and ``model_id`` prefix (request shape). The ABC is intentionally the only
    coupling point: operator, trigger, and the state/results layers depend on
    this interface, never on a concrete adapter module, so a future adapter
    (e.g. Azure OpenAI, or a pydantic-ai-native implementation) plugs in
    without touching them.
    """

    name: ClassVar[str]
    max_requests: ClassVar[int]
    max_payload_bytes: ClassVar[int]
    allows_per_request_model: ClassVar[bool]

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

        Must check ``len(requests)`` against ``max_requests`` and the
        serialized request size (including the per-request output directive
        built from ``output_spec`` -- a large schema is repeated in every
        request and can dominate the payload) against ``max_payload_bytes``.
        Must also check that the worst-case ``custom_id`` this batch would
        generate (``idempotency_key`` plus separator plus the largest index)
        stays within the provider's own id length limit (N7 -- Anthropic
        documents ``^[a-zA-Z0-9_-]{1,64}$``; not currently reachable given
        today's ``max_requests`` values, but a shrunk ``key16``/grown
        ``max_requests`` in the future must not silently produce an invalid
        ``custom_id``). Raise a subclass of ``LLMBatchInputError`` naming the
        limit, the actual count/size, and the ``.expand()`` remedy.

        ``**kwargs`` carries the same batch-level request-building context as
        :meth:`submit` (``system_prompt``, ``max_tokens``, ``request_params``)
        so the serialized-size estimate here matches what ``submit`` actually
        sends.
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

        ``input_fingerprint`` must be recorded somewhere the provider lets a later
        :meth:`find_orphaned_batch` read it back (N1) -- for a provider with batch-level
        ``metadata`` (OpenAI), that means putting it there alongside ``idempotency_key``.
        ``idempotency_key`` alone identifies the *task instance*, not *this* submission of it:
        it is stable across a ``clear`` even when the prompts changed, so a provider-side lookup
        keyed only on it risks recovering an older, unrelated batch and returning its results as
        if they belonged to the current input.
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
        Best-effort recovery for a Phase A orphan (§5.3, Risk R1).

        A submit whose response was never received -- a crash between "request sent" and
        "response recorded". Every request's ``custom_id`` is prefixed with ``idempotency_key``
        (§5.4), and the plan's own submit-time metadata (where the provider supports it) also
        carries it -- this method is what makes that recoverable instead of decorative.

        Three preconditions must **all** hold before a hit is returned (N1):

        - ``idempotency_key`` matches. Alone, this is not enough: the identity key is stable
          across a ``clear`` with *different* prompts (it is derived only from
          dag/task/run/map_index, never from the request content), so matching on it alone risks
          recovering a completely different, older submission and silently handing back its
          results as if they belonged to the current input.
        - ``input_fingerprint`` also matches -- this is what actually ties a candidate batch to
          *this* input, not just this task instance.
        - The candidate was created at or after ``not_before`` (an ISO 8601 timestamp -- the Phase
          A intent record's own write time). Without this, an adapter whose listing order is not
          guaranteed recency (see ``OpenAIBatchAdapter._ORPHAN_SCAN_LIMIT``) could pick an older
          batch that happens to share both the key and the fingerprint (e.g. a genuine repeat
          submission of identical content on an earlier attempt of the same task instance) instead
          of the one the current, still-orphaned submit attempt actually produced.

        Return the provider's batch id if a matching in-flight or completed batch is found (when
        multiple candidates satisfy all three, prefer the most recently created one), else
        ``None``. An adapter that genuinely has no way to correlate an orphan back to a batch (no
        queryable metadata/listing) must return ``None`` unconditionally and say so in its own
        docstring -- never fabricate a lookup that cannot actually distinguish batches.
        """

    def resolve_request_model(
        self, request_model: str | None, *, default_bare_model: str, request_index: int
    ) -> str:
        """
        Resolve a per-request ``model`` override (§1, D3) to this adapter's bare model name.

        ``request_model`` must be ``None`` (inherit the batch-level default, already bare) or a
        ``"<provider>:<model>"`` string using the *same* dispatch vocabulary as the batch-level
        ``model_id`` (§3) -- its prefix must equal ``self.name``. A request naming a different
        provider (e.g. ``"anthropic:claude-3-opus"`` inside an OpenAI batch) is rejected here,
        eagerly, before any network call -- not silently sent to the wrong provider's model
        namespace, and not silently treated as this adapter's own uniform-model check trivially
        "agreeing" because the (wrong) value happens to repeat across every request.

        Concrete, not abstract: both adapters need the exact same cross-adapter check: OpenAI on
        top of its own "one model per batch" rule, Anthropic on its own (where every request may
        otherwise use a distinct model).
        """
        if request_model is None:
            return default_bare_model
        prefix, sep, bare = request_model.partition(":")
        if not sep or prefix != self.name:
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
        Reject a batch whose worst-case ``custom_id`` would exceed ``max_length`` (N7).

        The worst case is ``f"{idempotency_key}-{index}"`` for the largest index. Anthropic's
        "Create a Message Batch" API docs (verified 2026-09-11) specify ``custom_id`` as
        ``pattern: ^[a-zA-Z0-9_-]{1,64}$``, ``minLength: 1``, ``maxLength: 64`` (also
        ``requests``: ``maxItems: 100000``, matching this codebase's own ``max_requests``). Not
        reachable today: a 16-character ``key16`` plus a separator plus the largest index this
        adapter's own ``max_requests`` allows (5 digits at 100,000 requests) is at most 22
        characters. Checked anyway, concretely on the ABC, so a future change to ``key16``'s
        length or either adapter's ``max_requests`` cannot silently start producing an invalid
        ``custom_id`` instead of a clear pre-submit rejection.
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

    Shared by the trigger (which only has provider-reported counts) and the
    results layer's manifest assembly (which has the full breakdown,
    including the validation-stage ``invalid_output``/``missing`` buckets) so
    both describe "some requests did not produce a usable result" the same
    way, without one importing the other.
    """
    total = sum(counts.values())
    succeeded = counts.get("succeeded", 0)
    return "succeeded" if total > 0 and succeeded == total else "partial"


#: ``BatchState.status`` values that mean "still running" -- everything else is terminal.
#: Shared by the trigger's async polling loop and the operator's synchronous polling loop
#: (``deferrable=False``) so both draw the in-progress/terminal line the same way.
IN_PROGRESS_STATUSES = frozenset({"in_progress"})

#: ``BatchState.status`` -> the trigger/operator event vocabulary (``"success"`` /
#: ``"failed"`` / ``"expired"`` / ``"timeout"`` / ``"cancelled"`` / ``"error"``), deliberately
#: aligned with the OpenAI-side ``openai-batch-termination-reason`` work in flight in parallel
#: (the plan's packaging-boundary section), so both sides call the same outcome by the same name.
#:
#: "expired" is deliberately its **own** event status, not folded into "timeout": a provider-side
#: expiry is a terminal, already-billed outcome that typically still carries partial results
#: (whatever finished before the SLA lapsed), so it must route through the same
#: fetch/validate/merge/land path as "success" -- discarding those results by treating expiry as
#: a plain timeout would throw away money already spent. "timeout" is reserved for *our own*
#: wall-clock defer/poll budget running out while the batch is still ``in_progress`` on the
#: provider side -- a fundamentally different situation (the batch may still complete later).
TERMINAL_STATUS_MAP: dict[str, str] = {
    "completed": "success",
    "failed": "failed",
    "expired": "expired",
    "cancelled": "cancelled",
}
