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
Batch adapters built on pydantic-ai's public ``Model.batch_*`` methods.

One backend drives every provider pydantic-ai can batch: it encodes each request with
``batch_encode``, submits them with ``batch_create``, polls with ``batch_status``, collects with
``batch_results`` and reads each result with ``batch_decode``. The provider subclasses set only
what differs between providers (the settings they accept and how a finished job is read).

pydantic-ai's ``Batch`` handle is plain data. It is stored with the batch's state record, so the
trigger and a later worker rebuild a model from the connection and continue the same batch without
the original process.

Importing this module never imports a provider SDK: the model classes are built by
:class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`, and the SDK-specific
settings types are imported inside the methods that need them.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import queue
import threading
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine, Iterator, Mapping
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Literal, TypeVar, cast

from pydantic import TypeAdapter
from pydantic_ai.exceptions import ModelAPIError, ModelHTTPError, UserError
from pydantic_ai.messages import (
    ModelRequest,
    ModelRequestPart,
    ModelResponse,
    SystemPromptPart,
    UserPromptPart,
)
from pydantic_ai.models import ModelRequestParameters

# pydantic-ai raises this for a job that ended with no results file. Nothing public tells it from an API failure
# (the handle carries no result destination), and a test pins that it stays a ``ModelHTTPError``.
from pydantic_ai.models._batch_codec import BatchJobError
from pydantic_ai.models.batch import (
    Batch,
    BatchRequest as PydanticAIBatchRequest,
    BatchResult,
    EncodedBatchRequest,
)
from pydantic_ai.output import OutputObjectDefinition
from pydantic_ai.settings import ModelSettings
from pydantic_ai.tools import ToolDefinition

from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.exceptions import (
    LLMBatchInputError,
    LLMBatchJobError,
    LLMBatchLimitExceededError,
    LLMBatchModelMismatchError,
    LLMBatchStateReadError,
)
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.ai.utils.task_logger import get_task_logger

if TYPE_CHECKING:
    from pydantic_ai.models import Model

    from airflow.providers.common.ai.batch.base import BatchRequest
    from airflow.providers.common.ai.batch.output_schema import OutputSpec
    from airflow.sdk import Connection

log = get_task_logger()

T = TypeVar("T")

_BATCH_ADAPTER: TypeAdapter[Batch] = TypeAdapter(Batch)

#: Sampling settings with the same meaning on every provider. A key in this set (or one of a
#: provider's own ``ModelSettings`` keys) becomes a typed setting; any other key is a raw
#: provider request field.
_GENERIC_SETTINGS = frozenset(
    {
        "temperature",
        "top_p",
        "top_k",
        "seed",
        "presence_penalty",
        "frequency_penalty",
        "logit_bias",
        "stop_sequences",
    }
)

#: Request fields this adapter sets itself. Taking them from ``request_params`` would replace the
#: prompt, the model or the structured-output constraint, so they are dropped with a warning.
_MANAGED_KEYS = frozenset(
    {
        "model",
        "messages",
        "input",
        "system",
        "system_instruction",
        "contents",
        "tools",
        "tool_choice",
        "stream",
        "max_tokens",
        "max_output_tokens",
        "max_completion_tokens",
        "output_config",
        "text",
        "response_format",
    }
)

#: pydantic-ai's status for a request that never ran, as this provider names it.
_UNRUN_STATUS: dict[str, Literal["cancelled", "expired"]] = {"canceled": "cancelled", "expired": "expired"}

#: Gemini documents no request limit, and the ABC wants an int.
_NO_REQUEST_LIMIT = 2**63 - 1

#: Decoded rows a batch's results may run ahead of the task that reads them.
_RESULT_BUFFER = 64


def _run(coro: Coroutine[Any, Any, T]) -> T:
    """
    Run ``coro`` to completion from synchronous code.

    ``asyncio.run`` refuses to start inside a thread that already runs a loop, which is where a
    kill signal's ``on_kill`` can land while a poll is in flight, so that case uses a helper thread.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop in this thread. Run outside the ``except`` so an error from ``coro`` isn't chained
        # onto this probe in the task log.
        pass
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(asyncio.run, coro).result()
    return asyncio.run(coro)


def _stream(source: AsyncGenerator[T, None]) -> Iterator[T]:
    """
    Read an async generator from synchronous code without collecting it first.

    The generator runs on its own event loop in a helper thread and hands items over through a queue of
    :data:`_RESULT_BUFFER` items, so a slow reader holds the download back instead of buffering it. An
    error from the generator is raised at the reader. Closing the iterator early (or dropping it) stops the
    helper thread, which closes the generator and so anything it holds open, and waits for it to finish.
    """
    items: queue.Queue[
        tuple[Literal["item"], T] | tuple[Literal["error"], Exception] | tuple[Literal["end"], None]
    ]
    items = queue.Queue(maxsize=_RESULT_BUFFER)
    closed = threading.Event()

    def put(message: tuple[Any, Any]) -> bool:
        # A bounded put that gives up once the reader is gone, so a full queue never strands the thread.
        while not closed.is_set():
            try:
                items.put(message, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    async def produce() -> None:
        try:
            async for item in source:
                if not put(("item", item)):
                    return
            put(("end", None))
        except Exception as e:
            put(("error", e))
        finally:
            await source.aclose()

    thread = threading.Thread(target=asyncio.run, args=(produce(),), name="llm-batch-results", daemon=True)
    thread.start()
    try:
        while True:
            message = items.get()
            if message[0] == "item":
                yield message[1]
            elif message[0] == "error":
                raise message[1]
            else:
                return
    finally:
        closed.set()
        thread.join()


@functools.cache
def _typed_settings(settings_type: type) -> frozenset[str]:
    """Return the keys a request may set as typed settings: the generic ones and the provider's own."""
    return _GENERIC_SETTINGS | (set(settings_type.__annotations__) - set(ModelSettings.__annotations__))


def _error_body(error: ModelHTTPError) -> Mapping[str, Any]:
    """Return the ``error`` object of an HTTP error's body, which carries the provider's own type and message."""
    body = error.body
    inner = body.get("error") if isinstance(body, Mapping) else None
    return inner if isinstance(inner, Mapping) else {}


def _reason(error: ModelHTTPError) -> str:
    """Return the provider's own explanation of an HTTP error; pydantic-ai's message wraps it in status and model."""
    return str(_error_body(error).get("message") or error.message)


def _error_info(status: str, message: str, provider_code: str | int | None = None) -> dict[str, Any]:
    return {"type": status, "message": message, "provider_code": provider_code, "stage": "provider"}


def _failed(
    custom_id: str,
    index: int,
    status: Literal["errored", "expired", "cancelled"],
    error: Mapping[str, Any],
) -> RawResultItem:
    return RawResultItem(
        custom_id=custom_id,
        index=index,
        provider_status=status,
        model=None,
        usage=None,
        finish_reason=None,
        error=error,
        raw=None,
    )


class PydanticAIBatchAdapter(BatchAdapter):
    """
    Batch adapter for any provider whose pydantic-ai ``Model`` implements the ``batch_*`` methods.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    The adapter is bound to one connection and one ``model_id``. It builds a fresh ``Model`` for
    every operation and runs it in its own event loop, so no client crosses a loop, and it never
    keeps a connection open between operations.

    :param hook: The hook the models are built from (credentials, ``provider_kwargs``, the
        connection's extra).
    :param model_id: The batch's ``"<provider>:<model>"`` id.
    :param batch_handle: The serialized ``Batch`` handle recorded when the batch was submitted.
    :param output_spec: The task's output spec. Needed only to read results.
    :param model_factory: Builds the ``Model`` for a model id instead of the hook. For tests.
    """

    conn_types = frozenset({"pydanticai"})
    #: ``True`` when a key that is not a typed setting is passed to the provider as a raw request field.
    accepts_raw_params: ClassVar[bool] = True

    def __init__(
        self,
        *,
        hook: PydanticAIHook | None = None,
        model_id: str | None = None,
        batch_handle: str | None = None,
        output_spec: OutputSpec | None = None,
        model_factory: Callable[[str], Model] | None = None,
    ) -> None:
        super().__init__()
        self._hook = hook
        self._model_id = model_id
        self._handle = batch_handle
        self._output_spec = output_spec or build_output_spec(str)
        self._model_factory = model_factory

    @classmethod
    def from_connection(
        cls,
        conn: Connection,
        *,
        model_id: str | None = None,
        batch_handle: str | None = None,
        output_spec: OutputSpec | None = None,
    ) -> PydanticAIBatchAdapter:
        hook = PydanticAIHook.get_hook(conn.conn_id, {"model_id": model_id})
        if hook._get_fallback_conn_ids():
            log.warning(
                "Connection has fallback connections configured; batch ignores them and uses the primary",
                llm_conn_id=conn.conn_id,
            )
        return cls(hook=hook, model_id=model_id, batch_handle=batch_handle, output_spec=output_spec)

    @classmethod
    def _settings_type(cls) -> type:
        """Return the provider's ``ModelSettings`` class, whose own keys a request may set directly."""
        raise NotImplementedError(f"{cls.__name__} does not name its ModelSettings type.")

    def _require_model_id(self) -> str:
        if not self._model_id:
            raise LLMBatchInputError(
                f"The {self.name!r} batch adapter needs the batch's model_id ('<provider>:<model>') to "
                "rebuild its model."
            )
        return self._model_id

    async def _amodel(self, model_id: str) -> Model:
        if self._model_factory is not None:
            return self._model_factory(model_id)
        if self._hook is None:
            raise LLMBatchInputError(f"The {self.name!r} batch adapter was built without a connection.")
        return await self._hook.aget_primary_model(model_id)

    @asynccontextmanager
    async def _existing(self, batch_id: str) -> AsyncIterator[tuple[Model, Batch]]:
        """
        Yield the model an already-submitted batch belongs to, and its handle.

        The model comes from the batch, not from the task's ``model_id``: pydantic-ai checks the
        handle's group on every call, and OpenAI's and Gemini's groups name the model the batch was
        submitted on. That differs from the task's model when every request overrode it, or when the
        task's model changed since the batch was submitted. Only the provider prefix is taken from
        ``model_id``, because the connection that submitted the batch is the one that resolves it.
        """
        model_id = self._model_id
        if not model_id:
            raise LLMBatchStateReadError(
                f"Cannot reach batch {batch_id!r}: it was deferred by an earlier version of this provider, "
                "which did not record the model it needs. Retry the task so it re-attaches."
            )
        if not self._handle:
            async with await self._amodel(model_id) as model:
                yield model, await self._unrecorded_batch(model, batch_id)
            return
        batch = _BATCH_ADAPTER.validate_json(self._handle)
        provider = model_id.partition(":")[0]
        async with await self._amodel(f"{provider}:{batch.model_name}") as model:
            yield model, batch

    async def _unrecorded_batch(self, model: Model, batch_id: str) -> Batch:
        """Return a handle for a batch recorded without one, by an earlier version of this provider."""
        raise LLMBatchStateReadError(
            f"No pydantic-ai batch handle is recorded for batch {batch_id!r}, so it cannot be polled. "
            "It was submitted before this provider recorded handles; retry the task so it re-attaches."
        )

    def _build_settings(
        self,
        model: Model,
        *layers: Mapping[str, Any] | None,
        max_tokens: int,
    ) -> tuple[ModelSettings, set[str]]:
        """
        Turn ``request_params`` and a request's ``params`` into ``ModelSettings``.

        Typed keys become settings. Any other key is a raw provider request field and goes through
        ``extra_body``, which is how these parameters worked before the adapter used pydantic-ai.
        Returns the settings and the managed keys that were dropped.
        """
        typed = _typed_settings(self._settings_type())
        merged: dict[str, Any] = {}
        for layer in layers:
            merged.update(layer or {})
        extra_body: dict[str, Any] = {}
        if "extra_body" in merged:
            body = merged.pop("extra_body")
            if not self.accepts_raw_params:
                raise self._unsupported(model, "extra_body")
            extra_body.update(body)
        settings: dict[str, Any] = {}
        for key, value in merged.items():
            if key in typed:
                if key in model.unsupported_settings:
                    raise self._unsupported(model, key)
                settings[key] = value
            elif self.accepts_raw_params:
                extra_body[key] = value
            else:
                raise LLMBatchInputError(
                    f"Unknown request parameter {key!r} for the {self.name!r} adapter. It accepts "
                    f"{sorted(typed)}."
                )
        dropped = set(_MANAGED_KEYS & extra_body.keys())
        for key in dropped:
            del extra_body[key]
        settings["max_tokens"] = max_tokens
        if extra_body:
            settings["extra_body"] = extra_body
        # The keys were checked against the provider's settings above; a TypedDict cannot be built from a dynamic dict.
        return cast("ModelSettings", settings), dropped

    @staticmethod
    def _unsupported(model: Model, key: str) -> LLMBatchInputError:
        return LLMBatchInputError(
            f"The request parameter {key!r} is not supported by {type(model).__name__}; "
            "pydantic-ai would drop it without a warning."
        )

    @staticmethod
    def _output_parameters(model: Model, spec: OutputSpec) -> ModelRequestParameters:
        """
        Ask for ``spec``'s schema the strongest way the model supports.

        Native JSON-schema output where the model has it. Otherwise a single forced output tool,
        which is a hard format constraint too. Neither is pydantic-ai's profile default
        (``tool`` mode with no output tool sends no schema at all), so the mode is set explicitly.
        """
        if not spec.is_structured:
            return ModelRequestParameters()
        if model.profile.get("supports_json_schema_output"):
            return ModelRequestParameters(
                output_mode="native",
                output_object=OutputObjectDefinition(json_schema=spec.json_schema, name=spec.schema_name),
            )
        return ModelRequestParameters(
            output_mode="tool",
            output_tools=[
                ToolDefinition(
                    name=spec.schema_name,
                    description="Return the result using this schema.",
                    parameters_json_schema=spec.json_schema,
                    kind="output",
                )
            ],
            allow_text_output=False,
        )

    async def _encode(
        self,
        stack: AsyncExitStack,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        output_spec: OutputSpec,
        system_prompt: str,
        max_tokens: int,
        request_params: dict[str, Any] | None,
    ) -> tuple[Model, str, list[EncodedBatchRequest]]:
        """
        Encode every request locally, enforcing the provider's batch limits and a single group.

        Returns request 0's model, its id and the encoded requests. Nothing is sent.
        """
        if not requests:
            raise LLMBatchInputError("A batch needs at least one request.")
        default_id = self._require_model_id()
        # Per model id: the model, the output parameters and the settings of a request with no params of its own.
        models: dict[str, tuple[Model, ModelRequestParameters, ModelSettings]] = {}
        dropped: set[str] = set()
        encoded_requests: list[EncodedBatchRequest] = []
        first_id = ""
        total_bytes = 0
        for index, request in enumerate(requests):
            model_id = default_id
            if request_model := request.get("model"):
                self.resolve_request_model(request_model, default_bare_model=model, request_index=index)
                model_id = request_model
            if model_id not in models:
                current = await stack.enter_async_context(await self._amodel(model_id))
                settings, dropped_here = self._build_settings(current, request_params, max_tokens=max_tokens)
                dropped |= dropped_here
                models[model_id] = (current, self._output_parameters(current, output_spec), settings)
            current, parameters, settings = models[model_id]
            if request.get("params") or request.get("max_tokens"):
                settings, dropped_here = self._build_settings(
                    current,
                    request_params,
                    request.get("params"),
                    max_tokens=request.get("max_tokens") or max_tokens,
                )
                dropped |= dropped_here
            parts: list[ModelRequestPart] = []
            if system := request.get("system_prompt") or system_prompt:
                parts.append(SystemPromptPart(content=system))
            parts.append(UserPromptPart(content=request["prompt"]))
            try:
                encoded = await current.batch_encode(
                    PydanticAIBatchRequest(
                        custom_id=f"{idempotency_key}-{index}",
                        messages=[ModelRequest(parts=parts)],
                        model_settings=settings,
                        model_request_parameters=parameters,
                    )
                )
            except UserError as e:
                raise LLMBatchInputError(f"Request {index} cannot be sent as a batch: {e}") from e
            if index == 0:
                first_id = model_id
            elif encoded.group != encoded_requests[0].group:
                raise LLMBatchModelMismatchError(
                    f"Request {index} cannot share a batch with request 0: they use different models, "
                    f"credentials or endpoints, and the {self.name!r} batch API takes one of each per batch. "
                    "Use one model for the whole task, or split the input across tasks."
                )
            total_bytes += current.batch_request_size(encoded)
            encoded_requests.append(encoded)
        if dropped:
            log.warning("Ignoring request parameters the batch sets itself", keys=sorted(dropped))
        first = models[first_id][0]
        self._check_limits(first, len(requests), total_bytes)
        return first, first_id, encoded_requests

    def _check_limits(self, model: Model, request_count: int, total_bytes: int) -> None:
        max_requests = model.batch_max_requests
        if max_requests is not None and request_count > max_requests:
            raise LLMBatchLimitExceededError(
                f"The {self.name!r} batch API accepts at most {max_requests} requests; got {request_count}. "
                f"Split the input across mapped task instances with .expand() so each instance submits at "
                f"most {max_requests} requests."
            )
        max_bytes = model.batch_max_bytes
        if max_bytes is not None and total_bytes > max_bytes:
            raise LLMBatchLimitExceededError(
                f"The {self.name!r} batch API accepts a payload of at most {max_bytes} bytes; this input is "
                f"{total_bytes} bytes (the output_type schema is repeated in every request). Split the input "
                "with .expand()."
            )

    @asynccontextmanager
    async def _encoded(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        output_spec: OutputSpec,
        system_prompt: str,
        max_tokens: int,
        request_params: dict[str, Any] | None,
    ) -> AsyncIterator[tuple[Model, list[EncodedBatchRequest]]]:
        """Yield request 0's model and the encoded requests, with every model open until the block ends."""
        async with AsyncExitStack() as stack:
            first, _, encoded = await self._encode(
                stack,
                requests,
                model=model,
                idempotency_key=idempotency_key,
                output_spec=output_spec,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                request_params=request_params,
            )
            yield first, encoded

    def validate_requests(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        output_spec: OutputSpec,
        idempotency_key: str,
        system_prompt: str = "",
        max_tokens: int = 1024,
        request_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.check_custom_id_length(idempotency_key, len(requests))

        async def validate() -> None:
            async with self._encoded(
                requests,
                model=model,
                idempotency_key=idempotency_key,
                output_spec=output_spec,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                request_params=request_params,
            ):
                pass

        _run(validate())

    def submit(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        input_fingerprint: str,
        output_spec: OutputSpec,
        system_prompt: str = "",
        max_tokens: int = 1024,
        request_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> SubmitResult:
        async def create() -> Batch:
            async with self._encoded(
                requests,
                model=model,
                idempotency_key=idempotency_key,
                output_spec=output_spec,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                request_params=request_params,
            ) as (submitter, encoded):
                return await submitter.batch_create(encoded)

        batch = _run(create())
        self._handle = _BATCH_ADAPTER.dump_json(batch).decode()
        return SubmitResult(
            batch_id=batch.id,
            provider_input_ref=(batch.provider_details or {}).get("input_file"),
            handle=self._handle,
        )

    async def aget_batch(self, batch_id: str) -> BatchState:
        async with self._existing(batch_id) as (model, stored):
            return await self._describe(model, await model.batch_status(stored))

    async def _describe(self, model: Model, batch: Batch) -> BatchState:
        """Reduce a refreshed handle to this adapter's status vocabulary; a provider with finer states extends it."""
        return BatchState(
            status="completed" if batch.status == "ended" else "in_progress", counts=None, error_message=None
        )

    def get_batch(self, batch_id: str) -> BatchState:
        return _run(self.aget_batch(batch_id))

    async def acancel_batch(self, batch_id: str) -> None:
        async with self._existing(batch_id) as (model, stored):
            await model.batch_cancel(stored)

    def cancel_batch(self, batch_id: str) -> None:
        _run(self.acancel_batch(batch_id))

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        """
        Return ``None`` unconditionally: ``batch_create`` takes no metadata to find a batch by.

        A crash between "submit sent" and "response recorded" falls through to ``on_orphaned_intent``.
        """
        return None

    async def _collect(self, model: Model, batch: Batch) -> list[BatchResult]:
        """Return the results of an ended batch; a provider that can end a job with no results file overrides it."""
        return await model.batch_results(batch)

    async def _stream_results(self, model: Model, batch: Batch) -> AsyncGenerator[BatchResult, None]:
        """
        Yield the results of an ended batch one at a time.

        The default collects them as one list with ``batch_results``, which is all a provider whose SDK
        downloads the results in one call can do (Gemini). A provider that can read its results file as a
        stream overrides this.
        """
        results = await self._collect(model, batch)
        # Popping frees each result once it is handed on; the order does not matter, the merge is by index.
        while results:
            yield results.pop()

    async def _read_results(self, batch_id: str) -> AsyncGenerator[RawResultItem, None]:
        async with self._existing(batch_id) as (model, stored):
            # ``batch_results`` refuses a handle that has not ended, and the stored one is from submit time.
            batch = await model.batch_status(stored)
            if batch.status != "ended":
                raise LLMBatchJobError(f"Batch {batch_id!r} has not ended (status {batch.status!r}).")
            parameters = self._output_parameters(model, self._output_spec)
            async with aclosing(self._stream_results(model, batch)) as results:
                async for result in results:
                    if (row := self._decode_or_skip(model, result, parameters, batch_id)) is not None:
                        yield row

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        return _stream(self._read_results(batch_id))

    def _decode_or_skip(
        self, model: Model, result: BatchResult, parameters: ModelRequestParameters, batch_id: str
    ) -> RawResultItem | None:
        try:
            return self._decode_one(model, result, parameters)
        except Exception as e:
            # One malformed item must not abort the whole batch; the merge counts the index as missing.
            log.warning(
                "Skipping unparsable result item for batch",
                batch_id=batch_id,
                custom_id=result.custom_id,
                error=str(e),
            )
            return None

    @staticmethod
    def _decode_one(model: Model, result: BatchResult, parameters: ModelRequestParameters) -> RawResultItem:
        custom_id = result.custom_id
        index = int(custom_id.rsplit("-", 1)[1])
        if result.status in _UNRUN_STATUS:
            return _failed(
                custom_id,
                index,
                _UNRUN_STATUS[result.status],
                _error_info(result.status, f"batch item {result.status}"),
            )
        try:
            response = model.batch_decode(
                result,
                PydanticAIBatchRequest(custom_id=custom_id, messages=[], model_request_parameters=parameters),
            )
        except ModelAPIError as e:
            provider_code = None
            if isinstance(e, ModelHTTPError):
                provider_code = _error_body(e).get("type") or e.status_code
            return _failed(custom_id, index, "errored", _error_info(result.status, e.message, provider_code))
        details = response.provider_details or {}
        return RawResultItem(
            custom_id=custom_id,
            index=index,
            provider_status="success",
            model=response.model_name,
            usage={
                "input_tokens": response.usage.input_tokens,
                "output_tokens": response.usage.output_tokens,
            },
            finish_reason=details.get("finish_reason") or response.finish_reason,
            error=None,
            raw=response,
        )

    def extract_output(self, raw: RawResultItem, spec: OutputSpec) -> ExtractedOutput:
        response: ModelResponse = raw.raw
        if not spec.is_structured:
            return ExtractedOutput(kind="text", text=response.text or None)
        if response.tool_calls:
            return ExtractedOutput(kind="json_value", value=response.tool_calls[0].args_as_dict())
        # Native structured output arrives as the response text, which is the JSON document.
        if response.text:
            return ExtractedOutput(kind="json_text", text=response.text)
        return ExtractedOutput(kind="absent")


class AnthropicBatchAdapter(PydanticAIBatchAdapter):
    """
    Batch adapter for Anthropic's Message Batches API.

    The batch's group leaves the model out, so one batch can hold requests for different Claude
    models. Raw Messages API fields in ``request_params`` (``metadata``, ``thinking``, ...) go through
    ``extra_body``.
    """

    name = "anthropic"
    #: Anthropic's documented limit: 100,000 requests or 256 MB, whichever is reached first.
    max_requests = 100_000
    max_payload_bytes = 256_000_000
    allows_per_request_model = True

    @classmethod
    def _settings_type(cls) -> type:
        # Heavy third-party SDK: importing the Anthropic model module imports ``anthropic``.
        from pydantic_ai.models.anthropic import AnthropicModelSettings

        return AnthropicModelSettings

    async def _stream_results(self, model: Model, batch: Batch) -> AsyncGenerator[BatchResult, None]:
        # Heavy third-party SDK: importing the Anthropic model module imports ``anthropic``.
        from anthropic import AsyncAnthropic
        from pydantic_ai.models.anthropic import AnthropicModel

        client = model.client if isinstance(model, AnthropicModel) else None
        if not isinstance(client, AsyncAnthropic):
            # Not a client pydantic-ai batches through (Bedrock, Vertex): its collect raises the right error.
            async for collected in super()._stream_results(model, batch):
                yield collected
            return
        # ``batch_status`` has already checked the credentials and the group, as ``batch_results`` would.
        stream = await client.beta.messages.batches.results(batch.id)
        try:
            async for item in stream:
                result = item.result
                # Mirrors the loop in pydantic-ai's Anthropic ``batch_results``, one result at a time.
                body = result.model_dump_json() if result.type in ("succeeded", "errored") else None
                yield BatchResult(custom_id=item.custom_id, status=result.type, body=body)
        finally:
            await stream.close()

    async def _unrecorded_batch(self, model: Model, batch_id: str) -> Batch:
        """
        Rebuild the handle of a batch an earlier version of this provider submitted.

        That version stored only the batch id. Anthropic's group is the endpoint and the credential
        (not the model), so a throwaway encode on the connection's model yields the same group the
        original submission had. ``created_at`` and ``request_count`` are placeholders: the status
        call replaces them with the provider's own.
        """
        try:
            probe = await model.batch_encode(
                PydanticAIBatchRequest(
                    custom_id="probe", messages=[ModelRequest(parts=[UserPromptPart(content="probe")])]
                )
            )
        except UserError as e:
            raise LLMBatchStateReadError(
                f"Batch {batch_id!r} was recorded by an earlier version of this provider and cannot be "
                f"re-attached: {e}"
            ) from e
        return Batch(
            id=batch_id,
            model_name=model.model_name,
            provider_name=model.system,
            group=probe.group,
            status="in_progress",
            created_at=datetime.now(UTC),
            request_count=0,
        )


class GoogleBatchAdapter(PydanticAIBatchAdapter):
    """
    Batch adapter for the Gemini Developer API (Batch Mode).

    Vertex AI is not supported, because its batch prediction reads and writes Cloud Storage or BigQuery.
    The Gemini batch API has no raw request passthrough, so ``request_params`` may only hold typed settings.
    A batch's group includes the model, so every request must use the same one.
    """

    name = "google"
    max_requests = _NO_REQUEST_LIMIT
    max_payload_bytes = 2_000_000_000
    allows_per_request_model = False
    accepts_raw_params = False

    #: Job states that end a batch before it runs a request, as the status this provider reports.
    _UNRUN_STATES: ClassVar[dict[str, Literal["cancelled", "expired"]]] = {
        "JOB_STATE_CANCELLED": "cancelled",
        "JOB_STATE_EXPIRED": "expired",
    }

    @classmethod
    def _settings_type(cls) -> type:
        # Heavy third-party SDK: importing the Google model module imports ``google.genai``.
        from pydantic_ai.models.google import GoogleModelSettings

        return GoogleModelSettings

    @staticmethod
    def _job_state(batch: Batch) -> str | None:
        return (batch.provider_details or {}).get("state")

    async def _describe(self, model: Model, batch: Batch) -> BatchState:
        state = self._job_state(batch)
        # ``ended`` covers every terminal state, so the outcome is in the job's own state.
        if batch.status == "ended":
            if state in self._UNRUN_STATES:
                status = self._UNRUN_STATES[state]
                return BatchState(status=status, counts={status: batch.request_count}, error_message=None)
            if state == "JOB_STATE_FAILED":
                message = f"Gemini batch job {batch.id} failed."
                try:
                    await model.batch_results(batch)
                except BatchJobError as e:
                    message = f"{message} {_reason(e)}"
                return BatchState(status="failed", counts=None, error_message=message)
        return await super()._describe(model, batch)

    async def _collect(self, model: Model, batch: Batch) -> list[BatchResult]:
        try:
            return await super()._collect(model, batch)
        except BatchJobError as e:
            state = self._job_state(batch)
            if state in self._UNRUN_STATES:
                # The job ended before it ran a request and wrote no results file; the merge reports
                # every request as missing and the terminal event's counts name why.
                return []
            if state == "JOB_STATE_FAILED":
                raise LLMBatchJobError(f"Gemini batch job {batch.id} has no results: {_reason(e)}") from e
            raise
