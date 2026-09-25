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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ValidationError

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.openai.exceptions import OpenAIBatchJobException
from airflow.providers.openai.hooks.openai import OpenAIHook, validate_execute_complete_event
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

if TYPE_CHECKING:
    from openai.types.responses import Response

    from airflow.providers.common.compat.sdk import Context


def _get_structured_response_details(response: Any) -> str:
    """Return API-reported context for a structured-response failure."""
    details = [f"status={response.status!r}"]
    if response.error is not None:
        details.append(f"error={response.error!r}")
    if response.incomplete_details is not None:
        details.append(f"incomplete_details={response.incomplete_details!r}")

    refusals = [
        content.refusal
        for output in response.output
        if output.type == "message"
        for content in output.content
        if content.type == "refusal"
    ]
    if refusals:
        details.append(f"refusal={'; '.join(refusals)!r}")
    else:
        output_types = [output.type for output in response.output]
        if output_types:
            details.append(f"output_types={output_types!r}")
    return ", ".join(details)


class OpenAIEmbeddingOperator(BaseOperator):
    """
    Operator that accepts input text to generate OpenAI embeddings using the specified model.

    :param conn_id: The OpenAI connection ID to use.
    :param input_text: The text to generate OpenAI embeddings for. This can be a string, a list of strings,
                    a list of integers, or a list of lists of integers.
    :param model: The OpenAI model to be used for generating the embeddings.
    :param embedding_kwargs: Additional keyword arguments to pass to the OpenAI `create_embeddings` method.

    Returns one embedding for a single string or token array, and one embedding per item for a batch.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenAIEmbeddingOperator`
        For possible options for `embedding_kwargs`, see:
        https://platform.openai.com/docs/api-reference/embeddings/create
    """

    template_fields: Sequence[str] = (
        "input_text",
        "conn_id",
    )

    def __init__(
        self,
        conn_id: str,
        input_text: str | list[str] | list[int] | list[list[int]],
        model: str = "text-embedding-3-small",
        embedding_kwargs: dict | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_text = input_text
        self.model = model
        self.embedding_kwargs = embedding_kwargs or {}

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> list[float] | list[list[float]]:
        if not self.input_text or not isinstance(self.input_text, (str, list)):
            raise ValueError(
                "The 'input_text' must be a non-empty string, list of strings, list of integers, or list of lists of integers."
            )
        self.log.info("Generating embeddings for the input text of length: %d", len(self.input_text))
        embeddings = self.hook.create_embeddings(self.input_text, model=self.model, **self.embedding_kwargs)
        self.log.info("Generated embeddings for %d items", len(embeddings))
        return embeddings


class OpenAIResponseOperator(BaseOperator):
    """
    Operator that generates a model response using the OpenAI Responses API.

    The operator is synchronous and returns the response's aggregated output text, or, when
    ``text_format`` is set, the structured output parsed into that Pydantic model (see
    ``text_format`` below). The response id is also pushed to XCom (see below), so a downstream
    task can pick it up for ``previous_response_id`` chaining without going through the hook. For
    ``background=True`` responses, or access to the full response object, use
    :class:`~airflow.providers.openai.hooks.openai.OpenAIHook` directly.

    ``max_output_tokens`` caps the number of tokens generated for the response; ``max_tool_calls``
    caps the number of built-in tool calls the model may make. Both limits are enforced by the
    OpenAI API itself -- OpenAI exposes no monetary cost limit on the Responses API, so this
    operator has no cost cap. For a monetary limit, use
    :doc:`apache-airflow-providers-common-ai:index` instead. When ``max_output_tokens`` is hit, the
    request does not fail: the response comes back with ``status="incomplete"`` -- but
    ``output_text`` is not guaranteed to contain any content, since a reasoning model can spend
    the entire ceiling on reasoning tokens without producing visible output. Hitting
    ``max_tool_calls`` is different: the OpenAI SDK documents it as silently dropping further
    tool calls, with no ``status`` change and no ``incomplete_details`` -- a run truncated this
    way looks identical to a clean one in both the logs and ``return_value``.

    :param conn_id: The OpenAI connection ID to use.
    :param input_text: The input prompt for the model. This can be a string or a structured list of
        input items.
    :param model: The OpenAI model to use.
    :param response_kwargs: Additional keyword arguments to pass to the OpenAI ``create_response``
        method, or ``parse_response`` when ``text_format`` is set (for example ``instructions``,
        ``tools``, ``conversation`` or ``previous_response_id``). Templated, so values (e.g.
        ``previous_response_id``) may reference upstream XCom.
        Do not set ``background`` or ``stream`` here: ``background=True`` returns before the response
        completes, so this operator logs a warning and the returned output text may be empty, while
        ``stream=True`` returns an object without ``status`` or ``output_text``, so the task raises
        ``AttributeError``. See :ref:`howto/operator:OpenAIResponseOperator` for these and other
        options this operator can pass through, such as ``truncation`` and ``metadata``. ``max_output_tokens``
        and ``max_tool_calls`` are special-cased when present as keys here -- see their own ``:param:``
        entries below for the exact validation, coercion, and blank-as-unset rules that apply to them.
    :param max_output_tokens: Optional upper bound on the number of tokens generated for the
        response. Templated, so it renders to a string; accepts an ``int`` or a string containing one.
        Must be a positive integer -- an invalid value raises instead of silently disabling the
        ceiling. A literal ``bool``, ``float``, or ``int`` value is validated when the operator is
        constructed; any other non-string literal (for example ``Decimal`` or ``Fraction``) is
        coerced -- and rejected if invalid -- only when the task executes. A string value --
        whether a template or a plain literal string -- is also validated when the task executes,
        after templating has resolved it. A blank or whitespace-only rendered value (for example
        ``{{ params.tokens | default('', true) }}`` rendering to ``''``) is treated as unset,
        disabling the ceiling; the literal strings ``"None"``, ``"none"`` and ``"null"`` are **not**
        treated as blank and still raise. A value that was supplied but resolves to ``None`` (for
        example an unresolved ``XComArg``, or a Jinja-native-rendered null) also raises -- it is not
        treated as unset. Mutually exclusive with ``max_output_tokens`` in ``response_kwargs`` --
        this is checked when the operator is constructed, regardless of what the templated value
        later renders to. These same validation, type-coercion, blank-as-unset, and
        supplied-but-``None``-raises rules apply identically when ``max_output_tokens`` is set as a
        key directly inside ``response_kwargs`` instead of passed as this operator argument -- with
        one difference: for that key, blank-as-unset means the key is removed from the payload
        passed to ``create_response`` (rather than never being added, as happens for this operator
        argument), and a present key whose value is a literal ``None`` still raises, since presence
        of the key -- not the value -- is what "supplied" means for that path.
    :param max_tool_calls: Optional upper bound on the number of built-in tool calls the model may
        make while generating the response. Same templating, type, validation, blank-as-unset, and
        mutual-exclusion rules as ``max_output_tokens``.
    :param text_format: Optional Pydantic ``BaseModel`` subclass describing the expected structured
        output. When set, the operator calls ``parse_response`` instead of ``create_response`` and
        returns the parsed model's ``model_dump(mode="json")``, so enums, dates and other non-JSON
        field types reach XCom as their JSON representations. The task fails with ``ValueError``
        rather than returning partial data when the response did not complete (for example because
        ``max_output_tokens`` was reached), when it carries no parsed output (for example a
        refusal), or when the SDK cannot validate the output against the model.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenAIResponseOperator`
        For possible options, see:
        https://platform.openai.com/docs/api-reference/responses/create

    When ``do_xcom_push`` is enabled (the default), ``execute`` also pushes two XCom keys:
    ``response_id`` (the response's ID) and ``usage`` (the result of
    ``ResponseUsage.model_dump()``, or ``None`` when the API omits it). When ``usage`` is
    not ``None`` it also carries a ``try_number`` key recording which attempt produced it --
    XCom is cleared at the start of every attempt, so this makes it visible that the value
    only reflects the current attempt rather than a silently under-reported total across
    retries. With ``text_format`` set, both keys are pushed before the structured output is
    checked, so a response that then fails the task still records its id and token usage.
    Both XCom pushes are skipped when ``do_xcom_push=False``.
    """

    template_fields: Sequence[str] = (
        "input_text",
        "response_kwargs",
        "max_output_tokens",
        "max_tool_calls",
        "conn_id",
    )

    _TOKEN_CEILING_PARAM_NAMES: ClassVar[tuple[str, ...]] = ("max_output_tokens", "max_tool_calls")

    def __init__(
        self,
        conn_id: str,
        input_text: str | list[Any],
        model: str = "gpt-4o-mini",
        response_kwargs: dict | None = None,
        *,
        max_output_tokens: int | str | None = None,
        max_tool_calls: int | str | None = None,
        text_format: type[BaseModel] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_text = input_text
        self.model = model
        self.response_kwargs = response_kwargs or {}
        self.max_output_tokens = max_output_tokens
        self.max_tool_calls = max_tool_calls
        self.text_format = text_format
        self._supplied_ceilings: frozenset[str] = frozenset(
            name for name in self._TOKEN_CEILING_PARAM_NAMES if getattr(self, name) is not None
        )
        self._validate_no_response_kwargs_conflict()
        self._validate_literal_ceiling_values()
        self._validate_text_format()

    def _validate_text_format(self) -> None:
        """
        Reject a ``text_format`` that is not a Pydantic ``BaseModel`` subclass.

        The SDK also accepts other Pydantic-compatible types, such as a ``pydantic.dataclasses``
        class, but those have no ``model_dump``: they would complete the billed API call and only
        then fail. Checking when the operator is constructed rejects them before any request.
        """
        if self.text_format is not None and not (
            isinstance(self.text_format, type) and issubclass(self.text_format, BaseModel)
        ):
            raise TypeError(
                f"Task {self.task_id!r}: 'text_format' must be a Pydantic BaseModel subclass, "
                f"got {self.text_format!r}."
            )

    def _validate_no_response_kwargs_conflict(self) -> None:
        """Reject a ceiling set both as an operator argument and in ``response_kwargs``."""
        for param_name in self._TOKEN_CEILING_PARAM_NAMES:
            value = getattr(self, param_name)
            if value is not None and param_name in self.response_kwargs:
                raise ValueError(
                    f"Task {self.task_id!r}: {param_name!r} was set both as an operator argument "
                    "and in 'response_kwargs'; set it in only one place."
                )

    def _validate_literal_ceiling_values(self) -> None:
        """
        Eagerly validate a ceiling value that is already a final literal, not a template.

        Only ``bool``, ``float``, and ``int`` are recognized as literals here -- these are the raw
        values passed at construction, before any templating runs, so an invalid one is rejected
        when the operator is constructed instead of surfacing only when the task runs. Anything
        else (``str`` templates awaiting ``render_template_fields()``, or template values such as
        ``XComArg`` that resolve later -- including a ``bool``, ``float``, or ``int`` produced by
        Jinja's native rendering with ``render_template_as_native_obj=True``) must wait for
        ``_build_response_kwargs()`` at ``execute()`` time. Looks for a literal in either place a
        ceiling can be set -- the operator argument, or a key set natively in ``response_kwargs`` --
        since ``_validate_no_response_kwargs_conflict()`` (run just before this) already guarantees
        at most one source per ``param_name``.
        """
        for param_name in self._TOKEN_CEILING_PARAM_NAMES:
            operator_value = getattr(self, param_name)
            if operator_value is not None and isinstance(operator_value, (bool, float, int)):
                self._coerce_token_ceiling(param_name, operator_value)
            # A literal None native to response_kwargs is deliberately not validated here:
            # isinstance(None, (bool, float, int)) is already False, so it falls through
            # untouched -- it's rejected later, at execute()-time in _build_response_kwargs(),
            # which treats a present-but-None key as "supplied but resolved to None".
            native_value = self.response_kwargs.get(param_name)
            if native_value is not None and isinstance(native_value, (bool, float, int)):
                self._coerce_token_ceiling(param_name, native_value)

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id)

    @staticmethod
    def _coerce_token_ceiling(param_name: str, value: int | float | str) -> int:
        """Coerce a templated token-ceiling argument to a positive int, or raise ``ValueError``."""
        # bool is an int subclass (isinstance(True, int) is True) and must be rejected before the
        # allowlist check below. Only int and str are accepted as real values to coerce; anything
        # else -- float, Decimal, Fraction, or any other numeric type -- is rejected here instead
        # of being handed to int(), since int() silently truncates those (e.g. int(10.5) == 10,
        # int(Decimal("10.5")) == 10) rather than raising. Such values can reach here as real Python
        # objects, not just strings, when a Dag uses render_template_as_native_obj=True.
        if isinstance(value, bool):
            raise ValueError(f"{param_name!r} must be an integer, got {value!r}.")
        if not isinstance(value, (int, str)):
            raise ValueError(f"{param_name!r} must be an integer, got {value!r}.")
        try:
            coerced = int(value)
        except (TypeError, ValueError):
            raise ValueError(f"{param_name!r} must be an integer, got {value!r}.")
        if coerced <= 0:
            raise ValueError(f"{param_name!r} must be a positive integer, got {coerced}.")
        return coerced

    def _build_response_kwargs(self) -> dict[str, Any]:
        """
        Merge the token-ceiling arguments into ``response_kwargs``, skipping unset ceilings.

        Also pops an already-present native key when its value is blank.
        """
        response_kwargs = dict(self.response_kwargs)
        for param_name in self._TOKEN_CEILING_PARAM_NAMES:
            # These two branches can never both match for the same param_name: a ceiling set
            # both as an operator argument and natively in response_kwargs is already rejected
            # by _validate_no_response_kwargs_conflict() at __init__ time.
            if param_name in self._supplied_ceilings:
                value = getattr(self, param_name)
                # Blank means unset; the key was never added to the dict copy above, so
                # there's nothing to remove.
                if isinstance(value, str) and value.strip() == "":
                    continue
            elif param_name in response_kwargs:
                value = response_kwargs[param_name]
                # Blank means unset here too, but the key already exists in the copied dict
                # and must be popped so it disappears from the payload entirely, matching the
                # "key absent" contract the operator-argument branch gives for free above.
                if isinstance(value, str) and value.strip() == "":
                    response_kwargs.pop(param_name)
                    continue
            else:
                continue
            if value is None:
                raise ValueError(
                    f"{param_name!r} was supplied but resolved to None (e.g. an unresolved "
                    "XComArg, or a Jinja-native-rendered null); pass a positive integer, or "
                    "leave the argument unset entirely to disable the ceiling."
                )
            response_kwargs[param_name] = self._coerce_token_ceiling(param_name, value)
        return response_kwargs

    def _push_response_metadata(self, context: Context, response: Response) -> None:
        """Push the response id and token usage to XCom when ``do_xcom_push`` is enabled."""
        if self.do_xcom_push:
            context["ti"].xcom_push(key="response_id", value=response.id)
            # model_dump (not a hand-picked field list) keeps a token-usage dimension
            # the API adds later from being silently dropped; mode="json" keeps the
            # value XCom-serializable.
            #
            # XCom is cleared at the start of every attempt, so this key only ever holds
            # the last one. Stamping the attempt makes that visible rather than silently
            # under-reporting total spend across retries. Built as a new dict rather than
            # mutating what model_dump() returned.
            usage = (
                {**response.usage.model_dump(mode="json"), "try_number": context["ti"].try_number}
                if response.usage is not None
                else None
            )
            context["ti"].xcom_push(key="usage", value=usage)

    def execute(self, context: Context) -> str | dict[str, Any]:
        response_kwargs = self._build_response_kwargs()
        if self.text_format is not None:
            try:
                parsed = self.hook.parse_response(
                    input=self.input_text,
                    model=self.model,
                    text_format=self.text_format,
                    **response_kwargs,
                )
            except ValidationError as exc:
                # ``responses.parse`` raises ``ValidationError`` when the model's JSON output
                # can't be coerced into ``text_format`` — most commonly because the response
                # was truncated (e.g. ``max_output_tokens`` hit) mid-JSON. Convert to a clean
                # ``ValueError`` so callers see a consistent shape across all parse failures.
                raise ValueError(
                    f"OpenAI Responses API returned a payload that does not match "
                    f"{self.text_format.__name__!r}. The response may have been truncated because "
                    f"max_output_tokens was reached: {exc}"
                ) from exc

            self.log.info("Generated response %s", parsed.id)
            # Pushed before the checks below: a response they reject was still billed.
            self._push_response_metadata(context, parsed)
            details = _get_structured_response_details(parsed)
            if parsed.status != "completed":
                raise ValueError(f"Response {parsed.id} did not complete ({details}).")
            if parsed.output_parsed is None:
                raise ValueError(f"Response {parsed.id} did not return a structured output ({details}).")
            return parsed.output_parsed.model_dump(mode="json")
        response = self.hook.create_response(input=self.input_text, model=self.model, **response_kwargs)
        if response.status == "incomplete":
            reason = response.incomplete_details.reason if response.incomplete_details else None
            if reason and response.output_text:
                # Any reason -- including max_output_tokens -- can fire before any output text
                # is produced (e.g. a reasoning model spends the whole ceiling on reasoning
                # tokens), so whether truncated content actually exists is decided by looking at
                # output_text itself, not by the reason string.
                self.log.warning(
                    "Response %s is incomplete (incomplete_details.reason=%s); the returned output "
                    "text is truncated, not empty.",
                    response.id,
                    reason,
                )
            elif reason:
                self.log.warning(
                    "Response %s is incomplete (incomplete_details.reason=%s); the returned output "
                    "text may be empty.",
                    response.id,
                    reason,
                )
            else:
                self.log.warning(
                    "Response %s is incomplete; the returned output text may be truncated or empty.",
                    response.id,
                )
        elif response.status != "completed":
            self.log.warning(
                "Response %s ended with status %s; the returned output text may be empty.",
                response.id,
                response.status,
            )
        self.log.info("Generated response %s", response.id)
        self._push_response_metadata(context, response)
        return response.output_text


class OpenAITriggerBatchOperator(BaseOperator):
    """
    Operator that triggers an OpenAI Batch API endpoint and waits for the batch to complete.

    :param file_id: Required. The ID of the batch file to trigger. (templated)
    :param endpoint: Required. The OpenAI Batch API endpoint to trigger. (templated) Allowed values
        are determined by the OpenAI Batch API; see
        :meth:`~airflow.providers.openai.hooks.openai.OpenAIHook.create_batch`.
    :param conn_id: Optional. The OpenAI connection ID to use. Defaults to 'openai_default'.
    :param deferrable: Optional. Run operator in the deferrable mode.
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``deferrable`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Applies in both deferrable and non-deferrable mode. Defaults to 24 hours, which is the SLA for
        OpenAI Batch API.
    :param wait_for_completion: Optional. Whether to wait for the batch to complete. If set to False, the operator
        will return immediately after triggering the batch. Defaults to True.
    :param metadata: Optional. A set of key-value pairs that can be attached to the batch. (templated)
    :param batch_kwargs: Optional. Additional keyword arguments to pass to the OpenAI `create_batch`
        method — for example `output_expires_after`, which sets the expiry on the batch's output and
        error files. Defaults to None.
    :param poll_interval: Optional. Number of seconds between checks. Only used when ``deferrable`` is True.
        Defaults to 60 seconds.

    .. seealso::
        For more information on how to use this operator, please take a look at the guide:
        :ref:`howto/operator:OpenAITriggerBatchOperator`
    """

    template_fields: Sequence[str] = ("file_id", "endpoint", "metadata", "conn_id")
    template_fields_renderers = {"metadata": "json"}

    def __init__(
        self,
        file_id: str,
        endpoint: str,
        conn_id: str = OpenAIHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        wait_seconds: float = 3,
        timeout: float = 24 * 60 * 60,
        wait_for_completion: bool = True,
        *,
        metadata: dict[str, str] | None = None,
        batch_kwargs: dict | None = None,
        poll_interval: float = 60,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.file_id = file_id
        self.endpoint = endpoint
        self.conn_id = conn_id
        self.deferrable = deferrable
        self.wait_seconds = wait_seconds
        self.timeout = timeout
        self.wait_for_completion = wait_for_completion
        self.metadata = metadata
        self.batch_kwargs = batch_kwargs or {}
        self.poll_interval = poll_interval

        self.batch_id: str | None = None

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str | None:
        batch = self.hook.create_batch(
            file_id=self.file_id,
            endpoint=self.endpoint,
            metadata=self.metadata,
            **self.batch_kwargs,
        )
        self.batch_id = batch.id
        if self.wait_for_completion:
            if self.deferrable:
                self.log.info(
                    "Deferring batch %s, polling every %s seconds via poll_interval "
                    "(wait_seconds is not used in deferrable mode)",
                    self.batch_id,
                    self.poll_interval,
                )
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=OpenAIBatchTrigger(
                        conn_id=self.conn_id,
                        batch_id=self.batch_id,
                        poll_interval=self.poll_interval,
                        timeout=self.timeout,
                    ),
                    method_name="execute_complete",
                )
            else:
                self.log.info(
                    "Waiting for batch %s to complete, polling every %s seconds via wait_seconds "
                    "(poll_interval is not used in non-deferrable mode)",
                    self.batch_id,
                    self.wait_seconds,
                )
                self.hook.wait_for_batch(self.batch_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
        return self.batch_id

    def execute_complete(self, context: Context, event: Any = None) -> str:
        """
        Invoke this callback when the trigger fires; return immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        event = validate_execute_complete_event(event)
        if event["status"] != "success":
            raise OpenAIBatchJobException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return event["batch_id"]

    def on_kill(self) -> None:
        """Cancel the batch if task is cancelled."""
        if self.batch_id:
            self.log.info("on_kill: cancel the OpenAI Batch %s", self.batch_id)
            self.hook.cancel_batch(self.batch_id)
