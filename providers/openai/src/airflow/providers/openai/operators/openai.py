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
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.openai.hooks.openai import (
    OpenAIHook,
    build_batch_error,
    validate_execute_complete_event,
)
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class OpenAIEmbeddingOperator(BaseOperator):
    """
    Operator that accepts input text to generate OpenAI embeddings using the specified model.

    :param conn_id: The OpenAI connection ID to use.
    :param input_text: The text to generate OpenAI embeddings for. This can be a string, a list of strings,
                    a list of integers, or a list of lists of integers.
    :param model: The OpenAI model to be used for generating the embeddings.
    :param embedding_kwargs: Additional keyword arguments to pass to the OpenAI `create_embeddings` method.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenAIEmbeddingOperator`
        For possible options for `embedding_kwargs`, see:
        https://platform.openai.com/docs/api-reference/embeddings/create
    """

    template_fields: Sequence[str] = ("input_text",)

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

    def execute(self, context: Context) -> list[float]:
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

    The operator is synchronous and returns the response's aggregated output text. For
    ``previous_response_id`` chaining, ``background=True`` responses, or access to the full
    structured response, use :class:`~airflow.providers.openai.hooks.openai.OpenAIHook` directly.

    :param conn_id: The OpenAI connection ID to use.
    :param input_text: The input prompt for the model. This can be a string or a structured list of
        input items.
    :param model: The OpenAI model to use.
    :param response_kwargs: Additional keyword arguments to pass to the OpenAI ``create_response``
        method (for example ``instructions``, ``tools``, ``conversation`` or ``previous_response_id``).

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenAIResponseOperator`
        For possible options, see:
        https://platform.openai.com/docs/api-reference/responses/create
    """

    template_fields: Sequence[str] = ("input_text",)

    def __init__(
        self,
        conn_id: str,
        input_text: str | list[Any],
        model: str = "gpt-4o-mini",
        response_kwargs: dict | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_text = input_text
        self.model = model
        self.response_kwargs = response_kwargs or {}

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str:
        response = self.hook.create_response(input=self.input_text, model=self.model, **self.response_kwargs)
        if response.status != "completed":
            self.log.warning(
                "Response %s ended with status %s; the returned output text may be empty.",
                response.id,
                response.status,
            )
        self.log.info("Generated response %s", response.id)
        return response.output_text


class OpenAITriggerBatchOperator(BaseOperator):
    """
    Operator that triggers an OpenAI Batch API endpoint and waits for the batch to complete.

    :param file_id: Required. The ID of the batch file to trigger.
    :param endpoint: Required. The OpenAI Batch API endpoint to trigger.
    :param conn_id: Optional. The OpenAI connection ID to use. Defaults to 'openai_default'.
    :param deferrable: Optional. Run operator in the deferrable mode.
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``deferrable`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Used in both modes: in the synchronous path it bounds ``wait_for_batch``; in the
        deferrable path it bounds the trigger's poll loop. When the deferrable path times out,
        the operator requests cancellation of the batch using the batch id carried by the
        trigger event, mirroring the synchronous path. Cancellation on OpenAI's side is
        asynchronous — the batch reports ``cancelling`` for up to 10 minutes before it settles
        as ``cancelled`` — so this only *requests* cancellation, it does not wait for it. If
        ``execution_timeout`` is set shorter than ``timeout``, the scheduler's deferral timeout
        fires first: the task is failed with ``TaskDeferralTimeout`` before the trigger ever
        times out, ``execute_complete`` is never called, and this cancellation path does not
        run. Defaults to 24 hour, which is the SLA for OpenAI Batch API.
    :param wait_for_completion: Optional. Whether to wait for the batch to complete. If set to False, the operator
        will return immediately after triggering the batch. Defaults to True.

    When ``deferrable`` is True and the batch does not reach a terminal state, ``execute_complete``
    raises :class:`~airflow.providers.openai.exceptions.OpenAIBatchTimeout`, matching the exception
    raised by the synchronous path for the same condition. A cancelled batch raises
    :class:`~airflow.providers.openai.exceptions.OpenAIBatchCancelled` (a subclass of
    :class:`~airflow.providers.openai.exceptions.OpenAIBatchJobException`), and any other failure
    raises :class:`~airflow.providers.openai.exceptions.OpenAIBatchJobException`.

    .. seealso::
        For more information on how to use this operator, please take a look at the guide:
        :ref:`howto/operator:OpenAITriggerBatchOperator`
    """

    template_fields: Sequence[str] = ("file_id",)

    def __init__(
        self,
        file_id: str,
        endpoint: Literal["/v1/chat/completions", "/v1/embeddings", "/v1/completions"],
        conn_id: str = OpenAIHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        wait_seconds: float = 3,
        timeout: float = 24 * 60 * 60,
        wait_for_completion: bool = True,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.file_id = file_id
        self.endpoint = endpoint
        self.deferrable = deferrable
        self.wait_seconds = wait_seconds
        self.timeout = timeout
        self.wait_for_completion = wait_for_completion
        self.batch_id: str | None = None

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> str | None:
        batch = self.hook.create_batch(file_id=self.file_id, endpoint=self.endpoint)
        self.batch_id = batch.id
        if self.wait_for_completion:
            if self.deferrable:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=OpenAIBatchTrigger(
                        conn_id=self.conn_id,
                        batch_id=self.batch_id,
                        poll_interval=60,
                        timeout=self.timeout,
                    ),
                    method_name="execute_complete",
                )
            else:
                self.log.info("Waiting for batch %s to complete", self.batch_id)
                self.hook.wait_for_batch(self.batch_id, wait_seconds=self.wait_seconds, timeout=self.timeout)
        return self.batch_id

    def execute_complete(self, context: Context, event: Any = None) -> str:
        """
        Invoke this callback when the trigger fires; return immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful. The exception raised depends on the event's ``termination_reason``:
        ``OpenAIBatchTimeout`` for a timeout, ``OpenAIBatchCancelled`` for a cancellation,
        and ``OpenAIBatchJobException`` for any other failure (including events from a
        trigger serialized before ``termination_reason`` existed).

        On a timeout, cancellation of the batch is requested before the timeout is raised
        (see :meth:`_cancel_batch_quietly`). No other termination reason triggers
        cancellation: a ``polling_error`` may be a transient, Airflow-side failure rather than
        a real batch problem, and cancellation is irreversible, so it is left alone to run to
        its own 24-hour completion window instead.
        """
        event = validate_execute_complete_event(event)
        if event["status"] != "success":
            if event.get("termination_reason") == "timeout":
                batch_id = event.get("batch_id")
                if batch_id:
                    self.log.warning(
                        "%s timed out waiting for batch %s; requesting cancellation.",
                        self.task_id,
                        batch_id,
                    )
                    self._cancel_batch_quietly(batch_id)
                else:
                    self.log.warning(
                        "%s timed out but the trigger event carried no batch_id; "
                        "skipping cancellation request.",
                        self.task_id,
                    )
            raise build_batch_error(event["message"], event.get("termination_reason"))

        self.log.info("%s completed successfully.", self.task_id)
        return event["batch_id"]

    def _cancel_batch_quietly(self, batch_id: str) -> None:
        """
        Best-effort request to cancel a batch; never raises.

        Called from ``execute_complete`` after a deferred timeout, using the batch id carried
        by the trigger event rather than ``self.batch_id`` — this method runs on a resumed task
        instance, a fresh operator object on which ``execute``'s assignment to ``self.batch_id``
        never happened, so ``self.batch_id`` is ``None`` here.

        Cancellation on OpenAI's side is asynchronous: the batch reports ``cancelling`` for up
        to 10 minutes before it settles as ``cancelled``, so this only requests cancellation. A
        failure to cancel is logged, not raised, so it never masks the timeout that is the
        task's real failure reason.
        """
        try:
            self.hook.cancel_batch(batch_id)
        except Exception as e:
            self.log.warning("Failed to request cancellation of batch %s: %s", batch_id, e)

    def on_kill(self) -> None:
        """Cancel the batch if task is cancelled."""
        if self.batch_id:
            self.log.info("on_kill: cancel the OpenAI Batch %s", self.batch_id)
            self.hook.cancel_batch(self.batch_id)
