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

import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, cast

from openai import NotFoundError

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.openai.exceptions import OpenAIBatchJobException
from airflow.providers.openai.hooks.openai import BatchStatus, OpenAIHook, validate_execute_complete_event
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

_DURABLE_UNSET: object = object()
_MISSING_BATCH_STATUS: str = "missing"


def _warn_and_disable_durable_pre_3_3(durable: Any) -> bool:
    """Disable unsupported durable mode and warn when explicitly requested."""
    if durable is not _DURABLE_UNSET:
        warnings.warn(
            "`durable` has no effect on Airflow versions below 3.3.",
            UserWarning,
            stacklevel=3,
        )
    return False


# ResumableJobMixin requires Airflow 3.3; this provider still supports Airflow 2.11.
try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Airflow <3.3 stub that always submits a fresh job."""

        external_id_key: str = "openai_batch_id"

        def __init__(self, *, durable: Any = _DURABLE_UNSET, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = _warn_and_disable_durable_pre_3_3(durable)

        def execute_resumable(self, context: Any) -> Any:
            external_id: Any = self.submit_job(context=context)
            self.poll_until_complete(external_id=external_id, context=context)
            return self.get_job_result(external_id=external_id, context=context)

        def submit_job(self, context: Any) -> Any:
            raise NotImplementedError

        def poll_until_complete(self, external_id: Any, context: Any) -> None:
            raise NotImplementedError

        def get_job_result(self, external_id: Any, context: Any) -> Any:
            raise NotImplementedError


if TYPE_CHECKING:
    from pydantic import JsonValue

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


class OpenAITriggerBatchOperator(ResumableJobMixin, BaseOperator):
    """
    Operator that triggers an OpenAI Batch API endpoint and waits for the batch to complete.

    :param file_id: Required. The ID of the batch file to trigger.
    :param endpoint: Required. The OpenAI Batch API endpoint to trigger.
    :param conn_id: Optional. The OpenAI connection ID to use. Defaults to 'openai_default'.
    :param deferrable: Optional. Run operator in the deferrable mode.
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``deferrable`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``deferrable`` is False. Defaults to 24 hour, which is the SLA for OpenAI Batch API.
    :param wait_for_completion: Optional. Whether to wait for the batch to complete. If set to False, the operator
        will return immediately after triggering the batch. Defaults to True.
    :param durable: When True (the default) and waiting synchronously, persist the OpenAI batch ID before
        polling so a worker crash reconnects to the existing batch on retry. Requires Airflow 3.3+; this
        option has no effect on earlier versions.

    .. seealso::
        For more information on how to use this operator, please take a look at the guide:
        :ref:`howto/operator:OpenAITriggerBatchOperator`
    """

    template_fields: Sequence[str] = ("file_id",)
    external_id_key: str = "openai_batch_id"

    def __init__(
        self,
        file_id: str,
        endpoint: Literal["/v1/chat/completions", "/v1/embeddings", "/v1/completions"],
        conn_id: str = OpenAIHook.default_conn_name,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        wait_seconds: float = 3,
        timeout: float = 24 * 60 * 60,
        wait_for_completion: bool = True,
        durable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if durable is not None:
            kwargs["durable"] = durable
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
        if self.wait_for_completion and not self.deferrable:
            self.execute_resumable(context)
            return self.batch_id

        batch_id = self.submit_job(context)
        if self.wait_for_completion:
            self.defer(
                timeout=self.execution_timeout,
                trigger=OpenAIBatchTrigger(
                    conn_id=self.conn_id,
                    batch_id=batch_id,
                    poll_interval=60,
                    timeout=self.timeout,
                ),
                method_name="execute_complete",
            )
        return batch_id

    def submit_job(self, context: Context) -> str:
        batch = self.hook.create_batch(file_id=self.file_id, endpoint=self.endpoint)
        self.batch_id = batch.id
        return self.batch_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        self.batch_id = cast("str", external_id)
        try:
            return self.hook.get_batch(batch_id=self.batch_id).status
        except NotFoundError:
            return _MISSING_BATCH_STATUS

    def is_job_active(self, status: str) -> bool:
        return BatchStatus.is_in_progress(status)

    def is_job_succeeded(self, status: str) -> bool:
        return status == BatchStatus.COMPLETED

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        self.batch_id = cast("str", external_id)
        self.log.info("Waiting for batch %s to complete", self.batch_id)
        self.hook.wait_for_batch(
            batch_id=self.batch_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

    def get_job_result(self, external_id: JsonValue, context: Context) -> str:
        self.batch_id = cast("str", external_id)
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
            self.hook.cancel_batch(batch_id=self.batch_id)
