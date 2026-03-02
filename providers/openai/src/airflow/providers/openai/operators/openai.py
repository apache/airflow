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

import time
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.compat.sdk import BaseOperator, conf
from airflow.providers.openai.exceptions import OpenAIBatchJobException
from airflow.providers.openai.hooks.openai import OpenAIHook
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
        model: str = "text-embedding-ada-002",
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
        Only used when ``deferrable`` is False. Defaults to 24 hour, which is the SLA for OpenAI Batch API.
    :param wait_for_completion: Optional. Whether to wait for the batch to complete. If set to False, the operator
        will return immediately after triggering the batch. Defaults to True.

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
                        end_time=time.time() + self.timeout,
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
        successful.
        """
        if event["status"] == "error":
            raise OpenAIBatchJobException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return event["batch_id"]

    def on_kill(self) -> None:
        """Cancel the batch if task is cancelled."""
        if self.batch_id:
            self.log.info("on_kill: cancel the OpenAI Batch %s", self.batch_id)
            self.hook.cancel_batch(self.batch_id)
