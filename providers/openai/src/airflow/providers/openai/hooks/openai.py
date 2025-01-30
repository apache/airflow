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
from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, BinaryIO, Literal

from openai import OpenAI

if TYPE_CHECKING:
    from openai.types import FileDeleted, FileObject
    from openai.types.batch import Batch
    from openai.types.beta import (
        Assistant,
        AssistantDeleted,
        Thread,
        ThreadDeleted,
        VectorStore,
        VectorStoreDeleted,
    )
    from openai.types.beta.threads import Message, Run
    from openai.types.beta.vector_stores import VectorStoreFile, VectorStoreFileBatch, VectorStoreFileDeleted
    from openai.types.chat import (
        ChatCompletionAssistantMessageParam,
        ChatCompletionFunctionMessageParam,
        ChatCompletionMessage,
        ChatCompletionSystemMessageParam,
        ChatCompletionToolMessageParam,
        ChatCompletionUserMessageParam,
    )
from airflow.hooks.base import BaseHook
from airflow.providers.openai.exceptions import OpenAIBatchJobException, OpenAIBatchTimeout


class BatchStatus(str, Enum):
    """Enum for the status of a batch."""

    VALIDATING = "validating"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    EXPIRED = "expired"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def is_in_progress(cls, status: str) -> bool:
        """Check if the batch status is in progress."""
        return status in (cls.VALIDATING, cls.IN_PROGRESS, cls.FINALIZING)


class OpenAIHook(BaseHook):
    """
    Use OpenAI SDK to interact with OpenAI APIs.

    .. seealso:: https://platform.openai.com/docs/introduction/overview

    :param conn_id: :ref:`OpenAI connection id <howto/connection:openai>`
    """

    conn_name_attr = "conn_id"
    default_conn_name = "openai_default"
    conn_type = "openai"
    hook_name = "OpenAI"

    def __init__(self, conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.conn.models.list()
            return True, "Connection established!"
        except Exception as e:
            return False, str(e)

    @cached_property
    def conn(self) -> OpenAI:
        """Return an OpenAI connection object."""
        return self.get_conn()

    def get_conn(self) -> OpenAI:
        """Return an OpenAI connection object."""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        openai_client_kwargs = extras.get("openai_client_kwargs", {})
        api_key = openai_client_kwargs.pop("api_key", None) or conn.password
        base_url = openai_client_kwargs.pop("base_url", None) or conn.host or None
        return OpenAI(
            api_key=api_key,
            base_url=base_url,
            **openai_client_kwargs,
        )

    def create_chat_completion(
        self,
        messages: list[
            ChatCompletionSystemMessageParam
            | ChatCompletionUserMessageParam
            | ChatCompletionAssistantMessageParam
            | ChatCompletionToolMessageParam
            | ChatCompletionFunctionMessageParam
        ],
        model: str = "gpt-3.5-turbo",
        **kwargs: Any,
    ) -> list[ChatCompletionMessage]:
        """
        Create a model response for the given chat conversation and returns a list of chat completions.

        :param messages: A list of messages comprising the conversation so far
        :param model: ID of the model to use
        """
        response = self.conn.chat.completions.create(model=model, messages=messages, **kwargs)
        return response.choices

    def create_assistant(self, model: str = "gpt-3.5-turbo", **kwargs: Any) -> Assistant:
        """
        Create an OpenAI assistant using the given model.

        :param model: The OpenAI model for the assistant to use.
        """
        assistant = self.conn.beta.assistants.create(model=model, **kwargs)
        return assistant

    def get_assistant(self, assistant_id: str) -> Assistant:
        """
        Get an OpenAI assistant.

        :param assistant_id: The ID of the assistant to retrieve.
        """
        assistant = self.conn.beta.assistants.retrieve(assistant_id=assistant_id)
        return assistant

    def get_assistants(self, **kwargs: Any) -> list[Assistant]:
        """Get a list of Assistant objects."""
        assistants = self.conn.beta.assistants.list(**kwargs)
        return assistants.data

    def modify_assistant(self, assistant_id: str, **kwargs: Any) -> Assistant:
        """
        Modify an existing Assistant object.

        :param assistant_id: The ID of the assistant to be modified.
        """
        assistant = self.conn.beta.assistants.update(assistant_id=assistant_id, **kwargs)
        return assistant

    def delete_assistant(self, assistant_id: str) -> AssistantDeleted:
        """
        Delete an OpenAI Assistant for a given ID.

        :param assistant_id: The ID of the assistant to delete.
        """
        response = self.conn.beta.assistants.delete(assistant_id=assistant_id)
        return response

    def create_thread(self, **kwargs: Any) -> Thread:
        """Create an OpenAI thread."""
        thread = self.conn.beta.threads.create(**kwargs)
        return thread

    def modify_thread(self, thread_id: str, metadata: dict[str, Any]) -> Thread:
        """
        Modify an existing Thread object.

        :param thread_id: The ID of the thread to modify. Only the metadata can be modified.
        :param metadata: Set of 16 key-value pairs that can be attached to an object.
        """
        thread = self.conn.beta.threads.update(thread_id=thread_id, metadata=metadata)
        return thread

    def delete_thread(self, thread_id: str) -> ThreadDeleted:
        """
        Delete an OpenAI thread for a given thread_id.

        :param thread_id: The ID of the thread to delete.
        """
        response = self.conn.beta.threads.delete(thread_id=thread_id)
        return response

    def create_message(
        self, thread_id: str, role: Literal["user", "assistant"], content: str, **kwargs: Any
    ) -> Message:
        """
        Create a message for a given Thread.

        :param thread_id: The ID of the thread to create a message for.
        :param role: The role of the entity that is creating the message. Allowed values include: 'user', 'assistant'.
        :param content: The content of the message.
        """
        thread_message = self.conn.beta.threads.messages.create(
            thread_id=thread_id, role=role, content=content, **kwargs
        )
        return thread_message

    def get_messages(self, thread_id: str, **kwargs: Any) -> list[Message]:
        """
        Return a list of messages for a given Thread.

        :param thread_id: The ID of the thread the messages belong to.
        """
        messages = self.conn.beta.threads.messages.list(thread_id=thread_id, **kwargs)
        return messages.data

    def modify_message(self, thread_id: str, message_id, **kwargs: Any) -> Message:
        """
        Modify an existing message for a given Thread.

        :param thread_id: The ID of the thread to which this message belongs.
        :param message_id: The ID of the message to modify.
        """
        thread_message = self.conn.beta.threads.messages.update(
            thread_id=thread_id, message_id=message_id, **kwargs
        )
        return thread_message

    def create_run(self, thread_id: str, assistant_id: str, **kwargs: Any) -> Run:
        """
        Create a run for a given thread and assistant.

        :param thread_id: The ID of the thread to run.
        :param assistant_id: The ID of the assistant to use to execute this run.
        """
        run = self.conn.beta.threads.runs.create(thread_id=thread_id, assistant_id=assistant_id, **kwargs)
        return run

    def create_run_and_poll(self, thread_id: str, assistant_id: str, **kwargs: Any) -> Run:
        """
        Create a run for a given thread and assistant and then polls until completion.

        :param thread_id: The ID of the thread to run.
        :param assistant_id: The ID of the assistant to use to execute this run.
        :return: An OpenAI Run object
        """
        run = self.conn.beta.threads.runs.create_and_poll(
            thread_id=thread_id, assistant_id=assistant_id, **kwargs
        )
        return run

    def get_run(self, thread_id: str, run_id: str) -> Run:
        """
        Retrieve a run for a given thread and run.

        :param thread_id: The ID of the thread that was run.
        :param run_id: The ID of the run to retrieve.
        """
        run = self.conn.beta.threads.runs.retrieve(thread_id=thread_id, run_id=run_id)
        return run

    def get_runs(self, thread_id: str, **kwargs: Any) -> list[Run]:
        """
        Return a list of runs belonging to a thread.

        :param thread_id: The ID of the thread the run belongs to.
        """
        runs = self.conn.beta.threads.runs.list(thread_id=thread_id, **kwargs)
        return runs.data

    def modify_run(self, thread_id: str, run_id: str, **kwargs: Any) -> Run:
        """
        Modify a run on a given thread.

        :param thread_id: The ID of the thread that was run.
        :param run_id: The ID of the run to modify.
        """
        run = self.conn.beta.threads.runs.update(thread_id=thread_id, run_id=run_id, **kwargs)
        return run

    def create_embeddings(
        self,
        text: str | list[str] | list[int] | list[list[int]],
        model: str = "text-embedding-ada-002",
        **kwargs: Any,
    ) -> list[float]:
        """
        Generate embeddings for the given text using the given model.

        :param text: The text to generate embeddings for.
        :param model: The model to use for generating embeddings.
        """
        response = self.conn.embeddings.create(model=model, input=text, **kwargs)
        embeddings: list[float] = response.data[0].embedding
        return embeddings

    def upload_file(self, file: str, purpose: Literal["fine-tune", "assistants", "batch"]) -> FileObject:
        """
        Upload a file that can be used across various endpoints. The size of all the files uploaded by one organization can be up to 100 GB.

        :param file: The File object (not file name) to be uploaded.
        :param purpose: The intended purpose of the uploaded file. Use "fine-tune" for
            Fine-tuning, "assistants" for Assistants and Messages, and "batch" for Batch API.
        """
        with open(file, "rb") as file_stream:
            file_object = self.conn.files.create(file=file_stream, purpose=purpose)
        return file_object

    def get_file(self, file_id: str) -> FileObject:
        """
        Return information about a specific file.

        :param file_id: The ID of the file to use for this request.
        """
        file = self.conn.files.retrieve(file_id=file_id)
        return file

    def get_files(self) -> list[FileObject]:
        """Return a list of files that belong to the user's organization."""
        files = self.conn.files.list()
        return files.data

    def delete_file(self, file_id: str) -> FileDeleted:
        """
        Delete a file.

        :param file_id: The ID of the file to be deleted.
        """
        response = self.conn.files.delete(file_id=file_id)
        return response

    def create_vector_store(self, **kwargs: Any) -> VectorStore:
        """Create a vector store."""
        vector_store = self.conn.beta.vector_stores.create(**kwargs)
        return vector_store

    def get_vector_stores(self, **kwargs: Any) -> list[VectorStore]:
        """Return a list of vector stores."""
        vector_stores = self.conn.beta.vector_stores.list(**kwargs)
        return vector_stores.data

    def get_vector_store(self, vector_store_id: str) -> VectorStore:
        """
        Retrieve a vector store.

        :param vector_store_id: The ID of the vector store to retrieve.
        """
        vector_store = self.conn.beta.vector_stores.retrieve(vector_store_id=vector_store_id)
        return vector_store

    def modify_vector_store(self, vector_store_id: str, **kwargs: Any) -> VectorStore:
        """
        Modify a vector store.

        :param vector_store_id: The ID of the vector store to modify.
        """
        vector_store = self.conn.beta.vector_stores.update(vector_store_id=vector_store_id, **kwargs)
        return vector_store

    def delete_vector_store(self, vector_store_id: str) -> VectorStoreDeleted:
        """
        Delete a vector store.

        :param vector_store_id: The ID of the vector store to delete.
        """
        response = self.conn.beta.vector_stores.delete(vector_store_id=vector_store_id)
        return response

    def upload_files_to_vector_store(
        self, vector_store_id: str, files: list[BinaryIO]
    ) -> VectorStoreFileBatch:
        """
        Upload files to a vector store and poll until completion.

        :param vector_store_id: The ID of the vector store the files are to be uploaded
            to.
        :param files: A list of binary files to upload.
        """
        file_batch = self.conn.beta.vector_stores.file_batches.upload_and_poll(
            vector_store_id=vector_store_id, files=files
        )
        return file_batch

    def get_vector_store_files(self, vector_store_id: str) -> list[VectorStoreFile]:
        """
        Return a list of vector store files.

        :param vector_store_id:
        """
        vector_store_files = self.conn.beta.vector_stores.files.list(vector_store_id=vector_store_id)
        return vector_store_files.data

    def delete_vector_store_file(self, vector_store_id: str, file_id: str) -> VectorStoreFileDeleted:
        """
        Delete a vector store file. This will remove the file from the vector store but the file itself will not be deleted. To delete the file, use delete_file.

        :param vector_store_id: The ID of the vector store that the file belongs to.
        :param file_id: The ID of the file to delete.
        """
        response = self.conn.beta.vector_stores.files.delete(vector_store_id=vector_store_id, file_id=file_id)
        return response

    def create_batch(
        self,
        file_id: str,
        endpoint: Literal["/v1/chat/completions", "/v1/embeddings", "/v1/completions"],
        metadata: dict[str, str] | None = None,
        completion_window: Literal["24h"] = "24h",
    ) -> Batch:
        """
        Create a batch for a given model and files.

        :param file_id: The ID of the file to be used for this batch.
        :param endpoint: The endpoint to use for this batch. Allowed values include:
            '/v1/chat/completions', '/v1/embeddings', '/v1/completions'.
        :param metadata: A set of key-value pairs that can be attached to an object.
        :param completion_window: The time window for the batch to complete. Default is 24 hours.
        """
        batch = self.conn.batches.create(
            input_file_id=file_id, endpoint=endpoint, metadata=metadata, completion_window=completion_window
        )
        return batch

    def get_batch(self, batch_id: str) -> Batch:
        """
        Get the status of a batch.

        :param batch_id: The ID of the batch to get the status of.
        """
        batch = self.conn.batches.retrieve(batch_id=batch_id)
        return batch

    def wait_for_batch(self, batch_id: str, wait_seconds: float = 3, timeout: float = 3600) -> None:
        """
        Poll a batch to check if it finishes.

        :param batch_id: Id of the Batch to wait for.
        :param wait_seconds: Optional. Number of seconds between checks.
        :param timeout: Optional. How many seconds wait for batch to be ready.
            Used only if not ran in deferred operator.
        """
        start = time.monotonic()
        while True:
            if start + timeout < time.monotonic():
                self.cancel_batch(batch_id=batch_id)
                raise OpenAIBatchTimeout(f"Timeout: OpenAI Batch {batch_id} is not ready after {timeout}s")
            batch = self.get_batch(batch_id=batch_id)

            if BatchStatus.is_in_progress(batch.status):
                time.sleep(wait_seconds)
                continue
            if batch.status == BatchStatus.COMPLETED:
                return
            if batch.status == BatchStatus.FAILED:
                raise OpenAIBatchJobException(f"Batch failed - \n{batch_id}")
            elif batch.status in (BatchStatus.CANCELLED, BatchStatus.CANCELLING):
                raise OpenAIBatchJobException(f"Batch failed - batch was cancelled:\n{batch_id}")
            elif batch.status == BatchStatus.EXPIRED:
                raise OpenAIBatchJobException(
                    f"Batch failed - batch couldn't be completed within the hour time window :\n{batch_id}"
                )

            raise OpenAIBatchJobException(
                f"Batch failed - encountered unexpected status `{batch.status}` for batch_id `{batch_id}`"
            )

    def cancel_batch(self, batch_id: str) -> Batch:
        """
        Cancel a batch.

        :param batch_id: The ID of the batch to delete.
        """
        batch = self.conn.batches.cancel(batch_id=batch_id)
        return batch
