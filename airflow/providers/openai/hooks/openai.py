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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from openai import OpenAI

if TYPE_CHECKING:
    from openai.types.beta import Assistant, AssistantDeleted, Thread, ThreadDeleted
    from openai.types.beta.threads import Message, Run
    from openai.types.chat import (
        ChatCompletionAssistantMessageParam,
        ChatCompletionFunctionMessageParam,
        ChatCompletionMessage,
        ChatCompletionSystemMessageParam,
        ChatCompletionToolMessageParam,
        ChatCompletionUserMessageParam,
    )

from airflow.hooks.base import BaseHook


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
        """Create an OpenAI assistant using the given model.

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

    def get_assistant_by_name(self, assistant_name: str) -> Assistant | None:
        """Get an OpenAI Assistant object for a given name.

        :param assistant_name: The name of the assistant to retrieve
        """
        response = self.get_assistants()
        for assistant in response:
            if assistant.name == assistant_name:
                return assistant
        return None

    def modify_assistant(self, assistant_id: str, **kwargs: Any) -> Assistant:
        """Modify an existing Assistant object.

        :param assistant_id: The ID of the assistant to be modified.
        """
        assistant = self.conn.beta.assistants.update(assistant_id=assistant_id, **kwargs)
        return assistant

    def delete_assistant(self, assistant_id: str) -> AssistantDeleted:
        """Delete an OpenAI Assistant for a given ID.

        :param assistant_id: The ID of the assistant to delete.
        """
        response = self.conn.beta.assistants.delete(assistant_id=assistant_id)
        return response

    def create_thread(self, **kwargs: Any) -> Thread:
        """Create an OpenAI thread."""
        thread = self.conn.beta.threads.create(**kwargs)
        return thread

    def modify_thread(self, thread_id: str, metadata: dict[str, Any]) -> Thread:
        """Modify an existing Thread object.

        :param thread_id: The ID of the thread to modify.
        :param metadata: Set of 16 key-value pairs that can be attached to an object.
        """
        thread = self.conn.beta.threads.update(thread_id=thread_id, metadata=metadata)
        return thread

    def delete_thread(self, thread_id: str) -> ThreadDeleted:
        """Delete an OpenAI thread for a given thread_id.

        :param thread_id: The ID of the thread to delete.
        """
        response = self.conn.beta.threads.delete(thread_id=thread_id)
        return response

    def create_message(
        self, thread_id: str, role: Literal["user", "assistant"], content: str, **kwargs: Any
    ) -> Message:
        """Create a message for a given Thread.

        :param thread_id: The ID of the thread to create a message for.
        :param role: The role of the entity that is creating the message. Allowed values include: 'user', 'assistant'.
        :param content: The content of the message.
        """
        thread_message = self.conn.beta.threads.messages.create(
            thread_id=thread_id, role=role, content=content, **kwargs
        )
        return thread_message

    def get_messages(self, thread_id: str, **kwargs: Any) -> list[Message]:
        """Return a list of messages for a given Thread.

        :param thread_id: The ID of the thread the messages belong to.
        """
        messages = self.conn.beta.threads.messages.list(thread_id=thread_id, **kwargs)
        return messages.data

    def modify_message(self, thread_id: str, message_id, **kwargs: Any) -> Message:
        """Modify an existing message for a given Thread.

        :param thread_id: The ID of the thread to which this message belongs.
        :param message_id: The ID of the message to modify.
        """
        thread_message = self.conn.beta.threads.messages.update(
            thread_id=thread_id, message_id=message_id, **kwargs
        )
        return thread_message

    def create_run(self, thread_id: str, assistant_id: str, **kwargs: Any) -> Run:
        """Create a run for a given thread and assistant.

        :param thread_id: The ID of the thread to run.
        :param assistant_id: The ID of the assistant to use to execute this run.
        """
        run = self.conn.beta.threads.runs.create(thread_id=thread_id, assistant_id=assistant_id, **kwargs)
        return run

    def get_run(self, thread_id: str, run_id: str) -> Run:
        """Retrieve a run for a given thread and run.

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
        """Generate embeddings for the given text using the given model.

        :param text: The text to generate embeddings for.
        :param model: The model to use for generating embeddings.
        """
        response = self.conn.embeddings.create(model=model, input=text, **kwargs)
        embeddings: list[float] = response.data[0].embedding
        return embeddings
