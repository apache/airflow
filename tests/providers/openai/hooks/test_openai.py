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

import os
from unittest.mock import patch

import pytest

openai = pytest.importorskip("openai")

from openai.pagination import SyncCursorPage
from openai.types import CreateEmbeddingResponse, Embedding
from openai.types.beta import Assistant, AssistantDeleted, Thread, ThreadDeleted
from openai.types.beta.threads import Message, Run
from openai.types.chat import ChatCompletion

from airflow.models import Connection
from airflow.providers.openai.hooks.openai import OpenAIHook

ASSISTANT_ID = "test_assistant_abc123"
ASSISTANT_NAME = "Test Assistant"
ASSISTANT_INSTRUCTIONS = "You are a test assistant."
THREAD_ID = "test_thread_abc123"
MESSAGE_ID = "test_message_abc123"
RUN_ID = "test_run_abc123"
MODEL = "gpt-4"
METADATA = {"modified": "true", "user": "abc123"}


@pytest.fixture
def mock_openai_connection():
    conn_id = "openai_conn"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    return conn


@pytest.fixture
def mock_openai_hook(mock_openai_connection):
    with patch("airflow.providers.openai.hooks.openai.OpenAI"):
        yield OpenAIHook(conn_id=mock_openai_connection.conn_id)


@pytest.fixture
def mock_embeddings_response():
    return CreateEmbeddingResponse(
        data=[Embedding(embedding=[0.1, 0.2, 0.3], index=0, object="embedding")],
        model="text-embedding-ada-002-v2",
        object="list",
        usage={"prompt_tokens": 4, "total_tokens": 4},
    )


@pytest.fixture
def mock_completion():
    return ChatCompletion(
        id="chatcmpl-123",
        object="chat.completion",
        created=1677652288,
        model=MODEL,
        choices=[
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello there, how may I assist you today?",
                },
                "logprobs": None,
                "finish_reason": "stop",
            }
        ],
    )


@pytest.fixture
def mock_assistant():
    return Assistant(
        id=ASSISTANT_ID,
        name=ASSISTANT_NAME,
        object="assistant",
        created_at=1677652288,
        model=MODEL,
        instructions=ASSISTANT_INSTRUCTIONS,
        tools=[],
        file_ids=[],
        metadata={},
    )


@pytest.fixture
def mock_assistant_list(mock_assistant):
    return SyncCursorPage[Assistant](data=[mock_assistant])


@pytest.fixture
def mock_thread():
    return Thread(id=THREAD_ID, object="thread", created_at=1698984975, metadata={})


@pytest.fixture
def mock_message():
    return Message(
        id=MESSAGE_ID,
        object="thread.message",
        created_at=1698984975,
        thread_id=THREAD_ID,
        status="completed",
        role="user",
        content=[{"type": "text", "text": {"value": "Tell me something interesting.", "annotations": []}}],
        assistant_id=ASSISTANT_ID,
        run_id=RUN_ID,
        file_ids=[],
        metadata={},
    )


@pytest.fixture
def mock_message_list(mock_message):
    return SyncCursorPage[Message](data=[mock_message])


@pytest.fixture
def mock_run():
    return Run(
        id=RUN_ID,
        object="thread.run",
        created_at=1698107661,
        assistant_id=ASSISTANT_ID,
        thread_id=THREAD_ID,
        status="completed",
        started_at=1699073476,
        completed_at=1699073476,
        model=MODEL,
        instructions="You are a test assistant.",
        tools=[],
        file_ids=[],
        metadata={},
    )


@pytest.fixture
def mock_run_list(mock_run):
    return SyncCursorPage[Run](data=[mock_run])


def test_create_chat_completion(mock_openai_hook, mock_completion):
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"},
    ]

    mock_openai_hook.conn.chat.completions.create.return_value = mock_completion
    completion = mock_openai_hook.create_chat_completion(model=MODEL, messages=messages)
    choice = completion[0]
    assert choice.message.content == "Hello there, how may I assist you today?"


def test_create_assistant(mock_openai_hook, mock_assistant):
    mock_openai_hook.conn.beta.assistants.create.return_value = mock_assistant
    assistant = mock_openai_hook.create_assistant(
        name=ASSISTANT_NAME, model=MODEL, instructions=ASSISTANT_INSTRUCTIONS
    )
    assert assistant.name == ASSISTANT_NAME
    assert assistant.model == MODEL
    assert assistant.instructions == ASSISTANT_INSTRUCTIONS


def test_get_assistant(mock_openai_hook, mock_assistant):
    mock_openai_hook.conn.beta.assistants.retrieve.return_value = mock_assistant
    assistant = mock_openai_hook.get_assistant(assistant_id=ASSISTANT_ID)
    assert assistant.name == ASSISTANT_NAME
    assert assistant.model == MODEL
    assert assistant.instructions == ASSISTANT_INSTRUCTIONS


def test_get_assistants(mock_openai_hook, mock_assistant_list):
    mock_openai_hook.conn.beta.assistants.list.return_value = mock_assistant_list
    assistants = mock_openai_hook.get_assistants()
    assert isinstance(assistants, list)


def test_get_assistant_by_name(mock_openai_hook, mock_assistant_list):
    mock_openai_hook.conn.beta.assistants.list.return_value = mock_assistant_list
    assistant = mock_openai_hook.get_assistant_by_name(assistant_name=ASSISTANT_NAME)
    assert assistant.name == ASSISTANT_NAME


def test_modify_assistant(mock_openai_hook, mock_assistant):
    new_assistant_name = "New Test Assistant"
    mock_assistant.name = new_assistant_name
    mock_openai_hook.conn.beta.assistants.update.return_value = mock_assistant
    assistant = mock_openai_hook.modify_assistant(assistant_id=ASSISTANT_ID, name=new_assistant_name)
    assert assistant.name == new_assistant_name


def test_delete_assistant(mock_openai_hook):
    delete_response = AssistantDeleted(id=ASSISTANT_ID, object="assistant.deleted", deleted=True)
    mock_openai_hook.conn.beta.assistants.delete.return_value = delete_response
    assistant_deleted = mock_openai_hook.delete_assistant(assistant_id=ASSISTANT_ID)
    assert assistant_deleted.deleted


def test_create_thread(mock_openai_hook, mock_thread):
    mock_openai_hook.conn.beta.threads.create.return_value = mock_thread
    thread = mock_openai_hook.create_thread()
    assert thread.id == THREAD_ID


def test_modify_thread(mock_openai_hook, mock_thread):
    mock_thread.metadata = METADATA
    mock_openai_hook.conn.beta.threads.update.return_value = mock_thread
    thread = mock_openai_hook.modify_thread(thread_id=THREAD_ID, metadata=METADATA)
    assert thread.metadata.get("modified") == "true"
    assert thread.metadata.get("user") == "abc123"


def test_delete_thread(mock_openai_hook):
    delete_response = ThreadDeleted(id=THREAD_ID, object="thread.deleted", deleted=True)
    mock_openai_hook.conn.beta.threads.delete.return_value = delete_response
    thread_deleted = mock_openai_hook.delete_thread(thread_id=THREAD_ID)
    assert thread_deleted.deleted


def test_create_message(mock_openai_hook, mock_message):
    role = "user"
    content = "Tell me something interesting."
    mock_openai_hook.conn.beta.threads.messages.create.return_value = mock_message
    message = mock_openai_hook.create_message(thread_id=THREAD_ID, content=content, role=role)
    assert message.id == MESSAGE_ID


def test_get_messages(mock_openai_hook, mock_message_list):
    mock_openai_hook.conn.beta.threads.messages.list.return_value = mock_message_list
    messages = mock_openai_hook.get_messages(thread_id=THREAD_ID)
    assert isinstance(messages, list)


def test_modify_messages(mock_openai_hook, mock_message):
    mock_message.metadata = METADATA
    mock_openai_hook.conn.beta.threads.messages.update.return_value = mock_message
    message = mock_openai_hook.modify_message(thread_id=THREAD_ID, message_id=MESSAGE_ID, metadata=METADATA)
    assert message.metadata.get("modified") == "true"
    assert message.metadata.get("user") == "abc123"


def test_create_run(mock_openai_hook, mock_run):
    thread_id = THREAD_ID
    assistant_id = ASSISTANT_ID
    mock_openai_hook.conn.beta.threads.runs.create.return_value = mock_run
    run = mock_openai_hook.create_run(thread_id=thread_id, assistant_id=assistant_id)
    assert run.id == RUN_ID


def test_get_runs(mock_openai_hook, mock_run_list):
    mock_openai_hook.conn.beta.threads.runs.list.return_value = mock_run_list
    runs = mock_openai_hook.get_runs(thread_id=THREAD_ID)
    assert isinstance(runs, list)


def test_get_run_with_run_id(mock_openai_hook, mock_run):
    mock_openai_hook.conn.beta.threads.runs.retrieve.return_value = mock_run
    run = mock_openai_hook.get_run(thread_id=THREAD_ID, run_id=RUN_ID)
    assert run.id == RUN_ID


def test_modify_run(mock_openai_hook, mock_run):
    mock_run.metadata = METADATA
    mock_openai_hook.conn.beta.threads.runs.update.return_value = mock_run
    message = mock_openai_hook.modify_run(thread_id=THREAD_ID, run_id=RUN_ID, metadata=METADATA)
    assert message.metadata.get("modified") == "true"
    assert message.metadata.get("user") == "abc123"


def test_create_embeddings(mock_openai_hook, mock_embeddings_response):
    text = "Sample text"
    mock_openai_hook.conn.embeddings.create.return_value = mock_embeddings_response
    embeddings = mock_openai_hook.create_embeddings(text)
    assert embeddings == [0.1, 0.2, 0.3]


def test_openai_hook_test_connection(mock_openai_hook):
    result, message = mock_openai_hook.test_connection()
    assert result is True
    assert message == "Connection established!"


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_api_key_in_extra(mock_client):
    conn_id = "api_key_in_extra"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={"openai_client_kwargs": {"api_key": "api_key_in_extra"}},
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_extra",
        base_url=None,
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_api_key_in_password(mock_client):
    conn_id = "api_key_in_password"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        password="api_key_in_password",
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_password",
        base_url=None,
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_base_url_in_extra(mock_client):
    conn_id = "base_url_in_extra"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={"openai_client_kwargs": {"base_url": "base_url_in_extra", "api_key": "api_key_in_extra"}},
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_extra",
        base_url="base_url_in_extra",
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_openai_client_kwargs(mock_client):
    conn_id = "openai_client_kwargs"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={
            "openai_client_kwargs": {
                "api_key": "api_key_in_extra",
                "organization": "organization_in_extra",
            }
        },
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_extra",
        base_url=None,
        organization="organization_in_extra",
    )
