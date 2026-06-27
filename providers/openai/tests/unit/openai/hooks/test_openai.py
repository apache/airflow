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
from unittest.mock import MagicMock, mock_open, patch

import pytest
from openai import OpenAI
from openai.pagination import SyncCursorPage
from openai.types import (
    Batch,
    CreateEmbeddingResponse,
    Embedding,
    FileDeleted,
    FileObject,
    VectorStore,
    VectorStoreDeleted,
)
from openai.types.beta import Assistant, AssistantDeleted, Thread, ThreadDeleted
from openai.types.beta.threads import Message, Run
from openai.types.chat import ChatCompletion
from openai.types.vector_stores import VectorStoreFile, VectorStoreFileBatch, VectorStoreFileDeleted

from airflow.models import Connection
from airflow.providers.openai.exceptions import OpenAIBatchJobException, OpenAIBatchTimeout
from airflow.providers.openai.hooks.openai import OpenAIHook

ASSISTANT_ID = "test_assistant_abc123"
ASSISTANT_NAME = "Test Assistant"
ASSISTANT_INSTRUCTIONS = "You are a test assistant."
THREAD_ID = "test_thread_abc123"
MESSAGE_ID = "test_message_abc123"
RUN_ID = "test_run_abc123"
MODEL = "gpt-4"
FILE_ID = "test_file_abc123"
FILE_NAME = "test_file.pdf"
METADATA = {"modified": "true", "user": "abc123"}
VECTOR_STORE_ID = "test_vs_abc123"
VECTOR_STORE_NAME = "Test Vector Store"
VECTOR_FILE_STORE_BATCH_ID = "test_vfsb_abc123"
BATCH_ID = "test_batch_abc123"

openai = pytest.importorskip("openai")


def create_batch(status) -> Batch:
    return Batch(
        id=BATCH_ID,
        object="batch",
        completion_window="24h",
        created_at=1699061776,
        endpoint="/v1/chat/completions",
        input_file_id=FILE_ID,
        status=status,
    )


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
    # spec=OpenAI guards top-level namespace access (an unknown attribute on the client fails);
    # the method surface (``conn.responses.create``) is type-checked by mypy against the SDK stubs.
    with patch("airflow.providers.openai.hooks.openai.OpenAI") as mock_client:
        mock_client.return_value = MagicMock(spec=OpenAI)
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
        parallel_tool_calls=False,
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


@pytest.fixture
def mock_file():
    return FileObject(
        id=FILE_ID,
        object="file",
        bytes=120000,
        created_at=1677610602,
        filename=FILE_NAME,
        purpose="assistants",
        status="processed",
    )


@pytest.fixture
def mock_file_list(mock_file):
    return SyncCursorPage[FileObject](data=[mock_file])


@pytest.fixture
def mock_vector_store():
    return VectorStore(
        id=VECTOR_STORE_ID,
        object="vector_store",
        created_at=1698107661,
        usage_bytes=123456,
        last_active_at=1698107661,
        name=VECTOR_STORE_NAME,
        bytes=123456,
        status="completed",
        file_counts={"in_progress": 0, "completed": 100, "cancelled": 0, "failed": 0, "total": 100},
        metadata={},
        last_used_at=1698107661,
    )


@pytest.fixture
def mock_vector_store_list(mock_vector_store):
    return SyncCursorPage[VectorStore](data=[mock_vector_store])


@pytest.fixture
def mock_vector_file_store_batch():
    return VectorStoreFileBatch(
        id=VECTOR_FILE_STORE_BATCH_ID,
        object="vector_store.files_batch",
        created_at=1699061776,
        vector_store_id=VECTOR_STORE_ID,
        status="completed",
        file_counts={
            "in_progress": 0,
            "completed": 3,
            "failed": 0,
            "cancelled": 0,
            "total": 0,
        },
    )


@pytest.fixture
def mock_vector_file_store_list():
    return SyncCursorPage[VectorStoreFile](
        data=[
            VectorStoreFile(
                id="test-file-abc123",
                object="vector_store.file",
                created_at=1699061776,
                usage_bytes=1234,
                vector_store_id=VECTOR_STORE_ID,
                status="completed",
                last_error=None,
            ),
            VectorStoreFile(
                id="test-file-abc456",
                object="vector_store.file",
                created_at=1699061776,
                usage_bytes=1234,
                vector_store_id=VECTOR_STORE_ID,
                status="completed",
                last_error=None,
            ),
        ]
    )


@pytest.fixture(
    params=[
        "completed",
        "expired",
        "cancelling",
        "cancelled",
        "failed",
    ]
)
def mock_terminated_batch(request):
    return create_batch(request.param)


@pytest.fixture(params=["validating", "in_progress", "finalizing"])
def mock_wip_batch(request):
    return create_batch(request.param)


def test_create_chat_completion(mock_openai_hook, mock_completion):
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Hello!"},
    ]

    mock_openai_hook.conn.chat.completions.create.return_value = mock_completion
    completion = mock_openai_hook.create_chat_completion(model=MODEL, messages=messages)
    choice = completion[0]
    assert choice.message.content == "Hello there, how may I assist you today?"


def test_create_response(mock_openai_hook):
    expected = mock_openai_hook.conn.responses.create.return_value
    result = mock_openai_hook.create_response(input="Hello", model=MODEL)
    mock_openai_hook.conn.responses.create.assert_called_once_with(model=MODEL, input="Hello")
    assert result is expected


def test_get_response(mock_openai_hook):
    expected = mock_openai_hook.conn.responses.retrieve.return_value
    result = mock_openai_hook.get_response("resp_123")
    mock_openai_hook.conn.responses.retrieve.assert_called_once_with("resp_123")
    assert result is expected


def test_delete_response(mock_openai_hook):
    mock_openai_hook.delete_response("resp_123")
    mock_openai_hook.conn.responses.delete.assert_called_once_with("resp_123")


def test_cancel_response(mock_openai_hook):
    expected = mock_openai_hook.conn.responses.cancel.return_value
    result = mock_openai_hook.cancel_response("resp_123")
    mock_openai_hook.conn.responses.cancel.assert_called_once_with("resp_123")
    assert result is expected


def test_create_conversation(mock_openai_hook):
    expected = mock_openai_hook.conn.conversations.create.return_value
    result = mock_openai_hook.create_conversation(metadata={"topic": "demo"})
    mock_openai_hook.conn.conversations.create.assert_called_once_with(metadata={"topic": "demo"})
    assert result is expected


def test_get_conversation(mock_openai_hook):
    expected = mock_openai_hook.conn.conversations.retrieve.return_value
    result = mock_openai_hook.get_conversation("conv_123")
    mock_openai_hook.conn.conversations.retrieve.assert_called_once_with("conv_123")
    assert result is expected


def test_update_conversation(mock_openai_hook):
    expected = mock_openai_hook.conn.conversations.update.return_value
    result = mock_openai_hook.update_conversation("conv_123", metadata={"topic": "demo"})
    mock_openai_hook.conn.conversations.update.assert_called_once_with("conv_123", metadata={"topic": "demo"})
    assert result is expected


def test_delete_conversation(mock_openai_hook):
    expected = mock_openai_hook.conn.conversations.delete.return_value
    result = mock_openai_hook.delete_conversation("conv_123")
    mock_openai_hook.conn.conversations.delete.assert_called_once_with("conv_123")
    assert result is expected


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


def test_create_run_and_poll(mock_openai_hook, mock_run):
    thread_id = THREAD_ID
    assistant_id = ASSISTANT_ID
    mock_openai_hook.conn.beta.threads.runs.create_and_poll.return_value = mock_run
    run = mock_openai_hook.create_run_and_poll(thread_id=thread_id, assistant_id=assistant_id)
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


@patch("builtins.open", new_callable=mock_open, read_data="test-data")
def test_upload_file(mock_file_open, mock_openai_hook, mock_file):
    mock_file.name = FILE_NAME
    mock_file.purpose = "assistants"
    mock_openai_hook.conn.files.create.return_value = mock_file
    file = mock_openai_hook.upload_file(file=mock_file_open(), purpose="assistants")
    assert file.name == FILE_NAME
    assert file.purpose == "assistants"


def test_get_file(mock_openai_hook, mock_file):
    mock_openai_hook.conn.files.retrieve.return_value = mock_file
    file = mock_openai_hook.get_file(file_id=FILE_ID)
    assert file.id == FILE_ID
    assert file.filename == FILE_NAME


def test_get_files(mock_openai_hook, mock_file_list):
    mock_openai_hook.conn.files.list.return_value = mock_file_list
    files = mock_openai_hook.get_files()
    assert isinstance(files, list)


def test_delete_file(mock_openai_hook):
    delete_response = FileDeleted(id=FILE_ID, object="file", deleted=True)
    mock_openai_hook.conn.files.delete.return_value = delete_response
    file_deleted = mock_openai_hook.delete_file(file_id=FILE_ID)
    assert file_deleted.deleted


def test_create_vector_store(mock_openai_hook, mock_vector_store):
    mock_openai_hook.conn.vector_stores.create.return_value = mock_vector_store
    vector_store = mock_openai_hook.create_vector_store(name=VECTOR_STORE_NAME)
    assert vector_store.id == VECTOR_STORE_ID
    assert vector_store.name == VECTOR_STORE_NAME


def test_get_vector_store(mock_openai_hook, mock_vector_store):
    mock_openai_hook.conn.vector_stores.retrieve.return_value = mock_vector_store
    vector_store = mock_openai_hook.get_vector_store(vector_store_id=VECTOR_STORE_ID)
    assert vector_store.id == VECTOR_STORE_ID
    assert vector_store.name == VECTOR_STORE_NAME


def test_get_vector_stores(mock_openai_hook, mock_vector_store_list):
    mock_openai_hook.conn.vector_stores.list.return_value = mock_vector_store_list
    vector_stores = mock_openai_hook.get_vector_stores()
    assert isinstance(vector_stores, list)


def test_modify_vector_store(mock_openai_hook, mock_vector_store):
    new_vector_store_name = "New Vector Store"
    mock_vector_store.name = new_vector_store_name
    mock_openai_hook.conn.vector_stores.update.return_value = mock_vector_store
    vector_store = mock_openai_hook.modify_vector_store(
        vector_store_id=VECTOR_STORE_ID, name=new_vector_store_name
    )
    assert vector_store.name == new_vector_store_name


def test_delete_vector_store(mock_openai_hook):
    delete_response = VectorStoreDeleted(id=VECTOR_STORE_ID, object="vector_store.deleted", deleted=True)
    mock_openai_hook.conn.vector_stores.delete.return_value = delete_response
    vector_store_deleted = mock_openai_hook.delete_vector_store(vector_store_id=VECTOR_STORE_ID)
    assert vector_store_deleted.deleted


def test_upload_files_to_vector_store(mock_openai_hook, mock_vector_file_store_batch):
    files = ["file1.txt", "file2.txt", "file3.txt"]
    mock_openai_hook.conn.vector_stores.file_batches.upload_and_poll.return_value = (
        mock_vector_file_store_batch
    )
    vector_file_store_batch = mock_openai_hook.upload_files_to_vector_store(
        vector_store_id=VECTOR_STORE_ID, files=files
    )
    assert vector_file_store_batch.id == VECTOR_FILE_STORE_BATCH_ID
    assert vector_file_store_batch.file_counts.completed == len(files)


def test_get_vector_store_files(mock_openai_hook, mock_vector_file_store_list):
    mock_openai_hook.conn.vector_stores.files.list.return_value = mock_vector_file_store_list
    vector_file_store_list = mock_openai_hook.get_vector_store_files(vector_store_id=VECTOR_STORE_ID)
    assert isinstance(vector_file_store_list, list)


def test_delete_vector_store_file(mock_openai_hook):
    delete_response = VectorStoreFileDeleted(
        id="test_file_abc123", object="vector_store.file.deleted", deleted=True
    )
    mock_openai_hook.conn.vector_stores.files.delete.return_value = delete_response
    vector_store_file_deleted = mock_openai_hook.delete_vector_store_file(
        vector_store_id=VECTOR_STORE_ID, file_id=FILE_ID
    )
    assert vector_store_file_deleted.id == FILE_ID
    assert vector_store_file_deleted.deleted


def test_create_batch(mock_openai_hook, mock_terminated_batch):
    mock_openai_hook.conn.batches.create.return_value = mock_terminated_batch
    batch = mock_openai_hook.create_batch(endpoint="/v1/chat/completions", file_id=FILE_ID)
    assert batch.id == mock_terminated_batch.id


def test_get_batch(mock_openai_hook, mock_terminated_batch):
    mock_openai_hook.conn.batches.retrieve.return_value = mock_terminated_batch
    batch = mock_openai_hook.get_batch(batch_id=BATCH_ID)
    assert batch.id == mock_terminated_batch.id


def test_cancel_batch(mock_openai_hook, mock_terminated_batch):
    mock_openai_hook.conn.batches.cancel.return_value = mock_terminated_batch
    batch = mock_openai_hook.cancel_batch(batch_id=BATCH_ID)
    assert batch.id == mock_terminated_batch.id


def test_wait_for_finished_batch(mock_openai_hook, mock_terminated_batch):
    mock_openai_hook.conn.batches.retrieve.return_value = mock_terminated_batch
    if mock_terminated_batch.status == "completed":
        try:
            mock_openai_hook.wait_for_batch(batch_id=BATCH_ID)
        except Exception as e:
            pytest.fail(f"Should not have raised exception: {e}")
    else:
        with pytest.raises(OpenAIBatchJobException, match="Batch failed"):
            mock_openai_hook.wait_for_batch(batch_id=BATCH_ID, wait_seconds=0.01, timeout=0.1)


def test_wait_for_in_progress_batch_timeout(mock_openai_hook, mock_wip_batch):
    mock_openai_hook.conn.batches.retrieve.return_value = mock_wip_batch
    with pytest.raises(OpenAIBatchTimeout, match="Timeout"):
        mock_openai_hook.wait_for_batch(batch_id=BATCH_ID, wait_seconds=0.2, timeout=0.01)
    assert mock_openai_hook.conn.batches.retrieve.call_count >= 1
    assert mock_openai_hook.conn.batches.cancel.call_count == 1


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


def _make_workload_identity_conn(conn_id, extra):
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={
            "auth_type": "workload_identity",
            "identity_provider_id": "idp-123",
            "service_account_id": "sa-456",
            **extra,
        },
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    return conn


@pytest.mark.parametrize(
    ("conn_id", "extra", "factory_attr", "expected_kwargs"),
    [
        (
            "wi_k8s",
            {"workload_identity_provider": "kubernetes", "token_file_path": "/var/run/token"},
            "k8s_service_account_token_provider",
            {"token_file_path": "/var/run/token"},
        ),
        (
            "wi_k8s_default",
            {"workload_identity_provider": "kubernetes"},
            "k8s_service_account_token_provider",
            {},
        ),
        (
            "wi_azure",
            {
                "workload_identity_provider": "azure",
                "resource": "https://cognitiveservices.azure.com/",
                "client_id": "client-789",
            },
            "azure_managed_identity_token_provider",
            {"resource": "https://cognitiveservices.azure.com/", "client_id": "client-789"},
        ),
        (
            "wi_gcp",
            {"workload_identity_provider": "gcp", "audience": "https://api.openai.com/v1"},
            "gcp_id_token_provider",
            {"audience": "https://api.openai.com/v1"},
        ),
    ],
)
@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_workload_identity_token_source(mock_client, conn_id, extra, factory_attr, expected_kwargs):
    provider = object()
    with patch(
        f"airflow.providers.openai.hooks.openai.{factory_attr}", return_value=provider
    ) as mock_factory:
        conn = _make_workload_identity_conn(conn_id, extra)
        OpenAIHook(conn_id=conn.conn_id).get_conn()
    mock_factory.assert_called_once_with(**expected_kwargs)
    mock_client.assert_called_once_with(
        workload_identity={
            "identity_provider_id": "idp-123",
            "service_account_id": "sa-456",
            "provider": provider,
        },
        base_url=None,
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
@patch("airflow.providers.openai.hooks.openai.import_string")
def test_get_conn_workload_identity_custom(mock_import_string, mock_client):
    def get_token():
        return "token"

    mock_import_string.return_value = get_token
    conn = _make_workload_identity_conn(
        "wi_custom",
        {
            "workload_identity_provider": "custom",
            "token_provider": "my.module.get_token",
            "token_type": "id",
            "refresh_buffer_seconds": 300,
        },
    )
    OpenAIHook(conn_id=conn.conn_id).get_conn()
    mock_import_string.assert_called_once_with("my.module.get_token")
    mock_client.assert_called_once_with(
        workload_identity={
            "identity_provider_id": "idp-123",
            "service_account_id": "sa-456",
            "provider": {"token_type": "id", "get_token": get_token},
            "refresh_buffer_seconds": 300,
        },
        base_url=None,
    )


@patch("airflow.providers.openai.hooks.openai.k8s_service_account_token_provider")
@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_workload_identity_ignores_stray_api_key(mock_client, mock_provider):
    provider = object()
    mock_provider.return_value = provider
    conn = _make_workload_identity_conn(
        "wi_stray_api_key",
        {
            "workload_identity_provider": "kubernetes",
            "openai_client_kwargs": {"api_key": "leftover-key"},
        },
    )
    OpenAIHook(conn_id=conn.conn_id).get_conn()
    # ``api_key`` is popped for every path, so it is never forwarded alongside ``workload_identity``.
    mock_client.assert_called_once_with(
        workload_identity={
            "identity_provider_id": "idp-123",
            "service_account_id": "sa-456",
            "provider": provider,
        },
        base_url=None,
    )


def test_get_conn_invalid_auth_type():
    conn = Connection(conn_id="bad_auth", conn_type="openai", extra={"auth_type": "oauth"})
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    with pytest.raises(ValueError, match="Unsupported auth_type 'oauth'"):
        OpenAIHook(conn_id=conn.conn_id).get_conn()


def test_get_conn_invalid_workload_identity_provider():
    conn = _make_workload_identity_conn("bad_wi_provider", {"workload_identity_provider": "saml"})
    with pytest.raises(ValueError, match="Unsupported workload_identity_provider 'saml'"):
        OpenAIHook(conn_id=conn.conn_id).get_conn()


def test_get_conn_workload_identity_missing_required_key():
    conn = Connection(
        conn_id="wi_missing_key",
        conn_type="openai",
        extra={
            "auth_type": "workload_identity",
            "workload_identity_provider": "kubernetes",
            "service_account_id": "sa-456",
        },
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    with pytest.raises(ValueError, match="Missing required 'identity_provider_id'"):
        OpenAIHook(conn_id=conn.conn_id).get_conn()


def test_get_conn_workload_identity_custom_missing_token_provider():
    conn = _make_workload_identity_conn("wi_custom_missing", {"workload_identity_provider": "custom"})
    with pytest.raises(ValueError, match="Missing required 'token_provider'"):
        OpenAIHook(conn_id=conn.conn_id).get_conn()
