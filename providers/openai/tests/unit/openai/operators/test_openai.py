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

from unittest.mock import Mock

import pytest
from openai.types.batch import Batch

from airflow.exceptions import TaskDeferred
from airflow.providers.common.compat.sdk import Context
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator, OpenAITriggerBatchOperator
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger

openai = pytest.importorskip("openai")
TASK_ID = "TaskId"
CONN_ID = "test_conn_id"
BATCH_ID = "batch_id"
FILE_ID = "file_id"
BATCH_ENDPOINT = "/v1/chat/completions"


@pytest.fixture
def mock_batch():
    return Batch(
        id=BATCH_ID,
        object="batch",
        completion_window="24h",
        created_at=1699061776,
        endpoint=BATCH_ENDPOINT,
        input_file_id=FILE_ID,
        status="in_progress",
    )


def test_execute_with_input_text():
    operator = OpenAIEmbeddingOperator(
        task_id=TASK_ID, conn_id=CONN_ID, model="test_model", input_text="Test input text"
    )
    mock_hook_instance = Mock()
    mock_hook_instance.create_embeddings.return_value = [1.0, 2.0, 3.0]
    operator.hook = mock_hook_instance

    context = Context()
    embeddings = operator.execute(context)

    assert embeddings == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("invalid_input", ["", None, 123])
def test_execute_with_invalid_input(invalid_input):
    operator = OpenAIEmbeddingOperator(
        task_id=TASK_ID, conn_id=CONN_ID, model="test_model", input_text=invalid_input
    )
    context = Context()
    with pytest.raises(
        ValueError,
        match="The 'input_text' must be a non-empty string, list of strings, list of integers, or list of lists of integers.",
    ):
        operator.execute(context)


@pytest.mark.parametrize("wait_for_completion", [True, False])
def test_openai_trigger_batch_operator_not_deferred(mock_batch, wait_for_completion):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        wait_for_completion=wait_for_completion,
        deferrable=False,
    )
    mock_hook_instance = Mock()
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    batch_id = operator.execute(context)
    assert batch_id == BATCH_ID


@pytest.mark.parametrize("wait_for_completion", [True, False])
def test_openai_trigger_batch_operator_with_deferred(mock_batch, wait_for_completion):
    operator = OpenAITriggerBatchOperator(
        task_id=TASK_ID,
        conn_id=CONN_ID,
        file_id=FILE_ID,
        endpoint=BATCH_ENDPOINT,
        deferrable=True,
        wait_for_completion=wait_for_completion,
    )
    mock_hook_instance = Mock()
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    if wait_for_completion:
        with pytest.raises(TaskDeferred) as exc:
            operator.execute(context)
        assert isinstance(exc.value.trigger, OpenAIBatchTrigger)
    else:
        batch_id = operator.execute(context)
        assert batch_id == BATCH_ID
