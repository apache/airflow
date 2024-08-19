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

openai = pytest.importorskip("openai")

from airflow.exceptions import TaskDeferred
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator, OpenAITriggerBatchOperator
from airflow.providers.openai.triggers.openai import OpenAIBatchTrigger
from airflow.utils.context import Context


def test_execute_with_input_text():
    operator = OpenAIEmbeddingOperator(
        task_id="TaskId", conn_id="test_conn_id", model="test_model", input_text="Test input text"
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
        task_id="TaskId", conn_id="test_conn_id", model="test_model", input_text=invalid_input
    )
    context = Context()
    with pytest.raises(ValueError):
        operator.execute(context)


def test_openai_trigger_batch_operator():
    operator = OpenAITriggerBatchOperator(
        task_id="TaskId",
        conn_id="test_conn_id",
        file_id="file_id",
        endpoint="/v1/chat/completions",
    )
    mock_batch = Batch(
        id="batch_id",
        object="batch",
        completion_window="24h",
        created_at=1699061776,
        endpoint="/v1/chat/completions",
        input_file_id="file_id",
        status="completed",
    )
    mock_hook_instance = Mock()
    mock_hook_instance.get_batch.return_value = mock_batch
    mock_hook_instance.create_batch.return_value = mock_batch
    operator.hook = mock_hook_instance

    context = Context()
    batch_id = operator.execute(context)
    assert batch_id == "batch_id"


def test_openai_trigger_batch_operator_with_deferred():
    operator = OpenAITriggerBatchOperator(
        task_id="TaskId",
        conn_id="test_conn_id",
        file_id="file_id",
        endpoint="/v1/chat/completions",
        deferrable=True,
    )
    mock_hook_instance = Mock()
    mock_hook_instance.get_batch.return_value = Batch(
        id="batch_id",
        object="batch",
        completion_window="24h",
        created_at=1699061776,
        endpoint="/v1/chat/completions",
        input_file_id="file_id",
        status="in_progress",
    )
    operator.hook = mock_hook_instance

    context = Context()
    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)
    assert isinstance(exc.value.trigger, OpenAIBatchTrigger)
