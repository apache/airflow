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

from airflow.exceptions import AirflowException
from airflow.providers.openai.operators.openai import OpenAIEmbeddingOperator
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


def test_execute_with_invalid_input_empty_string():
    with pytest.raises(AirflowException):
        OpenAIEmbeddingOperator(task_id="TaskId", conn_id="test_conn_id", model="test_model", input_text="")


def test_execute_with_invalid_input_none():
    with pytest.raises(AirflowException):
        OpenAIEmbeddingOperator(task_id="TaskId", conn_id="test_conn_id", model="test_model", input_text=None)


def test_execute_with_invalid_input_wrong_type():
    with pytest.raises(AirflowException):
        OpenAIEmbeddingOperator(task_id="TaskId", conn_id="test_conn_id", model="test_model", input_text=123)
