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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator


class TestWeaviateIngestOperator:
    @pytest.fixture
    def operator(self):
        return WeaviateIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            class_name="my_class",
            input_json={"data": "sample_data"},
        )

    def test_constructor(self, operator):
        assert operator.conn_id == "weaviate_conn"
        assert operator.class_name == "my_class"
        assert operator.input_json == {"data": "sample_data"}
        assert operator.input_callable is None
        assert operator.input_callable_args == ()
        assert operator.input_callable_kwargs == {}
        assert operator.batch_params == {}
        assert operator.hook_params == {}

    def test_constructor_with_callable(self):
        operator = WeaviateIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            class_name="my_class",
            input_callable=lambda: {"data": "sample_data_callable"},
        )
        assert operator.input_callable is not None

    def test_constructor_with_invalid_callable(self):
        with pytest.raises(AirflowException):
            WeaviateIngestOperator(
                task_id="weaviate_task",
                conn_id="weaviate_conn",
                class_name="my_class",
                input_callable="not_a_callable",
            ).execute(context=None)

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator.log")
    def test_execute_with_input_json(self, mock_log, operator):
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with("my_class", {"data": "sample_data"}, **{})
        mock_log.debug.assert_called_once_with("Input json: %s", {"data": "sample_data"})

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator.log")
    def test_execute_with_input_callable(self, mock_log, operator):
        operator.hook.batch_data = MagicMock()
        operator.input_json = None
        operator.input_callable = lambda: {"data": "sample_data_callable"}
        operator.input_callable_args = ()
        operator.input_callable_kwargs = {}

        operator.execute(context=None)

        mock_log.debug.assert_called_once_with("Input json: %s", {"data": "sample_data_callable"})

    def test_execute_with_both_input_json_and_callable(self, operator):
        operator.input_callable = lambda: {"data": "sample_data_callable"}
        with pytest.raises(RuntimeError):
            operator.execute(context=None)

    def test_execute_with_no_input(self, operator):
        operator.input_json = None
        operator.input_callable = None
        with pytest.raises(RuntimeError):
            operator.execute(context=None)
