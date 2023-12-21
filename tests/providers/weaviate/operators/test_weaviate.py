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
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.weaviate.hooks.weaviate import WeaviateHook
from airflow.providers.weaviate.operators.weaviate import WeaviateIngestOperator


@pytest.fixture
def mock_weaviate_connection():
    conn_id = "weaviate_conn"
    conn = Connection(
        conn_id=conn_id,
        conn_type="weaviate",
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    yield conn


@pytest.fixture
def mock_weaviate_hook(mock_weaviate_connection):
    with patch("airflow.providers.weaviate.hooks.weaviate.WeaviateHook"):
        yield WeaviateHook(conn_id=mock_weaviate_connection.conn_id)


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
        assert operator.input_data == {"data": "sample_data"}
        assert operator.batch_params == {}
        assert operator.hook_params == {}

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator.log")
    def test_execute_with_input_json(self, mock_log, operator):
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with(
            "my_class", {"data": "sample_data"}, vector_col="Vector", **{}
        )
        mock_log.debug.assert_called_once_with("Input data: %s", {"data": "sample_data"})

    def test_templates(self, create_task_instance_of_operator):
        dag_id = "TestWeaviateIngestOperator"
        ti = create_task_instance_of_operator(
            WeaviateIngestOperator,
            dag_id=dag_id,
            task_id="task-id",
            conn_id="weaviate_conn",
            class_name="my_class",
            input_json="{{ dag.dag_id }}",
            input_data="{{ dag.dag_id }}",
        )
        ti.render_templates()

        assert dag_id == ti.task.input_json
        assert dag_id == ti.task.input_data
