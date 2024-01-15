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

from airflow.providers.weaviate.operators.weaviate import (
    WeaviateDocumentIngestOperator,
    WeaviateIngestOperator,
)


class TestWeaviateIngestOperator:
    @pytest.fixture
    def operator(self):
        return WeaviateIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            class_name="my_class",
            input_json=[{"data": "sample_data"}],
        )

    def test_constructor(self, operator):
        assert operator.conn_id == "weaviate_conn"
        assert operator.class_name == "my_class"
        assert operator.input_data == [{"data": "sample_data"}]
        assert operator.batch_params == {}
        assert operator.hook_params == {}

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator.log")
    def test_execute_with_input_json(self, mock_log, operator):
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with(
            class_name="my_class",
            data=[{"data": "sample_data"}],
            batch_config_params={},
            vector_col="Vector",
            uuid_col="id",
            tenant=None,
        )
        mock_log.debug.assert_called_once_with("Input data: %s", [{"data": "sample_data"}])

    @pytest.mark.db_test
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


class TestWeaviateDocumentIngestOperator:
    @pytest.fixture
    def operator(self):
        return WeaviateDocumentIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            input_data=[{"data": "sample_data"}],
            class_name="my_class",
            document_column="docLink",
            existing="skip",
            uuid_column="id",
            vector_col="vector",
            batch_config_params={"size": 1000},
        )

    def test_constructor(self, operator):
        assert operator.conn_id == "weaviate_conn"
        assert operator.input_data == [{"data": "sample_data"}]
        assert operator.class_name == "my_class"
        assert operator.document_column == "docLink"
        assert operator.existing == "skip"
        assert operator.uuid_column == "id"
        assert operator.vector_col == "vector"
        assert operator.batch_config_params == {"size": 1000}
        assert operator.hook_params == {}

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateDocumentIngestOperator.log")
    def test_execute_with_input_json(self, mock_log, operator):
        operator.hook.create_or_replace_document_objects = MagicMock()

        operator.execute(context=None)

        operator.hook.create_or_replace_document_objects.assert_called_once_with(
            data=[{"data": "sample_data"}],
            class_name="my_class",
            document_column="docLink",
            existing="skip",
            uuid_column="id",
            vector_column="vector",
            batch_config_params={"size": 1000},
            tenant=None,
            verbose=False,
        )
        mock_log.debug.assert_called_once_with("Total input objects : %s", len([{"data": "sample_data"}]))
