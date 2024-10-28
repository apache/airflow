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

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.utils.task_instance_session import set_current_task_instance_session

pytest.importorskip("weaviate")

from airflow.providers.weaviate.operators.weaviate import (  # noqa: E402
    WeaviateDocumentIngestOperator,
    WeaviateIngestOperator,
)


class TestWeaviateIngestOperator:
    @pytest.fixture
    def operator(self):
        return WeaviateIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            collection_name="my_collection",
            input_data=[{"data": "sample_data"}],
        )

    def test_constructor(self, operator):
        assert operator.conn_id == "weaviate_conn"
        assert operator.collection_name == "my_collection"
        assert operator.input_data == [{"data": "sample_data"}]
        assert operator.hook_params == {}

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator.log")
    def test_execute_with_input_json(self, mock_log, operator):
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Passing 'input_json' to WeaviateIngestOperator is deprecated and you should use 'input_data' instead",
        ):
            operator = WeaviateIngestOperator(
                task_id="weaviate_task",
                conn_id="weaviate_conn",
                collection_name="my_collection",
                input_json=[{"data": "sample_data"}],
            )
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with(
            collection_name="my_collection",
            data=[{"data": "sample_data"}],
            vector_col="Vector",
            uuid_col="id",
        )
        mock_log.debug.assert_called_once_with(
            "Input data: %s", [{"data": "sample_data"}]
        )

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateIngestOperator.log")
    def test_execute_with_input_data(self, mock_log, operator):
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with(
            collection_name="my_collection",
            data=[{"data": "sample_data"}],
            vector_col="Vector",
            uuid_col="id",
        )
        mock_log.debug.assert_called_once_with(
            "Input data: %s", [{"data": "sample_data"}]
        )

    @pytest.mark.db_test
    def test_templates(self, create_task_instance_of_operator):
        dag_id = "TestWeaviateIngestOperator"
        ti = create_task_instance_of_operator(
            WeaviateIngestOperator,
            dag_id=dag_id,
            task_id="task-id",
            conn_id="weaviate_conn",
            collection_name="my_collection",
            input_json="{{ dag.dag_id }}",
            input_data="{{ dag.dag_id }}",
        )
        ti.render_templates()

        assert dag_id == ti.task.input_json
        assert dag_id == ti.task.input_data

    @pytest.mark.db_test
    def test_partial_batch_hook_params(self, dag_maker, session):
        with dag_maker(dag_id="test_partial_batch_hook_params", session=session):
            WeaviateIngestOperator.partial(
                task_id="fake-task-id",
                conn_id="weaviate_conn",
                collection_name="FooBar",
                hook_params={"baz": "biz"},
            ).expand(input_data=[{}, {}])

        dr = dag_maker.create_dagrun()
        tis = dr.get_task_instances(session=session)
        with set_current_task_instance_session(session=session):
            for ti in tis:
                ti.render_templates()
                assert ti.task.hook_params == {"baz": "biz"}


class TestWeaviateDocumentIngestOperator:
    @pytest.fixture
    def operator(self):
        return WeaviateDocumentIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            input_data=[{"data": "sample_data"}],
            collection_name="my_collection",
            document_column="docLink",
            existing="skip",
            uuid_column="id",
            vector_col="vector",
        )

    def test_constructor(self, operator):
        assert operator.conn_id == "weaviate_conn"
        assert operator.input_data == [{"data": "sample_data"}]
        assert operator.collection_name == "my_collection"
        assert operator.document_column == "docLink"
        assert operator.existing == "skip"
        assert operator.uuid_column == "id"
        assert operator.vector_col == "vector"
        assert operator.hook_params == {}

    @patch(
        "airflow.providers.weaviate.operators.weaviate.WeaviateDocumentIngestOperator.log"
    )
    def test_execute_with_input_json(self, mock_log, operator):
        operator.hook.create_or_replace_document_objects = MagicMock()

        operator.execute(context=None)

        operator.hook.create_or_replace_document_objects.assert_called_once_with(
            data=[{"data": "sample_data"}],
            collection_name="my_collection",
            document_column="docLink",
            existing="skip",
            uuid_column="id",
            vector_column="vector",
            verbose=False,
        )
        mock_log.debug.assert_called_once_with(
            "Total input objects : %s", len([{"data": "sample_data"}])
        )

    @pytest.mark.db_test
    def test_partial_hook_params(self, dag_maker, session):
        with dag_maker(dag_id="test_partial_hook_params", session=session):
            WeaviateDocumentIngestOperator.partial(
                task_id="fake-task-id",
                conn_id="weaviate_conn",
                collection_name="FooBar",
                document_column="spam-egg",
                hook_params={"baz": "biz"},
            ).expand(input_data=[{}, {}])

        dr = dag_maker.create_dagrun()
        tis = dr.get_task_instances(session=session)
        with set_current_task_instance_session(session=session):
            for ti in tis:
                ti.render_templates()
                assert ti.task.hook_params == {"baz": "biz"}
