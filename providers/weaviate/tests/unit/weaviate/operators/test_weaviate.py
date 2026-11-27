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

pytest.importorskip("weaviate")

from airflow.providers.weaviate.operators.weaviate import (
    WeaviateDocumentIngestOperator,
    WeaviateIngestOperator,
    WeaviateSwapAliasOperator,
)

from tests_common.test_utils.taskinstance import render_template_fields


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
    def test_execute_with_input_data(self, mock_log, operator):
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with(
            collection_name="my_collection",
            data=[{"data": "sample_data"}],
            vector_col="Vector",
            uuid_col="id",
            tenant=None,
        )
        mock_log.debug.assert_called_once_with("Input data: %s", [{"data": "sample_data"}])

    def test_execute_passes_tenant_to_hook(self):
        operator = WeaviateIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            collection_name="my_collection",
            input_data=[{"data": "sample_data"}],
            tenant="tenant-a",
        )
        operator.hook.batch_data = MagicMock()

        operator.execute(context=None)

        operator.hook.batch_data.assert_called_once_with(
            collection_name="my_collection",
            data=[{"data": "sample_data"}],
            vector_col="Vector",
            uuid_col="id",
            tenant="tenant-a",
        )

    def test_missing_input_data_raises_at_execute_not_init(self):
        """
        input_data is a template field, so the required-value check runs in execute()
        (after rendering), not in __init__. Constructing with input_data=None must not raise.
        """
        operator = WeaviateIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            collection_name="my_collection",
            input_data=None,
        )

        with pytest.raises(TypeError, match="input_data is required"):
            operator.execute(context=None)

    @pytest.mark.db_test
    def test_templates(self, create_task_instance_of_operator):
        dag_id = "TestWeaviateIngestOperator"
        ti = create_task_instance_of_operator(
            WeaviateIngestOperator,
            dag_id=dag_id,
            task_id="task-id",
            conn_id="weaviate_conn",
            collection_name="my_collection",
            input_data="{{ dag.dag_id }}",
        )
        task = ti.render_templates()
        assert dag_id == task.input_data

    @pytest.mark.db_test
    def test_partial_batch_hook_params(self, dag_maker, session):
        with dag_maker(dag_id="test_partial_batch_hook_params", session=session):
            task = WeaviateIngestOperator.partial(
                task_id="fake-task-id",
                conn_id="weaviate_conn",
                collection_name="FooBar",
                hook_params={"baz": "biz"},
            ).expand(input_data=[{}, {}])

        dr = dag_maker.create_dagrun()
        tis = dr.get_task_instances(session=session)
        for ti in tis:
            rendered = render_template_fields(ti, task.unmap({"input_data": {}}))
            assert rendered.hook_params == {"baz": "biz"}


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

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateDocumentIngestOperator.log")
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
            tenant=None,
        )
        mock_log.debug.assert_called_once_with("Total input objects : %s", len([{"data": "sample_data"}]))

    def test_execute_passes_tenant_to_hook(self):
        operator = WeaviateDocumentIngestOperator(
            task_id="weaviate_task",
            conn_id="weaviate_conn",
            input_data=[{"data": "sample_data"}],
            collection_name="my_collection",
            document_column="docLink",
            tenant="tenant-a",
        )
        operator.hook.create_or_replace_document_objects = MagicMock()

        operator.execute(context=None)

        operator.hook.create_or_replace_document_objects.assert_called_once_with(
            data=[{"data": "sample_data"}],
            collection_name="my_collection",
            document_column="docLink",
            existing="skip",
            uuid_column="id",
            vector_column="Vector",
            verbose=False,
            tenant="tenant-a",
        )

    @pytest.mark.db_test
    def test_partial_hook_params(self, dag_maker, session):
        with dag_maker(dag_id="test_partial_hook_params", session=session):
            task = WeaviateDocumentIngestOperator.partial(
                task_id="fake-task-id",
                conn_id="weaviate_conn",
                collection_name="FooBar",
                document_column="spam-egg",
                hook_params={"baz": "biz"},
            ).expand(input_data=[{}, {}])

        dr = dag_maker.create_dagrun()
        tis = dr.get_task_instances(session=session)
        for ti in tis:
            rendered = render_template_fields(ti, task.unmap({"input_data": {}}))
            assert rendered.hook_params == {"baz": "biz"}


class TestWeaviateSwapAliasOperator:
    @staticmethod
    def make_operator(target_collection: str = "Docs_v2", **kwargs):
        operator = WeaviateSwapAliasOperator(
            task_id="swap_alias",
            conn_id="weaviate_conn",
            alias_name="Docs",
            target_collection=target_collection,
            **kwargs,
        )
        mock_hook = MagicMock()
        mock_hook.get_alias.return_value = None
        mock_hook.update_alias.return_value = True
        mock_hook.list_aliases.return_value = {}
        operator.__dict__["hook"] = mock_hook
        return operator

    def test_creates_missing_alias(self):
        operator = self.make_operator()

        assert operator.execute(context=None) is None

        operator.hook.create_alias.assert_called_once_with("Docs", "Docs_v2")
        operator.hook.update_alias.assert_not_called()

    def test_missing_alias_raises_without_create_if_missing(self):
        operator = self.make_operator(create_if_missing=False)

        with pytest.raises(ValueError, match="does not exist"):
            operator.execute(context=None)

        operator.hook.create_alias.assert_not_called()

    @pytest.mark.parametrize("target_collection", ["Docs_v2", "docs_v2"])
    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateSwapAliasOperator.log")
    def test_alias_already_on_target_is_a_no_op(self, mock_log, target_collection):
        operator = self.make_operator(
            target_collection=target_collection,
            delete_previous_collection=True,
        )
        operator.hook.get_alias.return_value = MagicMock(collection="Docs_v2")

        assert operator.execute(context=None) is None

        operator.hook.update_alias.assert_not_called()
        operator.hook.list_aliases.assert_not_called()
        operator.hook.delete_collections.assert_not_called()
        mock_log.warning.assert_called_once()
        assert "skipping deletion" in mock_log.warning.call_args[0][0]

    def test_swaps_alias_and_keeps_previous_collection_by_default(self):
        operator = self.make_operator()
        operator.hook.get_alias.return_value = MagicMock(collection="Docs_v1")

        assert operator.execute(context=None) == "Docs_v1"

        operator.hook.update_alias.assert_called_once_with("Docs", "Docs_v2")
        operator.hook.list_aliases.assert_not_called()
        operator.hook.delete_collections.assert_not_called()

    def test_deletes_previous_collection_after_swap(self):
        operator = self.make_operator(delete_previous_collection=True)
        operator.hook.get_alias.return_value = MagicMock(collection="Docs_v1")

        assert operator.execute(context=None) == "Docs_v1"

        operator.hook.list_aliases.assert_called_once_with(collection_name="Docs_v1")
        operator.hook.delete_collections.assert_called_once_with("Docs_v1")

    @patch("airflow.providers.weaviate.operators.weaviate.WeaviateSwapAliasOperator.log")
    def test_skips_deleting_previous_collection_when_other_aliases_remain(self, mock_log):
        operator = self.make_operator(delete_previous_collection=True)
        operator.hook.get_alias.return_value = MagicMock(collection="Docs_v1")
        operator.hook.list_aliases.return_value = {"OtherAlias": MagicMock(collection="Docs_v1")}

        assert operator.execute(context=None) == "Docs_v1"

        operator.hook.list_aliases.assert_called_once_with(collection_name="Docs_v1")
        operator.hook.delete_collections.assert_not_called()
        mock_log.warning.assert_called_once()
        assert mock_log.warning.call_args[0][2] == ["OtherAlias"]

    def test_raises_when_alias_disappears_before_update(self):
        operator = self.make_operator(delete_previous_collection=True)
        operator.hook.get_alias.return_value = MagicMock(collection="Docs_v1")
        operator.hook.update_alias.return_value = False

        with pytest.raises(RuntimeError, match="disappeared"):
            operator.execute(context=None)

        operator.hook.list_aliases.assert_not_called()
        operator.hook.delete_collections.assert_not_called()
